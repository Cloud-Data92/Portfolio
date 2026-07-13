"""
Wallet reconciliation diagnostic for PolyBot.

Proves the ownership model: who signs, who owns tokens, who can redeem.
Run BEFORE any refactoring to establish a ground-truth baseline.

Usage:
    python -m src.reconcile_wallet
"""

import os
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
from dotenv import load_dotenv

# Load credentials (same order as config.py)
secure_env = Path.home() / ".config" / "polybot" / ".env"
if secure_env.exists():
    load_dotenv(secure_env)
project_env = Path(__file__).parent.parent / ".env"
load_dotenv(project_env, override=False)

# Contract addresses on Polygon
POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
USDCE_TOKEN = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
NATIVE_USDC = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# ERC-1155 balanceOf(address,uint256) selector
BALANCE_OF_SELECTOR = "0x00fdd58e"

# ERC-20 balanceOf(address) selector
ERC20_BALANCE_SELECTOR = "0x70a08231"


def _ok(msg: str):
    print(f"  \u2713 {msg}")


def _warn(msg: str):
    print(f"  \u2717 WARNING: {msg}")


def _info(msg: str):
    print(f"  \u2022 {msg}")


def _rpc_call(to: str, data: str) -> str:
    """Raw eth_call to Polygon RPC. Returns hex result."""
    resp = httpx.post(POLYGON_RPC, json={
        "jsonrpc": "2.0", "method": "eth_call",
        "params": [{"to": to, "data": data}, "latest"],
        "id": 1,
    }, timeout=10.0)
    return resp.json().get("result", "0x0")


def _erc20_balance(token: str, address: str) -> float:
    """Query ERC-20 balance (6 decimals for USDC variants)."""
    data = ERC20_BALANCE_SELECTOR + "000000000000000000000000" + address[2:].lower()
    raw = _rpc_call(token, data)
    return int(raw, 16) / 1e6


def _pol_balance(address: str) -> float:
    """Query native POL (MATIC) balance."""
    resp = httpx.post(POLYGON_RPC, json={
        "jsonrpc": "2.0", "method": "eth_getBalance",
        "params": [address, "latest"],
        "id": 1,
    }, timeout=10.0)
    return int(resp.json().get("result", "0x0"), 16) / 1e18


def _erc1155_balance(contract: str, owner: str, token_id: str) -> int:
    """Query ERC-1155 balanceOf(owner, tokenId) on CTF contract."""
    # balanceOf(address,uint256) — pad address to 32 bytes, token_id to 32 bytes
    addr_padded = "000000000000000000000000" + owner[2:].lower()
    # token_id may be a large number (condition token ID) — convert to hex, pad to 64 chars
    if token_id.startswith("0x"):
        tid_int = int(token_id, 16)
    else:
        tid_int = int(token_id)
    tid_hex = hex(tid_int)[2:].zfill(64)
    data = BALANCE_OF_SELECTOR + addr_padded + tid_hex
    raw = _rpc_call(contract, data)
    return int(raw, 16)


def _get_clob_maker(private_key: str, signature_type: int, funder: str) -> str:
    """Determine what the CLOB client sets as 'maker' (token recipient).

    The py_clob_client OrderBuilder sets maker = self.funder.
    For signature_type=0, funder defaults to signer.address() if not provided.
    """
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        api_key = os.getenv("POLYMARKET_API_KEY", "")
        api_secret = os.getenv("POLYMARKET_API_SECRET", "")
        api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "")

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        ) if api_key and api_secret and api_passphrase else None

        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            signature_type=signature_type,
            funder=funder if funder else None,
            creds=creds,
        )
        # The builder.funder IS the maker address (OrderBuilder sets maker = self.funder)
        maker = client.builder.funder
        return maker
    except Exception as e:
        return f"(error: {e})"


def _get_redeemable_positions(address: str) -> list[dict]:
    """Query Polymarket data API for redeemable positions."""
    try:
        r = httpx.get(
            "https://data-api.polymarket.com/positions",
            params={"user": address.lower(), "sizeThreshold": "0", "redeemable": "true"},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        return [p for p in data if float(p.get("size", 0)) > 0]
    except Exception as e:
        print(f"    Error fetching redeemable positions: {e}")
        return []


def _get_open_positions(address: str) -> list[dict]:
    """Query Polymarket data API for all open (non-settled) positions."""
    try:
        r = httpx.get(
            "https://data-api.polymarket.com/positions",
            params={"user": address.lower(), "sizeThreshold": "0"},
            timeout=10,
        )
        if r.status_code != 200:
            return []
        data = r.json()
        return [p for p in data if float(p.get("size", 0)) > 0]
    except Exception as e:
        print(f"    Error fetching positions: {e}")
        return []


def _test_api_creds() -> bool:
    """Verify API credentials are valid by hitting an authenticated endpoint."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        api_key = os.getenv("POLYMARKET_API_KEY", "")
        api_secret = os.getenv("POLYMARKET_API_SECRET", "")
        api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "")

        if not all([private_key, api_key, api_secret, api_passphrase]):
            return False

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            creds=creds,
        )
        # get_api_keys is an authenticated endpoint — returns dict with 'apiKeys' list
        result = client.get_api_keys()
        return isinstance(result, dict) and "apiKeys" in result
    except Exception:
        return False


def main():
    print("\n" + "=" * 70)
    print("POLYBOT WALLET RECONCILIATION")
    print("=" * 70)

    # ─── Resolve identity ───────────────────────────────────────────────
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
    funder_env = os.getenv("POLYMARKET_FUNDER", "").strip()
    proxy_env = os.getenv("POLYMARKET_PROXY_ADDRESS", "").strip()

    sig_labels = {0: "EOA", 1: "Magic.link", 2: "Gnosis Safe"}

    if not private_key:
        print("\n[ERROR] POLYMARKET_PRIVATE_KEY not set.")
        sys.exit(1)

    # Derive EOA address
    try:
        from eth_account import Account
        eoa = Account.from_key(private_key).address
    except Exception as e:
        print(f"\n[ERROR] Cannot derive EOA: {e}")
        sys.exit(1)

    # Resolve funder — who the CLOB treats as maker/owner
    funder = funder_env if funder_env else eoa
    can_self_redeem = (eoa.lower() == funder.lower())

    print(f"\n--- Identity ---")
    print(f"  Signature type:  {signature_type} ({sig_labels.get(signature_type, '?')})")
    print(f"  Private key EOA: {eoa}")
    print(f"  Funder:          {funder_env if funder_env else '(not set, defaults to EOA)'}")
    print(f"  Resolved funder: {funder}")
    print(f"  Proxy address:   {proxy_env if proxy_env else '(not set)'}")
    print(f"  Can self-redeem: {'YES (signer = token owner)' if can_self_redeem else 'NO (signer != token owner)'}")

    # ─── CLOB maker proof ───────────────────────────────────────────────
    print(f"\n--- Token Ownership Proof ---")
    maker = _get_clob_maker(private_key, signature_type, funder_env)
    print(f"  CLOB maker addr: {maker}")

    if maker.startswith("("):
        _warn(f"Could not determine maker: {maker}")
    elif maker.lower() == eoa.lower():
        _ok("Maker = EOA (tokens go to signer)")
    elif maker.lower() == funder.lower():
        _ok(f"Maker = funder ({funder[:10]}...)")
        if not can_self_redeem:
            _warn("Signer CANNOT redeem these tokens (signer != maker)")
    else:
        _warn(f"Maker ({maker}) differs from both EOA and funder!")

    # ─── Positions ──────────────────────────────────────────────────────
    print(f"\n--- Positions (queried for: {funder[:10]}...) ---")
    all_positions = _get_open_positions(funder)
    redeemable = _get_redeemable_positions(funder)

    open_count = len(all_positions)
    redeem_count = len(redeemable)
    print(f"  Total positions:     {open_count}")
    print(f"  Redeemable (wins):   {redeem_count}")

    if redeemable:
        total_redeemable = sum(float(p.get("size", 0)) for p in redeemable)
        print(f"  Redeemable value:    ~${total_redeemable:.2f}")
        for p in redeemable[:5]:
            title = p.get("title", "?")[:45]
            size = float(p.get("size", 0))
            cid = p.get("conditionId", "?")[:12]
            print(f"    {title}... ${size:.2f} (cid: {cid}...)")
        if len(redeemable) > 5:
            print(f"    ... and {len(redeemable) - 5} more")

    # If funder != EOA, also check EOA positions
    if not can_self_redeem:
        print(f"\n--- Positions (EOA: {eoa[:10]}...) ---")
        eoa_positions = _get_open_positions(eoa)
        eoa_redeemable = _get_redeemable_positions(eoa)
        print(f"  Total positions:     {len(eoa_positions)}")
        print(f"  Redeemable (wins):   {len(eoa_redeemable)}")

    # ─── ERC-1155 on-chain token balance spot-check ─────────────────────
    if all_positions:
        print(f"\n--- ERC-1155 On-Chain Spot Check ---")
        checked = 0
        for p in all_positions[:3]:
            asset = p.get("asset", "")
            if not asset:
                continue
            title = p.get("title", "?")[:40]
            api_size = float(p.get("size", 0))
            try:
                chain_balance = _erc1155_balance(CTF_CONTRACT, funder, asset)
                chain_size = chain_balance / 1e6  # CTF tokens have 6 decimals (USDC)
                match = abs(api_size - chain_size) < 0.01
                symbol = "\u2713" if match else "\u2717"
                print(f"  {symbol} {title}...")
                print(f"    API size:   {api_size:.4f}")
                print(f"    On-chain:   {chain_size:.4f} {'(match)' if match else '(MISMATCH!)'}")
                checked += 1
            except Exception as e:
                print(f"  ? {title}... (query error: {e})")
        if not checked:
            _info("No token IDs available for on-chain check")

    # ─── Balances ───────────────────────────────────────────────────────
    print(f"\n--- Balances ---")
    try:
        usdce_bal = _erc20_balance(USDCE_TOKEN, eoa)
        native_usdc_bal = _erc20_balance(NATIVE_USDC, eoa)
        pol_bal = _pol_balance(eoa)
        print(f"  EOA USDC.e:      ${usdce_bal:.6f}")
        print(f"  EOA Native USDC: ${native_usdc_bal:.6f}")
        print(f"  EOA POL (gas):   {pol_bal:.6f}")
    except Exception as e:
        _warn(f"Balance query failed: {e}")

    # CLOB balance via py-clob-client (requires L2 auth)
    clob_bal = None
    try:
        api_key = os.getenv("POLYMARKET_API_KEY", "")
        api_secret = os.getenv("POLYMARKET_API_SECRET", "")
        api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "")
        if all([api_key, api_secret, api_passphrase]):
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams
            creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
            client = ClobClient(
                host="https://clob.polymarket.com",
                key=private_key, chain_id=137,
                signature_type=signature_type,
                funder=funder_env if funder_env else None,
                creds=creds,
            )
            result = client.get_balance_allowance(
                BalanceAllowanceParams(asset_type="COLLATERAL", signature_type=signature_type)
            )
            clob_bal = float(result.get("balance", 0)) / 1e6 if result else None
            if clob_bal is not None:
                print(f"  CLOB deposited:  ${clob_bal:.6f}")
            else:
                print(f"  CLOB deposited:  (no balance in response)")
        else:
            print(f"  CLOB deposited:  (API credentials incomplete)")
    except Exception as e:
        print(f"  CLOB deposited:  (error: {e})")

    # Proxy balance
    if proxy_env and len(proxy_env) == 42:
        try:
            proxy_bal = _erc20_balance(USDCE_TOKEN, proxy_env)
            print(f"  Proxy USDC.e:    ${proxy_bal:.6f}")
        except Exception as e:
            print(f"  Proxy USDC.e:    (error: {e})")
    else:
        print(f"  Proxy USDC.e:    (no proxy address set)")

    # Funder balance if different
    if not can_self_redeem:
        try:
            funder_bal = _erc20_balance(USDCE_TOKEN, funder)
            print(f"  Funder USDC.e:   ${funder_bal:.6f}")
        except Exception:
            pass

    # ─── Checks ─────────────────────────────────────────────────────────
    print(f"\n--- Checks ---")

    # 1. API credentials
    api_ok = _test_api_creds()
    if api_ok:
        _ok("API credentials valid")
    else:
        _warn("API credentials invalid or incomplete")

    # 2. Self-redeem capability
    if can_self_redeem:
        _ok("Signer can redeem (signer = token owner)")
    else:
        _warn(f"Signer CANNOT redeem (EOA {eoa[:10]}... != funder {funder[:10]}...)")
        _warn("Auto-redeem will fail. Must redeem manually via Polymarket UI or provide funder key.")

    # 3. Gas check
    try:
        if pol_bal < 0.01:
            _warn(f"Low POL balance ({pol_bal:.4f}) — may not have gas for redeem txns")
        else:
            _ok(f"POL balance OK ({pol_bal:.4f})")
    except Exception:
        _warn("Could not check POL balance")

    # 4. Balance agreement (CLOB ~ on-chain)
    try:
        if clob_bal is not None:
            diff = abs(usdce_bal - clob_bal)
            if diff < 0.50:
                _ok(f"Balance agreement (CLOB ${clob_bal:.2f} ~ on-chain ${usdce_bal:.2f})")
            else:
                _warn(f"Balance mismatch: CLOB ${clob_bal:.2f} vs on-chain ${usdce_bal:.2f} (diff ${diff:.2f})")
        else:
            _info("Could not compare CLOB vs on-chain balance (no CLOB data)")
    except Exception:
        _info("Could not compare CLOB vs on-chain balance")

    # 5. Redeemable positions check
    if redeem_count > 0 and can_self_redeem:
        _info(f"{redeem_count} position(s) ready to redeem — run auto-redeem or POST /api/redeem")
    elif redeem_count > 0 and not can_self_redeem:
        _warn(f"{redeem_count} position(s) ready but cannot self-redeem!")
    else:
        _ok("No positions pending redemption")

    print("\n" + "=" * 70)
    print("[DONE] Wallet reconciliation complete")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
