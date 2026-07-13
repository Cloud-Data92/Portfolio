"""
Test your Polymarket wallet configuration and balance.

Usage:
    python -m src.test_balance
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

# Load .env
load_dotenv(Path(__file__).parent.parent / ".env")


def main():
    print("\n" + "=" * 70)
    print("POLYMARKET BALANCE TEST")
    print("=" * 70)
    
    # Check environment variables
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
    api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
    api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
    signature_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
    funder = os.getenv("POLYMARKET_FUNDER", "").strip()

    print(f"\nHost:           https://clob.polymarket.com")
    print(f"Signature Type: {signature_type} ({'EOA' if signature_type == 0 else 'Magic.link' if signature_type == 1 else 'Gnosis Safe'})")
    print(f"Private Key:    {'[OK]' if private_key else '[X]'} {'Set' if private_key else 'Not set'}")
    print(f"API Key:        {'[OK]' if api_key else '[X]'} {'Set' if api_key else 'Not set'}")
    print(f"API Secret:     {'[OK]' if api_secret else '[X]'} {'Set' if api_secret else 'Not set'}")
    print(f"API Passphrase: {'[OK]' if api_passphrase else '[X]'} {'Set' if api_passphrase else 'Not set'}")
    if signature_type == 1:
        print(f"Funder:         {'[OK]' if funder else '[X]'} {funder if funder else 'Not set (required for Magic.link!)'}")
    
    print("\n" + "=" * 70)
    
    if not private_key:
        print("[ERROR] Cannot test without POLYMARKET_PRIVATE_KEY")
        sys.exit(1)
    
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        
        print("\n1. Creating ClobClient...")
        
        creds = None
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
        
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=137,
            signature_type=signature_type,
            funder=funder if funder else None,
            creds=creds,
        )
        print("   [OK] Client created")

        print("\n2. Getting wallet address...")
        from eth_account import Account
        account = Account.from_key(private_key)
        print(f"   [OK] Address: {account.address}")
        
        print("\n3. Deriving/verifying API credentials...")
        if not creds:
            client.set_api_creds(client.create_or_derive_api_creds())
            print("   [OK] Credentials derived (add these to .env!)")
            print(f"      API_KEY: {client.creds.api_key}")
            print(f"      API_SECRET: {client.creds.api_secret}")
            print(f"      API_PASSPHRASE: {client.creds.api_passphrase}")
        else:
            print("   [OK] Using provided credentials")
        
        print("\n4. Getting USDC balance...")
        try:
            # Try different balance methods available in py_clob_client
            import httpx
            response = httpx.get(
                f"https://clob.polymarket.com/balance",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            if response.status_code == 200:
                data = response.json()
                print(f"   [$$] BALANCE: ${float(data.get('balance', 0)):.6f} USDC")
            else:
                print(f"   [INFO] Balance check returned: {response.status_code}")
                print("   (You may need to deposit USDC to trade)")
        except Exception as e:
            print(f"   [WARN] Could not get balance: {e}")
            print("   (This might be normal if you haven't traded yet)")

        print("\n5. Testing market API access...")
        import httpx
        response = httpx.get(
            "https://gamma-api.polymarket.com/markets",
            params={"closed": "false", "limit": 1}
        )
        if response.status_code == 200:
            print("   [OK] Market API accessible")
            markets = response.json()
            if markets:
                print(f"   Sample market: {markets[0].get('question', 'N/A')[:50]}...")
        else:
            print(f"   [WARN] Market API returned status {response.status_code}")
        
        print("\n" + "=" * 70)
        print("[SUCCESS] TEST COMPLETED")
        print("=" * 70 + "\n")
        
    except ImportError as e:
        print(f"\n[ERROR] Import error: {e}")
        print("Run: pip install py-clob-client eth-account")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
