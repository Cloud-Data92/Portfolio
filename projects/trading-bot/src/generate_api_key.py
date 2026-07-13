"""
Utility to generate Polymarket API credentials from your private key.

Usage:
    python -m src.generate_api_key
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
    print("\n" + "=" * 60)
    print("POLYMARKET API KEY GENERATOR")
    print("=" * 60)
    
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    
    if not private_key:
        print("\n[ERROR] Error: POLYMARKET_PRIVATE_KEY not set in .env file")
        print("\nSteps:")
        print("1. Copy .env.example to .env")
        print("2. Add your private key to POLYMARKET_PRIVATE_KEY")
        print("3. Run this script again")
        sys.exit(1)
    
    if not private_key.startswith("0x"):
        print("\n[ERROR] Error: Private key must start with 0x")
        sys.exit(1)
    
    print(f"\nPrivate key: {private_key[:6]}...{private_key[-4:]}")
    
    try:
        from py_clob_client.client import ClobClient
        
        print("\nGenerating API credentials...")
        
        # Create client and derive credentials
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=private_key,
            chain_id=137,  # Polygon mainnet
        )
        
        # Derive API credentials
        client.set_api_creds(client.create_or_derive_api_creds())
        creds = client.creds
        
        if not creds:
            print("[ERROR] Failed to generate credentials")
            sys.exit(1)
        
        print("\n" + "=" * 60)
        print("[OK] API CREDENTIALS GENERATED")
        print("=" * 60)
        print(f"\nPOLYMARKET_API_KEY={creds.api_key}")
        print(f"POLYMARKET_API_SECRET={creds.api_secret}")
        print(f"POLYMARKET_API_PASSPHRASE={creds.api_passphrase}")

        # Auto-save to .env
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            env_text = env_path.read_text(encoding="utf-8")
            import re
            env_text = re.sub(r'POLYMARKET_API_KEY=.*', f'POLYMARKET_API_KEY={creds.api_key}', env_text)
            env_text = re.sub(r'POLYMARKET_API_SECRET=.*', f'POLYMARKET_API_SECRET={creds.api_secret}', env_text)
            env_text = re.sub(r'POLYMARKET_API_PASSPHRASE=.*', f'POLYMARKET_API_PASSPHRASE={creds.api_passphrase}', env_text)
            env_path.write_text(env_text, encoding="utf-8")
            print("\n[OK] Saved to .env automatically!")
        else:
            print("\nCopy these values to your .env file!")
        print("=" * 60 + "\n")
        
    except ImportError:
        print("\n[ERROR] Error: py-clob-client not installed")
        print("Run: pip install py-clob-client")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
