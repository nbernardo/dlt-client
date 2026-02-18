"""
Store MySQL test credentials in Vault
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

# Set environment variables
os.environ.setdefault('ALLOW_ORIGINS', 'http://localhost:8080')
os.environ.setdefault('APP_SRV_ADDR', 'http://localhost:8000')
os.environ.setdefault('HASHICORP_HOST', 'http://127.0.0.1:8200')
os.environ.setdefault('HASHICORP_TOKEN', 'root')

import hvac

def store_mysql_credentials():
    """Store MySQL test credentials in Vault"""
    
    # MySQL test configuration
    mysql_config = {
        'host': 'localhost',
        'port': 3307,
        'database': 'airtable_test',
        'user': 'testuser',
        'password': 'testpass123',
        'connection_type': 'mysql'
    }
    
    # Vault configuration
    vault_addr = os.getenv('HASHICORP_HOST', 'http://127.0.0.1:8200')
    vault_token = os.getenv('HASHICORP_TOKEN', 'root')
    
    # Test configuration
    namespace = "test_user"
    connection_name = "mysql_test"
    
    print("=" * 60)
    print("Storing MySQL Test Credentials in Vault")
    print("=" * 60)
    print()
    print(f"Vault Address: {vault_addr}")
    print(f"Namespace: {namespace}")
    print(f"Connection Name: {connection_name}")
    print(f"MySQL Host: {mysql_config['host']}:{mysql_config['port']}")
    print(f"MySQL Database: {mysql_config['database']}")
    print(f"MySQL User: {mysql_config['user']}")
    print()
    
    try:
        # Connect to Vault
        print("Connecting to Vault...")
        client = hvac.Client(url=vault_addr, token=vault_token)
        
        if not client.is_authenticated():
            print("❌ Failed to authenticate with Vault")
            print("Make sure Vault is running")
            return False
        
        print("✓ Connected to Vault")
        
        # Store secret
        secret_path = f'main/db/{connection_name}'
        
        print(f"Storing secret at: {namespace}/{secret_path}...")
        client.secrets.kv.v2.create_or_update_secret(
            mount_point=namespace,
            path=secret_path,
            secret=mysql_config
        )
        
        print(f"✓ Stored secret successfully")
        
        # Verify secret was stored
        print("Verifying secret storage...")
        read_secret = client.secrets.kv.v2.read_secret_version(
            mount_point=namespace,
            path=secret_path,
            raise_on_deleted_version=False
        )
        
        stored_data = read_secret['data']['data']
        if stored_data['host'] == mysql_config['host'] and stored_data['database'] == mysql_config['database']:
            print("✓ Verified secret storage")
        else:
            print("⚠️  Secret verification mismatch")
        
        print()
        print("=" * 60)
        print("✅ MySQL Credentials Stored!")
        print("=" * 60)
        print()
        print("Next steps:")
        print("  1. Start MySQL: docker-compose -f backend/tests/docker-compose-mysql.yml up -d")
        print("  2. Run integration tests: pytest backend/tests/test_airtable_sql_integration.py")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        print("Troubleshooting:")
        print("1. Make sure Vault is running")
        print("2. Check environment variables:")
        print(f"   HASHICORP_HOST={vault_addr}")
        print(f"   HASHICORP_TOKEN={vault_token}")
        return False


if __name__ == '__main__':
    success = store_mysql_credentials()
    sys.exit(0 if success else 1)
