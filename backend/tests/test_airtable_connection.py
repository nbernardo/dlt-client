"""
Simple test to verify Airtable connection without Vault
"""

import json
from pathlib import Path

def test_airtable_connection():
    """Test direct connection to Airtable"""
    
    print("=" * 60)
    print("Testing Airtable Connection")
    print("=" * 60)
    print()
    
    # Load test config
    config_path = Path(__file__).parent / 'test_config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    airtable_config = config['airtable']
    api_key = airtable_config['api_key']
    base_id = airtable_config['base_id']
    
    print(f"Base ID: {base_id}")
    print(f"API Key: {api_key[:15]}...{api_key[-10:]}")
    print()
    
    try:
        from pyairtable import Api
        
        print("Connecting to Airtable...")
        api = Api(api_key)
        base = api.base(base_id)
        print("✓ Connected to Airtable")
        print()
        
        # Try to access tables
        test_tables = airtable_config.get('test_tables', ['Customers', 'Products', 'Orders'])
        found_tables = []
        
        for table_name in test_tables:
            try:
                print(f"Checking table: {table_name}...")
                table = base.table(table_name)
                records = table.all(max_records=5)
                found_tables.append(table_name)
                print(f"  ✓ Table '{table_name}' - {len(records)} records found")
                
                # Show first record structure
                if records:
                    first_record = records[0]
                    fields = list(first_record['fields'].keys())
                    print(f"    Fields: {', '.join(fields[:5])}{' ...' if len(fields) > 5 else ''}")
                
            except Exception as e:
                print(f"  ✗ Table '{table_name}' - {str(e)}")
        
        print()
        if found_tables:
            print("=" * 60)
            print(f"✅ Success! Found {len(found_tables)} table(s)")
            print("=" * 60)
            print()
            print("Your Airtable connection is working!")
            print(f"Accessible tables: {', '.join(found_tables)}")
            print()
            print("Next steps:")
            print("1. Install Vault for production use")
            print("2. Or continue testing with test_config.json")
            return True
        else:
            print("=" * 60)
            print("⚠️  No tables found")
            print("=" * 60)
            print()
            print("Make sure your Airtable base has tables with these names:")
            print(f"  {', '.join(test_tables)}")
            return False
            
    except ImportError:
        print("❌ pyairtable not installed")
        print("Install with: pip install pyairtable")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == '__main__':
    import sys
    success = test_airtable_connection()
    sys.exit(0 if success else 1)
