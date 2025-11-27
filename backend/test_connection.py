from supabase_processor import SupabaseProcessor

# Use your actual credentials
SUPABASE_URL = "https://xnraltsvlgxvddumkmuc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhucmFsdHN2bGd4dmRkdW1rbXVjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjQyMDAyNzksImV4cCI6MjA3OTc3NjI3OX0.TGg69kE4VhxFaJlOmxW_VH6iQWg9rvwa_G-FQ7_PKYA"  # Replace with your actual key

processor = SupabaseProcessor(SUPABASE_URL, SUPABASE_KEY)

print("🧪 Testing Supabase connection with lowercase table names...")
print("=" * 60)

# Test each table
tables = [
    'dimairlines', 
    'dimairports', 
    'dimpassengers', 
    'dimflights', 
    'dimdate', 
    'factsales', 
    'dirtydata'
]

for table in tables:
    print(f"Testing {table}...", end=" ")
    response = processor._make_request(table)
    if response and response.status_code == 200:
        data = response.json()
        print(f"✅ SUCCESS - {len(data)} records")
    else:
        status_code = response.status_code if response else "No response"
        print(f"❌ FAILED - Status: {status_code}")

print("=" * 60)
print("🎯 If all tables show ✅ SUCCESS, your connection is working!")
print("📊 You can now run: python upload_datasets.py")
