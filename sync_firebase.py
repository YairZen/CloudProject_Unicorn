import requests
import json
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, db
import os

# ============== FIREBASE INITIALIZATION ==============
# Initialize Firebase using credentials
cred = credentials.Certificate('firebase_key.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://cloud-81451-default-rtdb.europe-west1.firebasedatabase.app/'
})

# ============== API CONFIGURATION ==============
BASE_URL = "https://server-cloud-v645.onrender.com/"
FEED = "json"
BATCH_LIMIT = 200
EARLIEST_DATE = "2025-10-01T00:00:00Z"

# ============== HELPER FUNCTIONS ==============
def get_latest_timestamp_from_firebase():
    """Get the most recent timestamp stored in Firebase"""
    try:
        ref = db.reference('/sensor_data')
        latest = ref.order_by_child('created_at').limit_to_last(1).get()
        
        if latest:
            latest_key = list(latest.keys())[0]
            return latest[latest_key]['created_at']
    except Exception as e:
        print(f"Error getting latest timestamp: {e}")
    return None

def fetch_batch(before_timestamp=None):
    """Fetch a batch of data from the API"""
    params = {
        "feed": FEED,
        "limit": BATCH_LIMIT
    }
    
    if before_timestamp:
        params["before_created_at"] = before_timestamp
    
    response = requests.get(f"{BASE_URL}/history", params=params)
    return response.json()

def save_to_firebase(data_list):
    """Save data to Firebase"""
    ref = db.reference('/sensor_data')
    
    saved_count = 0
    for sample in data_list:
        timestamp_key = sample['created_at'].replace(':', '-').replace('.', '-')
        sensor_values = json.loads(sample['value'])
        
        record = {
            'created_at': sample['created_at'],
            'temperature': sensor_values['temperature'],
            'humidity': sensor_values['humidity'],
            'soil': sensor_values['soil']
        }
        
        ref.child(timestamp_key).set(record)
        saved_count += 1
        
        if saved_count % 100 == 0:
            print(f"  💾 Saved {saved_count}/{len(data_list)} records...")
    
    return saved_count

def download_all_data():
    """Download all historical data in batches"""
    print(f"🔄 Starting data download from now back to {EARLIEST_DATE}...")
    all_data = []
    previous_oldest = None
    
    response = fetch_batch()
    
    if "data" not in response:
        print("❌ Error fetching initial data:", response)
        return []
    
    all_data.extend(response["data"])
    print(f"✓ Fetched initial {len(response['data'])} samples.")
    
    batch_count = 1
    stuck_count = 0
    
    while True:
        if not all_data:
            break
        
        before_timestamp = all_data[-1]["created_at"]
        
        if before_timestamp == previous_oldest:
            stuck_count += 1
            if stuck_count >= 2:
                print(f"✓ No more older data available")
                break
        else:
            stuck_count = 0
        
        previous_oldest = before_timestamp
        
        if before_timestamp <= EARLIEST_DATE:
            print(f"✓ Reached earliest date limit: {EARLIEST_DATE}")
            all_data = [sample for sample in all_data if sample["created_at"] >= EARLIEST_DATE]
            break
        
        response = fetch_batch(before_timestamp)
        
        if "data" in response and len(response["data"]) > 0:
            batch_count += 1
            
            filtered_batch = [sample for sample in response["data"] 
                            if sample["created_at"] >= EARLIEST_DATE 
                            and sample["created_at"] < before_timestamp]
            
            if filtered_batch:
                all_data.extend(filtered_batch)
                print(f"✓ Batch {batch_count}: {len(filtered_batch)} samples. Total: {len(all_data)}")
                
                if len(filtered_batch) < len(response["data"]):
                    print(f"✓ Reached earliest date limit")
                    break
            else:
                print(f"✓ No new data in batch")
                break
        else:
            print("✓ No more data available")
            break
        
        if batch_count > 1000:
            print(f"⚠️  Safety limit reached")
            break
    
    return all_data

def download_new_data(latest_timestamp):
    """Download only data newer than the latest timestamp"""
    print(f"🔄 Downloading data newer than {latest_timestamp}...")
    new_data = []
    
    response = fetch_batch()
    
    if "data" not in response:
        print("❌ Error fetching data:", response)
        return []
    
    for sample in response["data"]:
        if sample["created_at"] > latest_timestamp:
            new_data.append(sample)
        else:
            print(f"✓ Reached existing data at {sample['created_at']}")
            return new_data
    
    print(f"✓ Fetched {len(new_data)} new samples.")
    return new_data

# ============== MAIN EXECUTION ==============
def main():
    print("=" * 60)
    print("🔥 SENSOR DATA FIREBASE SYNC")
    print(f"⏰ Run time: {datetime.now().isoformat()}")
    print("=" * 60)
    
    latest_timestamp = get_latest_timestamp_from_firebase()
    
    if latest_timestamp:
        print(f"\n✓ Found existing data in Firebase.")
        print(f"📅 Latest timestamp: {latest_timestamp}")
        print("\n🔄 Fetching only new data...")
        
        new_data = download_new_data(latest_timestamp)
        
        if new_data:
            print(f"\n💾 Saving {len(new_data)} new samples to Firebase...")
            saved = save_to_firebase(new_data)
            print(f"\n✅ {saved} new records saved!")
        else:
            print("\n✅ No new data. Database is up to date!")
    else:
        print("\n⚠️  No existing data found.")
        print("🔄 Starting full download...\n")
        
        all_data = download_all_data()
        
        if all_data:
            print(f"\n📊 Summary:")
            print(f"  Total: {len(all_data)}")
            print(f"  Earliest: {all_data[-1]['created_at']}")
            print(f"  Latest: {all_data[0]['created_at']}")
            
            print(f"\n💾 Saving {len(all_data)} samples...")
            saved = save_to_firebase(all_data)
            print(f"\n✅ {saved} records saved!")
        else:
            print("\n❌ No data downloaded.")
    
    print("\n" + "=" * 60)
    print("✅ SYNC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
