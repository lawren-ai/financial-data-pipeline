import os
from dotenv import load_dotenv
from datetime import date
import pytz

# 1. Load your credentials
load_dotenv()

# 2. Import your actual functions
# Assuming your file is named utils/market_calendar.py
try:
    from utils.market_calendar import (
        is_market_open_today, 
        is_market_open_now, 
        get_next_trading_day
    )
    print("✅ Import successful!")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    exit()

print("=" * 60)
print(f"RUNNING SMOKE TEST - CURRENT DATE: {date.today()}")
print("=" * 60)

# --- Test 1: Database Connection via Calendar ---
print("\n1. Testing Database & Holiday Logic...")
try:
    market_status = is_market_open_today()
    print(f"   ✓ is_market_open_today(): {market_status}")
    print(f"   (Note: Since today is Wednesday Jan 14, this should be True)")
except Exception as e:
    print(f"   ❌ Database/Logic Error: {e}")
    print("   Tip: Check if 'market_calendar' table exists and has 2026 data.")

# --- Test 2: Market Hours Logic ---
print("\n2. Testing Market Hours (NYC Time)...")
try:
    # Get current NYC time for manual verification
    et_tz = pytz.timezone('America/New_York')
    from datetime import datetime
    now_et = datetime.now(et_tz)
    
    is_open = is_market_open_now()
    print(f"   ✓ Current Time (ET): {now_et.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   ✓ is_market_open_now(): {is_open}")
    print(f"   (Expect True if between 9:30 AM and 4:00 PM ET)")
except Exception as e:
    print(f"   ❌ Hours Logic Error: {e}")

# --- Test 3: Next Trading Day ---
print("\n3. Testing Calendar Sequencing...")
try:
    next_day = get_next_trading_day()
    print(f"   ✓ Next trading day: {next_day}")
except Exception as e:
    print(f"   ❌ Sequencing Error: {e}")

print("\n" + "=" * 60)
print("SMOKE TEST COMPLETE")
print("=" * 60)