#!/usr/bin/env python3
"""
Test script for Store Monitoring API
"""

import requests
import json
import time
import sys

# API base URL
BASE_URL = "http://localhost:8000"

def test_root_endpoint():
    """Test the root endpoint"""
    print("🔍 Testing root endpoint...")
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Root endpoint: {data}")
            return True
        else:
            print(f"❌ Root endpoint failed: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API server. Is it running?")
        return False
    except Exception as e:
        print(f"❌ Error testing root endpoint: {e}")
        return False

def test_trigger_report():
    """Test report generation trigger"""
    print("\n🔍 Testing report generation trigger...")
    try:
        response = requests.post(f"{BASE_URL}/trigger_report")
        if response.status_code == 200:
            data = response.json()
            report_id = data.get('report_id')
            print(f"✅ Report triggered successfully: {report_id}")
            return report_id
        else:
            print(f"❌ Report trigger failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error testing report trigger: {e}")
        return None

def test_get_report(report_id):
    """Test getting report status"""
    print(f"\n🔍 Testing get report for ID: {report_id}")
    
    max_attempts = 10
    attempt = 0
    
    while attempt < max_attempts:
        try:
            response = requests.get(f"{BASE_URL}/get_report/{report_id}")
            
            if response.status_code == 200:
                if response.headers.get('content-type') == 'text/csv':
                    print("✅ Report completed and downloaded!")
                    return True
                else:
                    data = response.json()
                    status = data.get('status')
                    print(f"📊 Report status: {status}")
                    
                    if status == "Complete":
                        print("✅ Report generation completed!")
                        return True
                    elif status == "Failed":
                        print("❌ Report generation failed!")
                        return False
                    elif status == "Running":
                        print("⏳ Report still running... waiting 5 seconds...")
                        time.sleep(5)
                        attempt += 1
                        continue
                    else:
                        print(f"❓ Unknown status: {status}")
                        return False
            else:
                print(f"❌ Get report failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Error testing get report: {e}")
            return False
    
    print("⏰ Timeout waiting for report completion")
    return False

def main():
    """Main test function"""
    print("🧪 Store Monitoring API Test Suite")
    print("=" * 50)
    
    # Test 1: Root endpoint
    if not test_root_endpoint():
        print("\n❌ Root endpoint test failed. Stopping tests.")
        return 1
    
    # Test 2: Trigger report
    report_id = test_trigger_report()
    if not report_id:
        print("\n❌ Report trigger test failed. Stopping tests.")
        return 1
    
    # Test 3: Get report status
    if not test_get_report(report_id):
        print("\n❌ Get report test failed.")
        return 1
    
    print("\n🎉 All tests passed successfully!")
    return 0

if __name__ == "__main__":
    print("💡 Make sure the API server is running (python main.py)")
    print("💡 Then run this test script in another terminal")
    print("=" * 50)
    
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
        sys.exit(1)
