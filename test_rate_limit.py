#!/usr/bin/env python3
"""
Test rate limiting by sending multiple requests.
"""
import requests
import time

URL = "http://localhost:8000/buckets"

def test_rate_limit():
    print("Testing rate limiting (100 req/min)...")
    print("Sending 110 requests rapidly...\n")
    
    success_count = 0
    rate_limited_count = 0
    
    for i in range(110):
        resp = requests.get(URL)
        
        if resp.status_code == 200:
            success_count += 1
            print(f"Request {i+1}: ✓ 200 OK (Remaining: {resp.headers.get('X-RateLimit-Remaining', 'N/A')})")
        elif resp.status_code == 429:
            rate_limited_count += 1
            print(f"Request {i+1}: ✗ 429 RATE LIMITED")
        else:
            print(f"Request {i+1}: ? {resp.status_code}")
        
        time.sleep(0.05)  # Small delay to avoid connection issues
    
    print(f"\n📊 Results:")
    print(f"   Successful: {success_count}")
    print(f"   Rate Limited: {rate_limited_count}")
    
    if rate_limited_count > 0:
        print(f"\n✅ Rate limiting is working! Got rate limited after ~100 requests.")
    else:
        print(f"\n❌ Rate limiting may not be working correctly.")

if __name__ == "__main__":
    test_rate_limit()
