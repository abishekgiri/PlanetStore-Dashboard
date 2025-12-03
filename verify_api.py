import requests
import sys

BASE_URL = "http://localhost:8000"

def test_health():
    print("Testing /health...")
    try:
        resp = requests.get(f"{BASE_URL}/health")
        resp.raise_for_status()
        data = resp.json()
        print("OK:", data)
        if data.get("mode") != "erasure_coding":
            print("FAIL: Expected mode 'erasure_coding'")
            sys.exit(1)
        if len(data.get("nodes", [])) != 6:
            print(f"FAIL: Expected 6 nodes, got {len(data.get('nodes', []))}")
            sys.exit(1)
    except Exception as e:
        print("FAIL:", e)
        sys.exit(1)

def test_cors():
    print("Testing CORS headers...")
    try:
        resp = requests.options(f"{BASE_URL}/buckets", headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET"
        })
        # FastAPI CORS middleware handles OPTIONS
        # Check for Access-Control-Allow-Origin
        if "access-control-allow-origin" in resp.headers:
             print("OK: CORS header found:", resp.headers["access-control-allow-origin"])
        else:
             # Sometimes it's only on actual requests depending on config, but let's check GET too
             resp = requests.get(f"{BASE_URL}/health", headers={"Origin": "http://localhost:5173"})
             if "access-control-allow-origin" in resp.headers:
                 print("OK: CORS header found on GET:", resp.headers["access-control-allow-origin"])
             else:
                 print("WARNING: CORS header missing. Headers:", resp.headers)
    except Exception as e:
        print("FAIL:", e)

if __name__ == "__main__":
    test_health()
    test_cors()
