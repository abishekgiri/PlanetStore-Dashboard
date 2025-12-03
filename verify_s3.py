import boto3
import os

# Configure boto3 to use our local S3 endpoint
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:8000/s3/',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

BUCKET = 's3-test-bucket'
KEY = 'hello.txt'
CONTENT = b'Hello from boto3!'

def test_s3():
    print("1. Creating bucket...")
    try:
        s3.create_bucket(Bucket=BUCKET)
        print("   Bucket created.")
    except Exception as e:
        print(f"   Error (might exist): {e}")

    print("\n2. Listing buckets...")
    resp = s3.list_buckets()
    buckets = [b['Name'] for b in resp['Buckets']]
    print(f"   Buckets: {buckets}")
    assert BUCKET in buckets

    print(f"\n3. Uploading object '{KEY}'...")
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    print("   Upload complete.")

    print(f"\n4. Listing objects in '{BUCKET}'...")
    resp = s3.list_objects_v2(Bucket=BUCKET)
    keys = [o['Key'] for o in resp.get('Contents', [])]
    print(f"   Objects: {keys}")
    assert KEY in keys

    print(f"\n5. Downloading object '{KEY}'...")
    resp = s3.get_object(Bucket=BUCKET, Key=KEY)
    data = resp['Body'].read()
    print(f"   Content: {data}")
    assert data == CONTENT
    
    print("\n6. Head Object...")
    resp = s3.head_object(Bucket=BUCKET, Key=KEY)
    print(f"   ETag: {resp.get('ETag')}")
    print(f"   ContentLength: {resp.get('ContentLength')}")
    assert resp['ContentLength'] == len(CONTENT)

    print("\n✅ S3 Compatibility Test Passed!")

if __name__ == "__main__":
    test_s3()
