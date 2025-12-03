from fastapi import APIRouter, Request, Response, Header, HTTPException, Depends, UploadFile, File
from fastapi.responses import Response
from typing import Optional
import datetime
from metadata import MetadataManager
import json
import hashlib
import uuid
from events import manager

# We'll import the main upload/download logic from main.py later or refactor.
# For now, let's assume we can access the logic via the MetadataManager and some shared helpers.
# Ideally, we should refactor main.py to separate logic from API, but for this phase, 
# we will duplicate some small logic or import from main if possible.
# To avoid circular imports, we might need to move core logic to a `core.py` or similar.
# For now, let's just use MetadataManager and re-implement the EC/storage logic calls here 
# or import specific functions if they are clean.

from metadata import MetadataManager
# We need to access the same MetadataManager instance or create a new one.
# Since it's stateless (connects to DB), creating a new one is fine.
meta_mgr = MetadataManager()

router = APIRouter()

def create_xml_response(content: str, status_code: int = 200):
    return Response(content=content, media_type="application/xml", status_code=status_code)

@router.get("/")
async def list_buckets_s3():
    """S3 ListBuckets"""
    buckets = meta_mgr.list_buckets()
    
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
    xml += '  <Owner><ID>planetstore</ID><DisplayName>planetstore</DisplayName></Owner>\n'
    xml += '  <Buckets>\n'
    for b in buckets:
        created_iso = b.created_at.isoformat() if b.created_at else datetime.datetime.utcnow().isoformat()
        xml += '    <Bucket>\n'
        xml += f'      <Name>{b.name}</Name>\n'
        xml += f'      <CreationDate>{created_iso}</CreationDate>\n'
        xml += '    </Bucket>\n'
    xml += '  </Buckets>\n'
    xml += '</ListAllMyBucketsResult>'
    
    return create_xml_response(xml)

@router.put("/{bucket}")
async def create_bucket_s3(bucket: str):
    """S3 CreateBucket"""
    if meta_mgr.get_bucket(bucket):
        # S3 returns 200 if you own it, 409 if someone else does. 
        # We'll just return 200 for simplicity.
        pass
    else:
        meta_mgr.create_bucket(bucket)
    
    return Response(status_code=200)

@router.head("/{bucket}")
async def head_bucket_s3(bucket: str):
    """S3 HeadBucket"""
    if meta_mgr.get_bucket(bucket):
        return Response(status_code=200)
    else:
        return Response(status_code=404)

@router.get("/{bucket}")
async def list_objects_v2_s3(bucket: str, list_type: Optional[str] = None, prefix: Optional[str] = "", max_keys: int = 1000):
    """S3 ListObjectsV2"""
    # Note: This is a simplified implementation.
    # Real S3 has continuation tokens, delimiters, etc.
    
    b = meta_mgr.get_bucket(bucket)
    if not b:
        return Response(status_code=404)
        
    objects = meta_mgr.list_objects(bucket) # This gets all, we should filter by prefix if we can
    
    # Filter by prefix
    if prefix:
        objects = [o for o in objects if o.object_key.startswith(prefix)]
        
    # Truncate to max_keys
    is_truncated = len(objects) > max_keys
    objects = objects[:max_keys]
    
    xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    xml += '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">\n'
    xml += f'  <Name>{bucket}</Name>\n'
    xml += f'  <Prefix>{prefix}</Prefix>\n'
    xml += f'  <KeyCount>{len(objects)}</KeyCount>\n'
    xml += f'  <MaxKeys>{max_keys}</MaxKeys>\n'
    xml += f'  <IsTruncated>{str(is_truncated).lower()}</IsTruncated>\n'
    
    for o in objects:
        last_modified = o.created_at.isoformat() if o.created_at else datetime.datetime.utcnow().isoformat()
        etag = f'"{o.content_hash}"' if hasattr(o, 'content_hash') and o.content_hash else f'"{hashlib.md5(o.object_key.encode()).hexdigest()}"'
        
        xml += '  <Contents>\n'
        xml += f'    <Key>{o.object_key}</Key>\n'
        xml += f'    <LastModified>{last_modified}</LastModified>\n'
        xml += f'    <ETag>{etag}</ETag>\n'
        xml += f'    <Size>{o.size_bytes}</Size>\n'
        xml += '    <StorageClass>STANDARD</StorageClass>\n'
        xml += '  </Contents>\n'
        
    xml += '</ListBucketResult>'
    
    return create_xml_response(xml)

# -------------------------------------------------------------------
# Object Operations
# -------------------------------------------------------------------

import ec
import asyncio
import requests
from config import get_nodes_for_shards, NodeInfo, NODES

@router.put("/{bucket}/{key:path}")
async def put_object_s3(bucket: str, key: str, request: Request):
    """S3 PutObject"""
    # 1. Ensure bucket exists
    b = meta_mgr.get_bucket(bucket)
    if not b:
        # S3 auto-creates buckets? No, usually errors. But for convenience we might?
        # Standard S3 returns NoSuchBucket.
        return Response(status_code=404, content='<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code></Error>', media_type="application/xml")

    # 2. Read body
    # FastAPI Request.stream() or .body()
    body = await request.body()
    size = len(body)
    
    # Calculate MD5/SHA256 for ETag/Dedup
    content_hash = hashlib.sha256(body).hexdigest()
    etag = hashlib.md5(body).hexdigest()

    # 3. Dedup Check
    from sqlalchemy import text
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        existing_content = db_session.execute(
            text("SELECT content_hash, shards FROM content_store WHERE content_hash = :hash"),
            {"hash": content_hash}
        ).fetchone()
        
        if existing_content:
            # Dedup hit!
            db_session.execute(
                text("UPDATE content_store SET refcount = refcount + 1 WHERE content_hash = :hash"),
                {"hash": content_hash}
            )
            db_session.commit()
            
            obj = meta_mgr.put_object_metadata(
                bucket=bucket,
                key=key,
                size=size,
                shards=existing_content[1]
            )
            
            # Update object with content_hash
            db_session.execute(
                text("UPDATE objects SET content_hash = :hash WHERE id = :obj_id"),
                {"hash": content_hash, "obj_id": obj.id}
            )
            db_session.commit()
            
            await manager.broadcast({
                "type": "upload",
                "bucket": bucket,
                "key": key,
                "size": size,
                "deduplicated": True,
                "method": "s3"
            })
            
            return Response(status_code=200, headers={"ETag": f'"{etag}"'})
            
    finally:
        db_session.close()

    # 4. EC Encode
    shards_data = ec.encode_data(body)
    total_shards = len(shards_data)
    
    # 5. Distribute
    nodes = get_nodes_for_shards(total_shards)
    quorum_size = 4 # Strong consistency by default for S3
    
    async def upload_shard(i: int, node: NodeInfo, shard_val: bytes):
        try:
            shard_key = f"{bucket}/{key}/{obj_uuid}/{i}" # Wait, we need UUID first? 
            # Actually main.py generates UUID in metadata *after* upload? 
            # No, main.py generates shards then uploads, then metadata.
            # But where does it get the unique ID for storage?
            # main.py uses: shard_key = f"{bucket}/{key}/{i}" which is NOT unique per version!
            # Wait, main.py logic:
            # shard_key = f"{bucket}/{key}/{i}"
            # This overwrites shards on storage nodes!
            # If we have versioning, we should probably include version_id in shard key?
            # main.py doesn't seem to use version_id in shard key. 
            # Let's check main.py logic again.
            pass
        except:
            pass
            
    # Re-checking main.py logic:
    # shard_key = f"{bucket}/{key}/{i}"
    # This means new versions overwrite old shards on disk! 
    # This breaks versioning for the actual data!
    # We should fix this. But for now, let's match main.py behavior or improve it?
    # If I improve it here, main.py is still broken.
    # Let's stick to main.py behavior for now to be consistent, 
    # but note this as a bug to fix later (using UUID in shard key).
    
    # Actually, let's use a UUID for the shard key to be safe, 
    # but we need to store that in metadata.
    # main.py stores: shard_meta = [{"index": r["index"], "node_id": r["node_id"], "shard_key": r["shard_key"]} ...]
    # So if we change shard_key here, and store it in metadata, it works!
    # So I WILL use a UUID here to avoid overwrites.
    
    upload_uuid = str(uuid.uuid4())
    
    async def upload_shard_real(i: int, node: NodeInfo, shard_val: bytes):
        shard_key = f"{key}/{upload_uuid}/{i}" 
        url = f"{node.base_url}/internal/objects/{bucket}/{shard_key}"
        try:
            # We need to run this in a threadpool because requests is sync
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: requests.put(
                    url,
                    files={"file": ("shard", shard_val, "application/octet-stream")},
                    timeout=10
                )
            )
            if resp.status_code == 200:
                return {"success": True, "index": i, "node_id": node.node_id, "shard_key": shard_key}
            else:
                print(f"UPLOAD ERROR {node.node_id}: {resp.status_code} - {resp.text}")
                return {"success": False, "index": i, "node_id": node.node_id, "error": f"Status {resp.status_code}"}
        except Exception as e:
            print(f"UPLOAD EXCEPTION {node.node_id}: {e}")
            return {"success": False, "index": i, "node_id": node.node_id, "error": str(e)}

    tasks = [upload_shard_real(i, nodes[i], shards_data[i]) for i in range(total_shards)]
    results = await asyncio.gather(*tasks)
    
    successful = [r for r in results if r["success"]]
    if len(successful) < quorum_size:
        return Response(status_code=502, content='<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>Quorum failed</Message></Error>', media_type="application/xml")

    # 6. Store Content & Metadata
    shard_meta = [{"index": r["index"], "node_id": r["node_id"], "shard_key": r["shard_key"]} for r in successful]
    shards_json = json.dumps(shard_meta)
    
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        db_session.execute(
            text("INSERT INTO content_store (content_hash, size_bytes, shards, refcount) VALUES (:hash, :size, :shards, 1)"),
            {"hash": content_hash, "size": size, "shards": shards_json}
        )
        db_session.commit()
    finally:
        db_session.close()
        
    obj = meta_mgr.put_object_metadata(
        bucket=bucket,
        key=key,
        size=size,
        shards=shards_json
    )
    
    # Update object with content_hash
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        db_session.execute(
            text("UPDATE objects SET content_hash = :hash WHERE id = :obj_id"),
            {"hash": content_hash, "obj_id": obj.id}
        )
        db_session.commit()
    finally:
        db_session.close()
        
    await manager.broadcast({
        "type": "upload",
        "bucket": bucket,
        "key": key,
        "size": size,
        "deduplicated": False,
        "method": "s3"
    })
        
    return Response(status_code=200, headers={"ETag": f'"{etag}"'})

@router.get("/{bucket}/{key:path}")
async def get_object_s3(bucket: str, key: str):
    """S3 GetObject"""
    obj = meta_mgr.get_object_metadata(bucket, key)
    if not obj:
        return Response(status_code=404, content='<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code></Error>', media_type="application/xml")
        
    # Decode shards
    shards_info = json.loads(obj.shards)
    
    # Fetch shards
    # We need K shards.
    K = 4
    retrieved_shards = []
    retrieved_indices = []
    
    # Sort by index to try primary shards first? Or just any K.
    # We need to fetch in parallel.
    
    async def fetch_shard(info):
        node = NODES.get(info["node_id"])
        if not node: return None
        
        url = f"{node.base_url}/internal/objects/{bucket}/{info['shard_key']}"
        
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: requests.get(url, timeout=5))
            if resp.status_code == 200:
                return {"index": info["index"], "data": resp.content}
            else:
                print(f"FETCH ERROR {node.node_id}: {resp.status_code} - {url}")
        except Exception as e:
            print(f"FETCH EXCEPTION {node.node_id}: {e}")
            pass
        return None

    # Try all shards
    tasks = [fetch_shard(info) for info in shards_info]
    results = await asyncio.gather(*tasks)
    
    for r in results:
        if r:
            retrieved_shards.append(r["data"])
            retrieved_indices.append(r["index"])
            if len(retrieved_shards) >= K:
                break
                
    if len(retrieved_shards) < K:
        return Response(status_code=502, content='<?xml version="1.0" encoding="UTF-8"?><Error><Code>InternalError</Code><Message>Cannot recover object</Message></Error>', media_type="application/xml")
        
    original_data = ec.decode_data(retrieved_shards, retrieved_indices, obj.size_bytes)
    
    etag = f'"{obj.content_hash}"' if hasattr(obj, 'content_hash') and obj.content_hash else f'"{hashlib.md5(original_data).hexdigest()}"'
    
    return Response(
        content=original_data, 
        media_type="application/octet-stream",
        headers={
            "ETag": etag,
            "Content-Length": str(len(original_data)),
            "Last-Modified": obj.created_at.strftime("%a, %d %b %Y %H:%M:%S GMT") if obj.created_at else ""
        }
    )

@router.head("/{bucket}/{key:path}")
async def head_object_s3(bucket: str, key: str):
    """S3 HeadObject"""
    obj = meta_mgr.get_object_metadata(bucket, key)
    if not obj:
        return Response(status_code=404)
        
    etag = f'"{obj.content_hash}"' if hasattr(obj, 'content_hash') and obj.content_hash else ""
    
    return Response(
        status_code=200,
        headers={
            "ETag": etag,
            "Content-Length": str(obj.size_bytes),
            "Last-Modified": obj.created_at.strftime("%a, %d %b %Y %H:%M:%S GMT") if obj.created_at else "",
            "Content-Type": "application/octet-stream"
        }
    )
