# gateway/main.py
import json
import os
import time
import asyncio
from typing import List, Optional, Dict

import requests
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# New modules
from metadata import MetadataManager, Object, Bucket
import ec

# -------------------------------------------------------------------
# Config & Node Registry
# -------------------------------------------------------------------

from config import NodeInfo, NODES, NODES_LIST, get_nodes_for_shards
from events import manager
from scheduler import GCScheduler, gc_scheduler as _gc_scheduler
from fastapi import WebSocket, WebSocketDisconnect
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="PlanetStore Gateway (Advanced)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting
from rate_limiter import RateLimitMiddleware, rate_limiter
app.add_middleware(RateLimitMiddleware, rate_limiter=rate_limiter)

# Global GC scheduler instance
gc_scheduler_instance = None
health_monitor_instance = None

@app.on_event("startup")
async def startup_event():
    """Initialize scheduler on startup."""
    global gc_scheduler_instance
    logger.info("Starting up PlanetStore Gateway...")
    
    # Start GC scheduler (runs every 1 hour by default)
    from sqlalchemy import text
    
    def run_gc():
        """GC function to run on schedule."""
        db = meta_mgr.get_db()
        db_session = next(db)
        try:
            # Find old versions to clean up (>7 days old, not latest)
            cutoff = text("NOW() - INTERVAL '7 days'")
            old_objects = db_session.execute(text(
                "SELECT id, bucket_name, object_key, shards FROM objects WHERE is_latest = false AND created_at < NOW() - INTERVAL '7 days'"
            )).fetchall()
            
            deleted_count = 0
            for obj in old_objects:
                # Delete shards
                shards = json.loads(obj[3])
                for shard in shards:
                    node = NODES.get(shard["node_id"])
                    if node:
                        try:
                            requests.delete(f"{node.base_url}/internal/objects/{obj[1]}/{shard['shard_key']}", timeout=2)
                        except:
                            pass
                
                # Delete metadata
                db_session.execute(text("DELETE FROM objects WHERE id = :id"), {"id": obj[0]})
                deleted_count += 1
            
            db_session.commit()
            return {"deleted_versions": deleted_count}
        finally:
            db_session.close()
    
    gc_scheduler_instance = GCScheduler(run_gc, interval_hours=1)
    gc_scheduler_instance.start()
    logger.info("GC Scheduler started successfully")
    
    # Start Health Monitor
    from health_monitor import HealthMonitor, health_monitor as _hm
    global health_monitor_instance
    health_monitor_instance = HealthMonitor(check_interval_seconds=30)
    health_monitor_instance.start()
    logger.info("Health Monitor started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    """Shutdown scheduler gracefully."""
    global gc_scheduler_instance, health_monitor_instance
    logger.info("Shutting down PlanetStore Gateway...")
    if gc_scheduler_instance:
        gc_scheduler_instance.shutdown()
    if health_monitor_instance:
        health_monitor_instance.shutdown()
    logger.info("Shutdown complete")

@app.websocket("/ws/events")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Mount S3 API
from s3_api import router as s3_router
app.include_router(s3_router, prefix="/s3")

# -------------------------------------------------------------------
# Metadata Manager
# -------------------------------------------------------------------

meta_mgr = MetadataManager()

# -------------------------------------------------------------------
# Authentication Endpoints
# -------------------------------------------------------------------

from auth import (
    authenticate_user, create_user, create_access_token,
    Token, UserCreate, get_current_user, update_last_login
)
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta

@app.post("/auth/register")
def register(user_data: UserCreate):
    """Register a new user."""
    try:
        user = create_user(meta_mgr, user_data.username, user_data.email, user_data.password)
        return {"status": "success", "user": user}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Registration failed: {str(e)}")

@app.post("/auth/login", response_model=Token)
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Login and get access token."""
    user = authenticate_user(meta_mgr, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Update last login
    update_last_login(meta_mgr, user["username"])
    
    # Create token
    access_token = create_access_token(
        data={"sub": user["username"]},
        expires_delta=timedelta(minutes=24 * 60)
    )
    
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/auth/me")
async def get_me(current_user = Depends(lambda token=Depends(get_current_user): get_current_user(token, meta_mgr))):
    """Get current logged-in user."""
    if not current_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return current_user

# -------------------------------------------------------------------
# Schemas
# -------------------------------------------------------------------

class BucketCreate(BaseModel):
    name: str
    versioning: bool = False

class BucketInfo(BaseModel):
    name: str
    versioning_enabled: bool

class ObjectInfoSchema(BaseModel):
    key: str
    size_bytes: int
    version_id: str
    is_latest: bool
    shards_count: int

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def get_nodes_for_shards(count: int, preferred_region: str = None) -> List[NodeInfo]:
    """
    Select 'count' nodes for placing shards.
    If preferred_region is specified, try to select nodes from that region first.
    """
    available_nodes = list(NODES.values())
    if len(available_nodes) < count:
        raise HTTPException(status_code=500, detail=f"Not enough nodes available. Need {count}, have {len(available_nodes)}")
    
    selected_nodes = []
    
    # 1. Try to pick from preferred region first
    if preferred_region:
        region_nodes = [n for n in available_nodes if n.region == preferred_region]
        # Shuffle to distribute load
        random.shuffle(region_nodes)
        selected_nodes.extend(region_nodes[:count])
        
    # 2. Fill remaining slots with other nodes
    if len(selected_nodes) < count:
        remaining_count = count - len(selected_nodes)
        other_nodes = [n for n in available_nodes if n not in selected_nodes]
        random.shuffle(other_nodes)
        selected_nodes.extend(other_nodes[:remaining_count])
    
    # Just in case we still don't have enough (should be caught by initial check, but logic might vary)
    if len(selected_nodes) < count:
         raise HTTPException(status_code=500, detail="Not enough nodes to satisfy replication requirement")

    return selected_nodes

# -------------------------------------------------------------------
# API endpoints
# -------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "nodes": list(NODES.keys()), "mode": "erasure_coding"}

@app.get("/nodes")
def list_nodes():
    # Return simple list for now, stats logic removed for simplicity in this phase
    return [{"node_id": n.node_id, "base_url": n.base_url, "latency_ms": 0, "object_count": 0} for n in NODES_LIST]

@app.get("/admin/metrics")
def get_metrics():
    """Comprehensive cluster metrics for monitoring dashboard"""
    from sqlalchemy import text, func
    
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        # Total storage stats
        total_objects = db_session.execute(text("SELECT COUNT(*) FROM objects WHERE is_latest = true")).scalar()
        total_size = db_session.execute(text("SELECT COALESCE(SUM(size_bytes), 0) FROM objects WHERE is_latest = true")).scalar()
        total_versions = db_session.execute(text("SELECT COUNT(*) FROM objects")).scalar()
        
        # Dedup stats
        unique_content = db_session.execute(text("SELECT COUNT(*) FROM content_store")).scalar()
        total_refcount = db_session.execute(text("SELECT COALESCE(SUM(refcount), 0) FROM content_store")).scalar()
        dedup_savings = (total_refcount - unique_content) / max(total_refcount, 1) * 100 if total_refcount > 0 else 0
        
        # Per-bucket stats
        bucket_stats = db_session.execute(text("""
            SELECT bucket_name, COUNT(*) as object_count, SUM(size_bytes) as total_size
            FROM objects WHERE is_latest = true
            GROUP BY bucket_name
        """)).fetchall()
        
        # Storage distribution (per node, approximate)
        node_distribution = []
        for node in NODES_LIST:
            # Count shards on this node
            shard_count = db_session.execute(text("""
                SELECT COUNT(*) FROM content_store
                WHERE shards LIKE :pattern
            """), {"pattern": f'%"node_id": "{node.node_id}"%'}).scalar()
            
            # Get health status from monitor
            global health_monitor_instance
            node_status = "unknown"
            if health_monitor_instance and node.node_id in health_monitor_instance.node_health:
                node_status = health_monitor_instance.node_health[node.node_id].status
            
            node_distribution.append({
                "node_id": node.node_id,
                "shard_count": shard_count,
                "status": node_status
            })
        
        return {
            "cluster": {
                "total_objects": total_objects,
                "total_size_bytes": total_size,
                "total_versions": total_versions,
                "unique_content": unique_content,
                "dedup_savings_percent": round(dedup_savings, 2),
                "node_count": len(NODES_LIST)
            },
            "buckets": [{"name": b[0], "objects": b[1], "size_bytes": b[2]} for b in bucket_stats],
            "nodes": node_distribution
        }
    finally:
        db_session.close()

@app.get("/admin/gc/status")
def get_gc_status():
    """Get GC scheduler status."""
    global gc_scheduler_instance
    if not gc_scheduler_instance:
        return {"error": "GC scheduler not initialized"}
    return gc_scheduler_instance.get_status()

@app.get("/admin/regions")
def get_regions():
    """Get multi-region topology."""
    from config import REGIONS, NODE_TO_REGION
    from sqlalchemy import text
    
    # Get shard distribution per region
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        region_stats = {}
        for region, node_ids in REGIONS.items():
            shard_count = 0
            for node_id in node_ids:
                count = db_session.execute(text("""
                    SELECT COUNT(*) FROM content_store
                    WHERE shards LIKE :pattern
                """), {"pattern": f'%"node_id": "{node_id}"%'}).scalar()
                shard_count += count
            
            region_stats[region] = {
                "nodes": node_ids,
                "shard_count": shard_count
            }
        
        return {
            "regions": region_stats,
            "node_to_region": NODE_TO_REGION
        }
    finally:
        db_session.close()

@app.get("/admin/health")
def get_node_health(node_id: str = None):
    """Get health status of storage nodes."""
    global health_monitor_instance
    if not health_monitor_instance:
        return {"error": "Health monitor not initialized"}
    return health_monitor_instance.get_health_status(node_id)

@app.get("/buckets/{bucket}/quota")
def get_bucket_quota(bucket: str):
    """Get quota for a bucket."""
    from quota_manager import quota_manager
    quota = quota_manager.get_quota(meta_mgr, bucket)
    
    # Also get current usage
    from sqlalchemy import text
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        stats = db_session.execute(text("""
            SELECT COUNT(*), COALESCE(SUM(size_bytes), 0)
            FROM objects WHERE bucket_name = :bucket AND is_latest = true
        """), {"bucket": bucket}).fetchone()
        
        return {
            "bucket": bucket,
            "quota": quota,
            "usage": {
                "objects": stats[0],
                "size_bytes": stats[1]
            },
            "usage_percent": {
                "objects": round((stats[0] / quota["max_objects"]) * 100, 2),
                "size": round((stats[1] / quota["max_size_bytes"]) * 100, 2)
            }
        }
    finally:
        db_session.close()

@app.put("/buckets/{bucket}/quota")
def set_bucket_quota(bucket: str, max_size_gb: float, max_objects: int):
    """Set custom quota for a bucket."""
    from quota_manager import quota_manager
    max_size_bytes = int(max_size_gb * 1024 * 1024 * 1024)
    quota_manager.set_quota(meta_mgr, bucket, max_size_bytes, max_objects)
    return {"status": "ok", "bucket": bucket, "max_size_bytes": max_size_bytes, "max_objects": max_objects}

# --- Buckets ---

@app.post("/buckets", response_model=BucketInfo)
def create_bucket(payload: BucketCreate):
    try:
        bucket = meta_mgr.create_bucket(payload.name, payload.versioning)
        return BucketInfo(name=bucket.name, versioning_enabled=bucket.versioning_enabled)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/buckets", response_model=List[BucketInfo])
def list_buckets():
    buckets = meta_mgr.list_buckets()
    return [BucketInfo(name=b.name, versioning_enabled=b.versioning_enabled) for b in buckets]

# --- Objects ---

@app.get("/buckets/{bucket}/objects", response_model=List[ObjectInfoSchema])
def list_objects(bucket: str):
    objs = meta_mgr.list_objects(bucket)
    results = []
    for o in objs:
        shards = json.loads(o.shards)
        results.append(ObjectInfoSchema(
            key=o.object_key,
            size_bytes=o.size_bytes,
            version_id=o.version_id,
            is_latest=o.is_latest,
            shards_count=len(shards)
        ))
    return results

@app.put("/buckets/{bucket}/objects/{key:path}")
async def upload_object(bucket: str, key: str, file: UploadFile = File(...), consistency: str = "eventual", region: str = None):
    """
    Upload object with configurable consistency and content deduplication.
    consistency: 'strong' (quorum 4/6) or 'eventual' (best-effort all 6)
    """
    import hashlib
    from sqlalchemy import text
    
    # 1. Ensure bucket exists
    b = meta_mgr.get_bucket(bucket)
    if not b:
        b = meta_mgr.create_bucket(bucket)

    # 2. Read file and calculate hash
    file_bytes = await file.read()
    size = len(file_bytes)
    content_hash = hashlib.sha256(file_bytes).hexdigest()
    
    # 2.5 Check quota BEFORE processing
    from quota_manager import quota_manager
    quota_info = quota_manager.check_quota(meta_mgr, bucket, additional_size=size)
    
    # 3. Check if content already exists (DEDUPLICATION)
    db = meta_mgr.get_db()
    db_session = next(db)
    try:
        existing_content = db_session.execute(
            text("SELECT content_hash, shards FROM content_store WHERE content_hash = :hash"),
            {"hash": content_hash}
        ).fetchone()
        
        if existing_content:
            # Content exists! Just increment refcount and create metadata pointer
            db_session.execute(
                text("UPDATE content_store SET refcount = refcount + 1 WHERE content_hash = :hash"),
                {"hash": content_hash}
            )
            db_session.commit()
            
            # Create object metadata pointing to existing content
            obj = meta_mgr.put_object_metadata(
                bucket=bucket,
                key=key,
                size=size,
                shards=existing_content[1]  # Reuse existing shards
            )
            
            # Update object with content_hash
            db_session.execute(
                text("UPDATE objects SET content_hash = :hash WHERE id = :obj_id"),
                {"hash": content_hash, "obj_id": obj.id}
            )
            db_session.commit()
            
            # Broadcast Event
            await manager.broadcast({
                "type": "upload",
                "bucket": bucket,
                "key": key,
                "size": size,
                "deduplicated": True,
                "method": "api"
            })
            
            return {
                "status": "ok",
                "bucket": bucket,
                "key": key,
                "version_id": obj.version_id,
                "deduplicated": True,
                "content_hash": content_hash
            }
    finally:
        db_session.close()
    
    # 4. New content - Erasure Code
    shards_data = ec.encode_data(file_bytes)
    total_shards = len(shards_data)
    
    # 5. Select Nodes (with region preference)
    nodes = get_nodes_for_shards(total_shards, preferred_region=region)
    
    # 6. Distribute Shards (Parallel with Quorum)
    quorum_size = 4 if consistency == "strong" else total_shards
    
    async def upload_shard(i: int, node: NodeInfo, shard_val: bytes):
        shard_key = f"{key}.{b.name}.shard-{i}"
        url = f"{node.base_url}/internal/objects/{bucket}/{shard_key}"
        
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: requests.put(
                    url,
                    files={"file": (shard_key, shard_val, "application/octet-stream")},
                    timeout=10
                )
            )
            if resp.status_code == 200:
                return {"success": True, "index": i, "node_id": node.node_id, "shard_key": shard_key}
            else:
                return {"success": False, "index": i, "node_id": node.node_id, "error": f"Status {resp.status_code}"}
        except Exception as e:
            return {"success": False, "index": i, "node_id": node.node_id, "error": str(e)}
    
    # Upload all shards in parallel
    tasks = [upload_shard(i, nodes[i], shards_data[i]) for i in range(total_shards)]
    results = await asyncio.gather(*tasks)
    
    # Check quorum
    successful = [r for r in results if r["success"]]
    if len(successful) < quorum_size:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to meet quorum. Needed {quorum_size}/{total_shards}, got {len(successful)}/{total_shards}"
        )
    
    # 7. Store in content_store (new content)
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
    
    # 8. Store Metadata (pointing to content)
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
    
    # Broadcast Event
    await manager.broadcast({
        "type": "upload",
        "bucket": bucket,
        "key": key,
        "size": size,
        "deduplicated": False,
        "method": "api"
    })
    
    # 10. Trigger cross-region replication (async)
    if region:
        from replication import replication_coordinator
        await replication_coordinator.replicate_to_regions(bucket, key, shard_meta)

    return {
        "status": "ok",
        "bucket": bucket,
        "key": key,
        "version_id": obj.version_id,
        "shards_stored": len(shard_meta),
        "consistency": consistency,
        "quorum_met": True,
        "deduplicated": False,
        "content_hash": content_hash
    }

@app.get("/buckets/{bucket}/objects/{key:path}")
def download_object(bucket: str, key: str, version_id: Optional[str] = None):
    # 1. Get Metadata
    obj = meta_mgr.get_object_metadata(bucket, key, version_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found")

    shard_meta = json.loads(obj.shards)
    
    # 2. Fetch Shards
    # We need at least K shards. Let's try to fetch K.
    # If one fails, try another.
    
    retrieved_shards = []
    retrieved_indices = []
    
    # Sort by index just to be tidy, though not strictly necessary for decoding logic if we pass indices
    shard_meta.sort(key=lambda x: x["index"])
    
    for sm in shard_meta:
        if len(retrieved_shards) >= ec.K:
            break
            
        node = NODES.get(sm["node_id"])
        if not node:
            continue
            
        shard_key = sm["shard_key"]
        url = f"{node.base_url}/internal/objects/{bucket}/{shard_key}"
        
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                retrieved_shards.append(resp.content)
                retrieved_indices.append(sm["index"])
        except Exception:
            continue

    if len(retrieved_shards) < ec.K:
        raise HTTPException(status_code=502, detail=f"Could not retrieve enough shards. Need {ec.K}, got {len(retrieved_shards)}")

    # 3. Decode
    try:
        original_data = ec.decode_data(retrieved_shards, retrieved_indices, obj.size_bytes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erasure decode failed: {e}")

    # 4. Return
    return StreamingResponse(
        iter([original_data]),
        media_type="application/octet-stream"
    )

@app.delete("/buckets/{bucket}/objects/{key:path}")
async def delete_object(bucket: str, key: str):
    # This is tricky with versioning. 
    # For now, let's say it deletes the LATEST version's shards.
    # Or we could just mark it as deleted in metadata.
    # Let's do a "soft delete" in metadata? 
    # No, let's keep it simple: Delete metadata entry and shards for latest version.
    
    obj = meta_mgr.get_object_metadata(bucket, key)
    if not obj:
        return {"status": "not_found"}
        
    shard_meta = json.loads(obj.shards)
    
    for sm in shard_meta:
        node = NODES.get(sm["node_id"])
        if not node: continue
        
        shard_key = sm["shard_key"]
        url = f"{node.base_url}/internal/objects/{bucket}/{shard_key}"
        try:
            requests.delete(url, timeout=2)
        except:
            pass

    # 3. Delete from DB
    meta_mgr.delete_object_metadata(bucket, key)
    
    await manager.broadcast({
        "type": "delete",
        "bucket": bucket,
        "key": key,
        "method": "api"
    })
    
    return {"status": "deleted"}

# -------------------------------------------------------------------
# Multipart Upload Endpoints
# -------------------------------------------------------------------

from multipart import mp_manager

@app.post("/buckets/{bucket}/objects/{key:path}/uploads")
async def initiate_multipart(bucket: str, key: str):
    """Start a multipart upload session"""
    upload_id = mp_manager.initiate_upload(bucket, key)
    return {"upload_id": upload_id, "bucket": bucket, "key": key}

@app.put("/buckets/{bucket}/objects/{key:path}/uploads/{upload_id}/parts/{part_number}")
async def upload_part(bucket: str, key: str, upload_id: str, part_number: int, file: UploadFile = File(...)):
    """Upload a single part of a multipart upload"""
    data = await file.read()
    result = mp_manager.upload_part(upload_id, part_number, data)
    return result

@app.post("/buckets/{bucket}/objects/{key:path}/uploads/{upload_id}/complete")
async def complete_multipart(bucket: str, key: str, upload_id: str, consistency: str = "eventual"):
    """Complete multipart upload - concatenate parts and EC-encode"""
    # 1. Get all parts and concatenate
    full_data = mp_manager.complete_upload(upload_id)
    
    # 2. Now treat it like a regular upload (EC-encode and distribute)
    b = meta_mgr.get_bucket(bucket)
    if not b:
        b = meta_mgr.create_bucket(bucket)
    
    size = len(full_data)
    shards_data = ec.encode_data(full_data)
    total_shards = len(shards_data)
    nodes = get_nodes_for_shards(total_shards)
    quorum_size = 4 if consistency == "strong" else total_shards
    
    async def upload_shard(i: int, node: NodeInfo, shard_val: bytes):
        shard_key = f"{key}.{b.name}.shard-{i}"
        url = f"{node.base_url}/internal/objects/{bucket}/{shard_key}"
        try:
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(
                None,
                lambda: requests.put(url, files={"file": (shard_key, shard_val, "application/octet-stream")}, timeout=10)
            )
            if resp.status_code == 200:
                return {"success": True, "index": i, "node_id": node.node_id, "shard_key": shard_key}
            else:
                return {"success": False, "index": i, "node_id": node.node_id}
        except Exception as e:
            return {"success": False, "index": i, "node_id": node.node_id}
    
    tasks = [upload_shard(i, nodes[i], shards_data[i]) for i in range(total_shards)]
    results = await asyncio.gather(*tasks)
    
    successful = [r for r in results if r["success"]]
    if len(successful) < quorum_size:
        raise HTTPException(status_code=502, detail=f"Quorum not met: {len(successful)}/{total_shards}")
    
    shard_meta = [{"index": r["index"], "node_id": r["node_id"], "shard_key": r["shard_key"]} for r in successful]
    obj = meta_mgr.put_object_metadata(bucket, key, size, json.dumps(shard_meta))
    
    return {"status": "ok", "version_id": obj.version_id, "size": size}

@app.delete("/buckets/{bucket}/objects/{key:path}/uploads/{upload_id}")
async def abort_multipart(bucket: str, key: str, upload_id: str):
    """Abort and cleanup multipart upload"""
    mp_manager.abort_upload(upload_id)
    return {"status": "aborted"}

# -------------------------------------------------------------------
# Garbage Collection Endpoint
# -------------------------------------------------------------------

from gc_service import run_gc

@app.post("/admin/gc")
async def trigger_gc():
    """Manually trigger garbage collection"""
    result = run_gc()
    return result

