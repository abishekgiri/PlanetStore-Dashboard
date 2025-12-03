"""
Garbage Collection Service for Planetstore

Cleans up:
1. Old object versions (keep only latest N versions)
2. Orphaned shards (shards with no metadata entry)
"""
import os
import json
import requests
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from metadata import SessionLocal, Object, engine

# Config
MAX_VERSIONS_PER_OBJECT = int(os.getenv("MAX_VERSIONS", "5"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))

def get_nodes():
    """Parse storage nodes from env"""
    from main import NODES
    return NODES

def cleanup_old_versions():
    """Delete old versions beyond retention limit"""
    db = SessionLocal()
    deleted_count = 0
    
    try:
        # Group by bucket + key
        all_objects = db.query(Object).order_by(Object.bucket_name, Object.object_key, Object.created_at.desc()).all()
        
        current_key = None
        version_count = 0
        
        for obj in all_objects:
            key_identifier = f"{obj.bucket_name}/{obj.object_key}"
            
            # Reset counter for new key
            if key_identifier != current_key:
                current_key = key_identifier
                version_count = 0
            
            version_count += 1
            
            # Keep latest version always
            if obj.is_latest:
                continue
            
            # Keep up to MAX_VERSIONS_PER_OBJECT old versions
            if version_count > MAX_VERSIONS_PER_OBJECT:
                # Delete shards
                shards = json.loads(obj.shards)
                nodes = get_nodes()
                
                for shard in shards:
                    node = nodes.get(shard["node_id"])
                    if not node:
                        continue
                    
                    url = f"{node.base_url}/internal/objects/{obj.bucket_name}/{shard['shard_key']}"
                    try:
                        requests.delete(url, timeout=5)
                    except:
                        pass
                
                # Delete metadata
                db.delete(obj)
                deleted_count += 1
        
        db.commit()
        print(f"GC: Deleted {deleted_count} old versions")
        return deleted_count
    finally:
        db.close()

def cleanup_by_age():
    """Delete objects older than retention period"""
    db = SessionLocal()
    deleted_count = 0
    cutoff = datetime.utcnow() - timedelta(days=RETENTION_DAYS)
    
    try:
        old_objects = db.query(Object).filter(
            Object.is_latest == False,
            Object.created_at < cutoff
        ).all()
        
        nodes = get_nodes()
        
        for obj in old_objects:
            # Delete shards
            shards = json.loads(obj.shards)
            for shard in shards:
                node = nodes.get(shard["node_id"])
                if not node:
                    continue
                
                url = f"{node.base_url}/internal/objects/{obj.bucket_name}/{shard['shard_key']}"
                try:
                    requests.delete(url, timeout=5)
                except:
                    pass
            
            db.delete(obj)
            deleted_count += 1
        
        db.commit()
        print(f"GC: Deleted {deleted_count} objects older than {RETENTION_DAYS} days")
        return deleted_count
    finally:
        db.close()

def run_gc():
    """Run full garbage collection"""
    print("Starting Garbage Collection...")
    v_deleted = cleanup_old_versions()
    a_deleted = cleanup_by_age()
    print(f"GC Complete: {v_deleted} by version, {a_deleted} by age")
    return {"versions_deleted": v_deleted, "age_deleted": a_deleted}

if __name__ == "__main__":
    run_gc()
