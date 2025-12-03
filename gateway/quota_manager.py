"""
Quota management for buckets.
Enforces storage limits per bucket.
"""
from typing import Optional, Dict
from sqlalchemy import text
from fastapi import HTTPException

class QuotaManager:
    """
    Manages storage quotas for buckets.
    """
    def __init__(self, default_max_size_gb: float = 10.0, default_max_objects: int = 10000):
        self.default_max_size_bytes = int(default_max_size_gb * 1024 * 1024 * 1024)
        self.default_max_objects = default_max_objects
        # In a real system, these would be stored in database per bucket
        self.custom_quotas: Dict[str, Dict[str, int]] = {}
        
    
    def get_quota(self, meta_mgr, bucket: str) -> Dict[str, int]:
        """Get quota limits for bucket from database."""
        db = meta_mgr.get_db()
        db_session = next(db)
        try:
            from sqlalchemy import text
            result = db_session.execute(
                text("SELECT max_size_bytes, max_objects FROM bucket_quotas WHERE bucket_name = :bucket"),
                {"bucket": bucket}
            ).fetchone()
            
            if result:
                return {
                    "max_size_bytes": result[0],
                    "max_objects": result[1]
                }
            
            # Return defaults
            return {
                "max_size_bytes": self.default_max_size_bytes,
                "max_objects": self.default_max_objects
            }
        finally:
            db_session.close()
    
    def set_quota(self, meta_mgr, bucket: str, max_size_bytes: int, max_objects: int):
        """Set custom quota for a bucket in database."""
        db = meta_mgr.get_db()
        db_session = next(db)
        try:
            from sqlalchemy import text
            # Upsert quota
            db_session.execute(text("""
                INSERT INTO bucket_quotas (bucket_name, max_size_bytes, max_objects)
                VALUES (:bucket, :size, :objects)
                ON CONFLICT (bucket_name) 
                DO UPDATE SET max_size_bytes = :size, max_objects = :objects
            """), {"bucket": bucket, "size": max_size_bytes, "objects": max_objects})
            db_session.commit()
        finally:
            db_session.close()
    
    def check_quota(self, meta_mgr, bucket: str, additional_size: int = 0) -> Dict:
        """
        Check if bucket is within quota.
        Raises HTTPException if quota would be exceeded.
        Returns current usage info.
        """
        db = meta_mgr.get_db()
        db_session = next(db)
        
        try:
            # Get current usage
            current_stats = db_session.execute(text("""
                SELECT COUNT(*) as object_count, COALESCE(SUM(size_bytes), 0) as total_size
                FROM objects
                WHERE bucket_name = :bucket AND is_latest = true
            """), {"bucket": bucket}).fetchone()
            
            current_objects = current_stats[0]
            current_size = current_stats[1]
            
            # Get quota limits from database
            quota = self.get_quota(meta_mgr, bucket)
            
            # Check if adding this object would exceed quota
            new_size = current_size + additional_size
            new_objects = current_objects + (1 if additional_size > 0 else 0)
            
            if new_size > quota["max_size_bytes"]:
                raise HTTPException(
                    status_code=507,  # Insufficient Storage
                    detail=f"Bucket quota exceeded: {new_size}/{quota['max_size_bytes']} bytes",
                    headers={"X-Quota-Used": str(new_size), "X-Quota-Limit": str(quota["max_size_bytes"])}
                )
            
            if new_objects > quota["max_objects"]:
                raise HTTPException(
                    status_code=507,
                    detail=f"Bucket quota exceeded: {new_objects}/{quota['max_objects']} objects",
                    headers={"X-Quota-Used": str(new_objects), "X-Quota-Limit": str(quota["max_objects"])}
                )
            
            return {
                "current_objects": current_objects,
                "current_size_bytes": current_size,
                "max_objects": quota["max_objects"],
                "max_size_bytes": quota["max_size_bytes"],
                "objects_remaining": quota["max_objects"] - new_objects,
                "bytes_remaining": quota["max_size_bytes"] - new_size
            }
        finally:
            db_session.close()


# Global instance
quota_manager = QuotaManager(default_max_size_gb=10.0, default_max_objects=10000)
