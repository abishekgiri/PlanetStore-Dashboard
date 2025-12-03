"""
Multipart Upload Support

Allows uploading large files in chunks that can be resumed if interrupted.
"""
import uuid
import json
import os
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, func
from metadata import Base, SessionLocal, engine
from typing import List, Dict

# New table for tracking multipart uploads
class MultipartUpload(Base):
    __tablename__ = "multipart_uploads"
    id = Column(Integer, primary_key=True)
    upload_id = Column(String(36), unique=True, nullable=False)
    bucket_name = Column(String(255), nullable=False)
    object_key = Column(String(2048), nullable=False)
    parts = Column(Text, default="[]")  # JSON list of uploaded parts
    created_at = Column(DateTime(timezone=True), server_default=func.now())

# Create table
try:
    Base.metadata.create_all(bind=engine)
except:
    pass

class MultipartManager:
    def initiate_upload(self, bucket: str, key: str) -> str:
        """Start a new multipart upload session"""
        db = SessionLocal()
        try:
            upload_id = str(uuid.uuid4())
            upload = MultipartUpload(
                upload_id=upload_id,
                bucket_name=bucket,
                object_key=key
            )
            db.add(upload)
            db.commit()
            return upload_id
        finally:
            db.close()
    
    def upload_part(self, upload_id: str, part_number: int, data: bytes) -> Dict:
        """Upload a single part"""
        db = SessionLocal()
        try:
            upload = db.query(MultipartUpload).filter_by(upload_id=upload_id).first()
            if not upload:
                raise ValueError("Upload not found")
            
            # Store part temporarily (in real system, use object storage)
            part_dir = f"/tmp/multipart/{upload_id}"
            os.makedirs(part_dir, exist_ok=True)
            part_path = f"{part_dir}/part-{part_number}"
            
            with open(part_path, "wb") as f:
                f.write(data)
            
            # Update parts list
            parts = json.loads(upload.parts)
            parts.append({
                "part_number": part_number,
                "size": len(data),
                "path": part_path
            })
            upload.parts = json.dumps(parts)
            db.commit()
            
            return {"part_number": part_number, "size": len(data)}
        finally:
            db.close()
    
    def complete_upload(self, upload_id: str) -> bytes:
        """Finalize upload by concatenating parts"""
        db = SessionLocal()
        try:
            upload = db.query(MultipartUpload).filter_by(upload_id=upload_id).first()
            if not upload:
                raise ValueError("Upload not found")
            
            parts = json.loads(upload.parts)
            parts.sort(key=lambda x: x["part_number"])
            
            # Concatenate all parts
            full_data = b""
            for part in parts:
                with open(part["path"], "rb") as f:
                    full_data += f.read()
            
            # Cleanup temp files
            for part in parts:
                try:
                    os.remove(part["path"])
                except:
                    pass
            
            # Delete upload session
            db.delete(upload)
            db.commit()
            
            return full_data
        finally:
            db.close()
    
    def abort_upload(self, upload_id: str):
        """Cancel and cleanup multipart upload"""
        db = SessionLocal()
        try:
            upload = db.query(MultipartUpload).filter_by(upload_id=upload_id).first()
            if upload:
                parts = json.loads(upload.parts)
                for part in parts:
                    try:
                        os.remove(part["path"])
                    except:
                        pass
                db.delete(upload)
                db.commit()
        finally:
            db.close()

mp_manager = MultipartManager()
