import os
import uuid
from typing import List, Optional
from sqlalchemy import create_engine, Column, Integer, String, Boolean, BigInteger, DateTime, Text, func, Index
from sqlalchemy.orm import sessionmaker, declarative_base
from pydantic import BaseModel

# Config
DB_URL = os.getenv("DB_URL", "postgresql://user:password@localhost:5432/planetstore")

engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# -------------------------------------------------------------------
# Models
# -------------------------------------------------------------------

class Bucket(Base):
    __tablename__ = "buckets"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    versioning_enabled = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class Object(Base):
    __tablename__ = "objects"
    id = Column(Integer, primary_key=True, index=True)
    bucket_name = Column(String(255), nullable=False)
    object_key = Column(String(2048), nullable=False)
    version_id = Column(String(36), nullable=False)  # UUID
    is_latest = Column(Boolean, default=True)
    size_bytes = Column(BigInteger)
    # shards: JSON list of {"index": int, "node_id": str}
    shards = Column(Text, nullable=False) 
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index('idx_bucket_key_version', 'bucket_name', 'object_key', 'version_id'),
    )

# -------------------------------------------------------------------
# Manager
# -------------------------------------------------------------------

class MetadataManager:
    def __init__(self):
        # Create tables if they don't exist
        # In prod, use Alembic migrations. Here, auto-create is fine for dev.
        try:
            Base.metadata.create_all(bind=engine)
        except Exception as e:
            print(f"Warning: Could not create tables (db might not be ready): {e}")

    def get_db(self):
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def create_bucket(self, name: str, versioning: bool = False) -> Bucket:
        db = SessionLocal()
        try:
            bucket = Bucket(name=name, versioning_enabled=versioning)
            db.add(bucket)
            db.commit()
            db.refresh(bucket)
            return bucket
        finally:
            db.close()

    def get_bucket(self, name: str) -> Optional[Bucket]:
        db = SessionLocal()
        try:
            return db.query(Bucket).filter_by(name=name).first()
        finally:
            db.close()

    def list_buckets(self) -> List[Bucket]:
        db = SessionLocal()
        try:
            return db.query(Bucket).all()
        finally:
            db.close()

    def put_object_metadata(self, bucket: str, key: str, size: int, shards: str) -> Object:
        db = SessionLocal()
        try:
            # If versioning is NOT enabled (or for simplicity in this phase), 
            # we might want to mark old objects as not latest.
            # For now, let's assume we always create a new version.
            
            # 1. Mark existing latest as not latest
            existing = db.query(Object).filter_by(
                bucket_name=bucket, object_key=key, is_latest=True
            ).first()
            if existing:
                existing.is_latest = False
            
            # 2. Create new object version
            ver_id = str(uuid.uuid4())
            obj = Object(
                bucket_name=bucket,
                object_key=key,
                version_id=ver_id,
                is_latest=True,
                size_bytes=size,
                shards=shards
            )
            db.add(obj)
            db.commit()
            db.refresh(obj)
            return obj
        finally:
            db.close()

    def get_object_metadata(self, bucket: str, key: str, version_id: str = None) -> Optional[Object]:
        db = SessionLocal()
        try:
            query = db.query(Object).filter_by(bucket_name=bucket, object_key=key)
            if version_id:
                query = query.filter_by(version_id=version_id)
            else:
                query = query.filter_by(is_latest=True)
            return query.first()
        finally:
            db.close()

    def list_objects(self, bucket: str) -> List[Object]:
        db = SessionLocal()
        try:
            # Only list latest versions for now
            return db.query(Object).filter_by(bucket_name=bucket, is_latest=True).all()
        finally:
            db.close()

    def delete_object_metadata(self, bucket: str, key: str) -> bool:
        db = SessionLocal()
        try:
            # Delete the latest version of the object
            obj = db.query(Object).filter_by(bucket_name=bucket, object_key=key, is_latest=True).first()
            if obj:
                db.delete(obj)
                db.commit()
                return True
            return False
        finally:
            db.close()
