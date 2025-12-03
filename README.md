Planetstore Distributed Object Storage System
Planetstore is a high-performance, distributed object storage engine inspired by AWS S3, built with modern cloud-native principles.It supports erasure coding, deduplication, versioning, multi-region replication, quotas, auth, rate-limiting, monitoring, and a full React admin dashboard.
This is not a toy project, it is a mini-S3 with real distributed-systems engineering.
Table of Contents
 Key Features


 How It Works


 Project Structure


Data Storage Structure


 Getting Started


 Usage Guide


 Tech Stack


 Key Features
Durability & Availability
Erasure Coding (4+2 Reed-Solomon)
 Every file is split into 4 data shards + 2 parity shards.
 The system can survive two node failures while still reconstructing the file.


Distributed Architecture
 Shards are stored across 6 independent storage nodes.


Multi-Region Replication
 Simulated geo-replication for disaster recovery and cross-region redundancy.


 Performance & Efficiency
Content Deduplication
 Identical files are stored only once using SHA-256 hashing.


Parallel IO
 Uploads and downloads run in parallel across nodes.


Hot Cache (LRU)
 Frequently accessed objects are temporarily cached in memory for speed.


Advanced Storage Management
S3-Compatible API
 Supports standard endpoints like PUT, GET, DELETE, LIST, HEAD.


Bucket Quotas
 Limit bucket storage size and object count.


Automated Garbage Collection
 Background jobs delete old versions, expired objects, and unused shards.


Real-Time Monitoring
 WebSocket-powered dashboard displays:


Live logs


Node health checks


Active users


Replication events


Rate Limiting
 Token bucket algorithm to prevent API abuse.


Security
User Authentication (JWT)


Secure Password Hashing (bcrypt)


Role-Based Access Control (admin vs user)


Signed Upload URLs (optional extension)



How It Works
Upload Flow (Write Path)
Client uploads file → Gateway receives the object.


Deduplication
 File hashed using SHA-256.


If hash exists → metadata updated (no upload).


If new → continue.


Erasure Coding
 File encoded into 6 shards (4 data + 2 parity) using Reed-Solomon.


Distribution
 Shards uploaded in parallel to 6 different storage nodes.


Metadata Stored
 Gateway writes:


Bucket name


Object key


File version ID


Shard list & node mapping


Size, timestamp
 to PostgreSQL.


Download Flow (Read Path)
Client requests object.


Gateway fetches its metadata (shard locations).


Shards retrieved in parallel from storage nodes.


As soon as any 4 shards arrive → object is reconstructed.


File is streamed back to the client.



📁 Project Structure
planetstore/
├── gateway/                        # Gateway (control plane)
│   ├── main.py                     # FastAPI entry point + S3-like API
│   ├── ec.py                       # Reed-Solomon erasure coding engine
│   ├── metadata.py                 # PostgreSQL ORM models + manager
│   ├── auth.py                     # JWT, password hashing, RBAC
│   ├── quota_manager.py            # Per-bucket quotas
│   ├── s3_api.py                   # S3-compatible interface (XML)
│   ├── replication.py              # Multi-region replication engine
│   ├── health_monitor.py           # Periodic node health checks
│   ├── scheduler.py                # GC + monitoring cron scheduler
│   ├── rate_limiter.py             # Token bucket rate limiter
│   ├── gc_service.py               # Garbage collection (versions/shards)
│   ├── config.py                   # Node registry & region mappings
│   ├── schema.sql                  # DB schema (buckets, objects, versions)
│   ├── schema_auth.sql             # Users + tokens
│   ├── schema_dedup.sql            # Deduplication table
│   ├── schema_quotas.sql           # Quota limits
│   └── ...
│
├── storage_node/                   # Storage nodes (data plane)
│   ├── main.py                     # Reads/writes shards on local disk
│   ├── Dockerfile                  # Container configuration
│   └── requirements.txt            # Per-node requirements
│
├── frontend/                       # React + Vite admin dashboard
│   ├── src/
│   │   ├── pages/                  # Login, Dashboard, Buckets, Settings
│   │   ├── components/             # Reusable charts, health cards, tables
│   │   └── api.ts                  # API wrapper
│   └── ...
│
├── docker-compose.yml              # Spins up Gateway + PG + 6 nodes + UI
└── README.md                       # Documentation


Data Storage Structure
Inside each storage node container:
/data/
 └── buckets/
     └── {bucket_name}/
         └── {shard_uuid}     # Erasure-coded shard file

Each node stores only the shards assigned to it, never the full file.

Getting Started
Prerequisites
Docker & Docker Compose


Node.js 18+ (for the frontend)


Python 3.11+ (if developing locally)


Start Backend + Storage Nodes
docker-compose up --build

Services started:
Service
Description
gateway
API + EC + dedup + metadata
db
PostgreSQL metadata store
node1–node6
Storage nodes holding shards


Start the Frontend Dashboard
cd frontend
npm install
npm run dev

2. Manage Storage
Create a Bucket
Through UI or via API:
Upload a File
Download a File
Delete an Object
3. Admin Dashboard Features
You can view:
Live Activity Stream
See uploads, deletes, GC events, health checks.
Node Health
Online/offline


Latency


Error logs


Storage usage


Bucket Insights
Object count


Storage used


Growth over time


 Tech Stack
Backend
Python 3.11


FastAPI + Uvicorn


SQLAlchemy ORM


APScheduler


Reed-Solomon (zfec)


JWT Auth (python-jose)


bcrypt


Storage Nodes
FastAPI


Local disk persistence


Dockerized microservices


Frontend
React 18


Vite


TypeScript


Tailwind CSS


Recharts


Lucide Icons


Infrastructure
Docker


Docker Compose


PostgreSQL 15


Final Notes
Planetstore is designed as a learning-grade, research-grade, and production-inspired distributed system, implementing real-world storage concepts:
Erasure coding


Deduplication


Multi-region replication


S3 API compatibility


GC + lifecycle policies


Health checks


Multi-node parallel I/O


This project demonstrates full-stack distributed-systems engineering, cloud concepts, backend architecture, and frontend dashboarding.
# PlanetStore-Dashboard
