# storage_node/main.py
import os
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse

NODE_ID = os.getenv("NODE_ID", "node-1")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data")).resolve()

app = FastAPI(title=f"PlanetStore Storage Node {NODE_ID}")

DATA_DIR.mkdir(parents=True, exist_ok=True)


def object_path(bucket: str, key: str) -> Path:
    # avoid directory traversal
    safe_bucket = bucket.replace("..", "_")
    safe_key = key.replace("..", "_")
    return DATA_DIR / safe_bucket / safe_key


@app.get("/internal/health")
def health():
    return {"status": "ok", "node_id": NODE_ID}


@app.put("/internal/objects/{bucket}/{key:path}")
async def put_object(bucket: str, key: str, file: UploadFile = File(...)):
    path = object_path(bucket, key)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with path.open("wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Write error: {e}") from e

    return {"status": "stored", "node_id": NODE_ID, "bucket": bucket, "key": key}


@app.get("/internal/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str):
    path = object_path(bucket, key)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Object not found on this node.")

    return FileResponse(path)


@app.delete("/internal/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str):
    path = object_path(bucket, key)
    if not path.exists():
        return {"status": "not_found", "node_id": NODE_ID}

    try:
        path.unlink()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Delete error: {e}") from e

    return {"status": "deleted", "node_id": NODE_ID}
