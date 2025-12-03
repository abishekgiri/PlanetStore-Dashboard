import os
from typing import List, Dict, Optional
from pydantic import BaseModel
from fastapi import HTTPException

class NodeInfo(BaseModel):
    node_id: str
    base_url: str

def parse_nodes(env_value: str) -> List[NodeInfo]:
    nodes: List[NodeInfo] = []
    for part in env_value.split(","):
        part = part.strip()
        if not part:
            continue
        # Format: node_id:url
        if ":" in part:
            # Handle http:// case which adds extra colons
            # Split by first colon only? No, URL has colons.
            # Let's assume first colon separates ID from URL
            idx = part.find(":")
            nid = part[:idx]
            url = part[idx+1:]
            nodes.append(NodeInfo(node_id=nid, base_url=url))
    return nodes

# STORAGE_NODES="node1:http://storage-node-1:9001,..."
STORAGE_NODES_ENV = os.getenv(
    "STORAGE_NODES",
    "node1:http://localhost:9001,node2:http://localhost:9002,node3:http://localhost:9003,node4:http://localhost:9004,node5:http://localhost:9005,node6:http://localhost:9006",
)

NODES_LIST: List[NodeInfo] = parse_nodes(STORAGE_NODES_ENV)
NODES: Dict[str, NodeInfo] = {n.node_id: n for n in NODES_LIST}

# -------------------------------------------------------------------
# Multi-Region Configuration
# -------------------------------------------------------------------

REGIONS = {
    "us-east": ["node1", "node2"],
    "eu-west": ["node3", "node4"],
    "ap-south": ["node5", "node6"]
}

# Reverse mapping: node_id -> region
NODE_TO_REGION = {}
for region, nodes in REGIONS.items():
    for node_id in nodes:
        NODE_TO_REGION[node_id] = region

def get_nodes_for_shards(count: int, preferred_region: Optional[str] = None) -> List[NodeInfo]:
    """
    Get nodes for shards. If preferred_region is specified, prioritize nodes from that region.
    """
    if count > len(NODES_LIST):
        raise HTTPException(status_code=500, detail=f"Not enough storage nodes. Need {count}, have {len(NODES_LIST)}")
    
    if not preferred_region or preferred_region not in REGIONS:
        # No region preference, use all nodes
        return NODES_LIST[:count]
    
    # Get nodes from preferred region
    preferred_node_ids = REGIONS[preferred_region]
    preferred_nodes = [NODES[nid] for nid in preferred_node_ids if nid in NODES]
    
    # If we have enough in preferred region, use them
    if len(preferred_nodes) >= count:
        return preferred_nodes[:count]
    
    # Otherwise, fill with nodes from other regions
    other_nodes = [n for n in NODES_LIST if n.node_id not in preferred_node_ids]
    return preferred_nodes + other_nodes[:count - len(preferred_nodes)]
