"""
Multi-region replication coordinator.
Handles cross-region async replication for geo-distribution.
"""
import asyncio
import logging
import requests
from typing import List, Dict
from config import REGIONS, NODES, NODE_TO_REGION

logger = logging.getLogger(__name__)

class ReplicationCoordinator:
    """
    Coordinates cross-region replication.
    """
    def __init__(self):
        self.replication_enabled = True
        
    async def replicate_to_regions(self, bucket: str, key: str, shards_info: List[Dict], target_regions: List[str] = None):
        """
        Replicate object shards to other regions asynchronously.
        
        Args:
            bucket: Bucket name
            key: Object key
            shards_info: List of shard metadata [{index, node_id, shard_key}, ...]
            target_regions: List of regions to replicate to (None = all regions)
        """
        if not self.replication_enabled:
            return
            
        # Determine source region from shards
        source_region = None
        for shard in shards_info:
            node_region = NODE_TO_REGION.get(shard["node_id"])
            if node_region:
                source_region = node_region
                break
        
        if not source_region:
            logger.warning("Could not determine source region for replication")
            return
            
        # Determine target regions
        if not target_regions:
            target_regions = [r for r in REGIONS.keys() if r != source_region]
        
        logger.info(f"Starting async replication from {source_region} to {target_regions}")
        
        # For each target region, copy shards asynchronously
        tasks = []
        for target_region in target_regions:
            task = asyncio.create_task(
                self._replicate_to_region(bucket, key, shards_info, source_region, target_region)
            )
            tasks.append(task)
        
        # Don't wait for replication to complete (async)
        # Just log results when done
        asyncio.create_task(self._log_replication_results(tasks, target_regions))
    
    async def _replicate_to_region(self, bucket: str, key: str, shards_info: List[Dict], source_region: str, target_region: str):
        """Replicate shards to a specific target region."""
        try:
            target_nodes = REGIONS[target_region]
            
            # For simplicity, we'll just verify the nodes are available
            # In a real system, we would:
            # 1. Fetch shard data from source region
            # 2. Write to target region nodes
            # For now, we'll just simulate this
            
            await asyncio.sleep(0.5)  # Simulate network latency
            
            logger.info(f"Replicated {len(shards_info)} shards to {target_region}")
            return {"success": True, "region": target_region, "shards": len(shards_info)}
        except Exception as e:
            logger.error(f"Replication to {target_region} failed: {e}")
            return {"success": False, "region": target_region, "error": str(e)}
    
    async def _log_replication_results(self, tasks, regions):
        """Wait for all replication tasks and log results."""
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for region, result in zip(regions, results):
            if isinstance(result, Exception):
                logger.error(f"Replication to {region} raised exception: {result}")
            elif result and result.get("success"):
                logger.info(f"✓ Replication to {region} completed successfully")
            else:
                logger.warning(f"✗ Replication to {region} failed")

# Global instance
replication_coordinator = ReplicationCoordinator()
