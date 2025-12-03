"""
Health monitoring for storage nodes.
Pings nodes periodically and tracks health status.
"""
import logging
import time
import requests
from datetime import datetime
from typing import Dict, Optional
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from config import NODES_LIST, NodeInfo

logger = logging.getLogger(__name__)

class NodeHealthStatus:
    """Health status for a single node."""
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.status = "unknown"  # healthy, unhealthy, unknown
        self.last_check = None
        self.response_time_ms = None
        self.error_message = None

class HealthMonitor:
    """
    Monitors health of all storage nodes.
    Pings each node's /health endpoint periodically.
    """
    def __init__(self, check_interval_seconds: int = 30):
        self.check_interval = check_interval_seconds
        self.scheduler = BackgroundScheduler()
        self.node_health: Dict[str, NodeHealthStatus] = {}
        
        # Initialize health status for all nodes
        for node in NODES_LIST:
            self.node_health[node.node_id] = NodeHealthStatus(node.node_id)
    
    def start(self):
        """Start the health monitor."""
        self.scheduler.add_job(
            func=self._check_all_nodes,
            trigger=IntervalTrigger(seconds=self.check_interval),
            id='health_check_job',
            name='Node Health Check',
            replace_existing=True
        )
        self.scheduler.start()
        logger.info(f"Health Monitor started. Checking every {self.check_interval}s")
        
        # Run initial check immediately
        self._check_all_nodes()
    
    def shutdown(self):
        """Stop the health monitor."""
        self.scheduler.shutdown(wait=True)
        logger.info("Health Monitor stopped")
    
    def _check_all_nodes(self):
        """Check health of all nodes."""
        for node in NODES_LIST:
            self._check_node(node)
    
    def _check_node(self, node: NodeInfo):
        """Check health of a single node."""
        health_status = self.node_health[node.node_id]
        
        try:
            start_time = time.time()
            response = requests.get(f"{node.base_url}/internal/health", timeout=5)
            response_time = (time.time() - start_time) * 1000  # Convert to ms
            
            if response.status_code == 200:
                health_status.status = "healthy"
                health_status.response_time_ms = round(response_time, 2)
                health_status.error_message = None
                logger.debug(f"Node {node.node_id} is healthy ({response_time:.2f}ms)")
            else:
                health_status.status = "unhealthy"
                health_status.error_message = f"HTTP {response.status_code}"
                logger.warning(f"Node {node.node_id} returned {response.status_code}")
        
        except requests.exceptions.Timeout:
            health_status.status = "unhealthy"
            health_status.error_message = "Timeout"
            health_status.response_time_ms = None
            logger.warning(f"Node {node.node_id} timed out")
        
        except requests.exceptions.ConnectionError:
            health_status.status = "unhealthy"
            health_status.error_message = "Connection refused"
            health_status.response_time_ms = None
            logger.warning(f"Node {node.node_id} connection refused")
        
        except Exception as e:
            health_status.status = "unhealthy"
            health_status.error_message = str(e)
            health_status.response_time_ms = None
            logger.error(f"Node {node.node_id} check failed: {e}")
        
        finally:
            health_status.last_check = datetime.utcnow()
    
    def get_health_status(self, node_id: Optional[str] = None) -> Dict:
        """Get health status for a node or all nodes."""
        if node_id:
            if node_id not in self.node_health:
                return {"error": f"Node {node_id} not found"}
            
            status = self.node_health[node_id]
            return {
                "node_id": status.node_id,
                "status": status.status,
                "last_check": status.last_check.isoformat() if status.last_check else None,
                "response_time_ms": status.response_time_ms,
                "error": status.error_message
            }
        
        # Return all nodes
        return {
            "nodes": [
                {
                    "node_id": status.node_id,
                    "status": status.status,
                    "last_check": status.last_check.isoformat() if status.last_check else None,
                    "response_time_ms": status.response_time_ms,
                    "error": status.error_message
                }
                for status in self.node_health.values()
            ],
            "check_interval_seconds": self.check_interval
        }

# Global instance
health_monitor = None
