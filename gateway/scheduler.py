"""
Background scheduler for automated garbage collection.
"""
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)

class GCScheduler:
    def __init__(self, gc_function, interval_hours: int = 1):
        """
        Initialize the GC scheduler.
        
        Args:
            gc_function: The garbage collection function to run
            interval_hours: How often to run GC (default: 1 hour)
        """
        self.gc_function = gc_function
        self.interval_hours = interval_hours
        self.scheduler = BackgroundScheduler()
        self.last_run = None
        self.next_run = None
        
    def start(self):
        """Start the scheduler."""
        self.scheduler.add_job(
            func=self._run_gc,
            trigger=IntervalTrigger(hours=self.interval_hours),
            id='gc_job',
            name='Garbage Collection',
            replace_existing=True
        )
        self.scheduler.start()
        logger.info(f"GC Scheduler started. Running every {self.interval_hours} hour(s)")
        
    def shutdown(self):
        """Stop the scheduler gracefully."""
        self.scheduler.shutdown(wait=True)
        logger.info("GC Scheduler stopped")
        
    def _run_gc(self):
        """Internal wrapper to run GC and track timing."""
        try:
            logger.info("Starting scheduled garbage collection...")
            self.last_run = datetime.utcnow()
            result = self.gc_function()
            logger.info(f"Scheduled GC completed: {result}")
            
            # Calculate next run
            jobs = self.scheduler.get_jobs()
            if jobs:
                self.next_run = jobs[0].next_run_time
        except Exception as e:
            logger.error(f"Scheduled GC failed: {e}", exc_info=True)
            
    def get_status(self):
        """Get scheduler status."""
        jobs = self.scheduler.get_jobs()
        return {
            "running": self.scheduler.running,
            "interval_hours": self.interval_hours,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": jobs[0].next_run_time.isoformat() if jobs and jobs[0].next_run_time else None
        }

# Global instance
gc_scheduler = None
