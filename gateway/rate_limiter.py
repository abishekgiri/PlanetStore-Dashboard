"""
Rate limiting middleware for API endpoints.
Implements sliding window rate limiting per IP address.
"""
import time
from collections import defaultdict
from threading import Lock
from typing import Dict, Tuple
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

class RateLimiter:
    """
    In-memory rate limiter using sliding window.
    Tracks requests per IP address.
    """
    def __init__(self, requests_per_minute: int = 100):
        self.requests_per_minute = requests_per_minute
        self.window_size = 60  # seconds
        # ip -> list of (timestamp,)
        self.requests: Dict[str, list] = defaultdict(list)
        self.lock = Lock()
        
    def is_allowed(self, ip: str) -> Tuple[bool, int]:
        """
        Check if request from IP is allowed.
        Returns (allowed, remaining_requests)
        """
        now = time.time()
        cutoff = now - self.window_size
        
        with self.lock:
            # Clean old requests
            self.requests[ip] = [ts for ts in self.requests[ip] if ts > cutoff]
            
            # Check limit
            current_count = len(self.requests[ip])
            if current_count >= self.requests_per_minute:
                return False, 0
            
            # Add this request
            self.requests[ip].append(now)
            return True, self.requests_per_minute - current_count - 1
    
    def cleanup_old_entries(self):
        """Remove old IP entries to prevent memory leak."""
        now = time.time()
        cutoff = now - self.window_size * 2  # Keep 2x window for safety
        
        with self.lock:
            ips_to_remove = []
            for ip, timestamps in self.requests.items():
                # Remove old timestamps
                self.requests[ip] = [ts for ts in timestamps if ts > cutoff]
                # If no recent requests, mark for removal
                if not self.requests[ip]:
                    ips_to_remove.append(ip)
            
            for ip in ips_to_remove:
                del self.requests[ip]


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Middleware to enforce rate limiting."""
    
    def __init__(self, app, rate_limiter: RateLimiter):
        super().__init__(app)
        self.rate_limiter = rate_limiter
        
    async def dispatch(self, request: Request, call_next):
        # Skip rate limiting for certain paths (admin, health checks)
        if request.url.path.startswith("/admin") or request.url.path == "/health":
            return await call_next(request)
        
        # Get client IP
        client_ip = request.client.host if request.client else "unknown"
        
        # Check rate limit
        allowed, remaining = self.rate_limiter.is_allowed(client_ip)
        
        if not allowed:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Try again later."},
                headers={"Retry-After": "60"}
            )
        
        # Process request
        response = await call_next(request)
        
        # Add rate limit headers
        response.headers["X-RateLimit-Limit"] = str(self.rate_limiter.requests_per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        
        return response


# Global instance
rate_limiter = RateLimiter(requests_per_minute=100)
