import asyncio
import time
from typing import List

class RateLimiter:
    def __init__(self, max_concurrent: int = 2, requests_per_minute: int = 60):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.request_times: List[float] = []
        self.max_per_minute = requests_per_minute
    
    async def acquire(self):
        await self.semaphore.acquire()
        
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        if len(self.request_times) >= self.max_per_minute:
            sleep_time = 60 - (now - self.request_times[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        self.request_times.append(now)
    
    def release(self):
        self.semaphore.release()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.release()