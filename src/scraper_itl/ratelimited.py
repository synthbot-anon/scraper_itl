import io
from random import random
import time
import traceback
import asyncio

import httpx


class RateLimitedSession:
    def __init__(self, perSecond) -> None:
        self.perSecond = perSecond
        self.lastGetTime = 0
        self.client = httpx.AsyncClient()
        self.update_queue = asyncio.Queue()

    def update(self, perSecond):
        self.perSecond = perSecond
        self.update_queue.put_nowait(None)

    async def wait(self):
        while True:
            currentTime = time.time()
            nextGetTime = self.lastGetTime + 1.0 / self.perSecond
            try:
                await asyncio.wait_for(
                    self.update_queue.get(), nextGetTime - currentTime
                )
                continue
            except asyncio.TimeoutError:
                self.lastGetTime = time.time()
                break

    async def get(self, url, fragile=False, debug_print=print, *args, **kwargs):
        backoff = 1
        while True:
            try:
                await self.wait()
                response = await self.client.get(url, *args, **kwargs)
                if response.status_code == 200:
                    return response
                elif response.status_code == 404:
                    return
                else:
                    if fragile:
                        return None
                    debug_print(
                        f"Rejected {response.status_code} trying to GET {url}"
                    )
            except KeyboardInterrupt:
                raise
            except Exception as err:
                debug_print(f"Failed to GET {url}: {err}")
                if fragile:
                    return None

            backoff = backoff * 2
            debug_print(f"Retrying {url} in {backoff} seconds")
            await asyncio.sleep(random() * backoff)

    async def downloadImage(self, url, timeout=None, debug_print=print):
        while True:
            try:
                response = io.BytesIO()
                async with self.client.stream("GET", url, timeout=timeout) as stream:
                    async for chunk in stream.aiter_bytes():
                        response.write(chunk)
                return response.getvalue()
            except KeyboardInterrupt:
                raise
            except:
                traceback.print_exc()
                debug_print(f"Failed to download {url}")
                return None
