import asyncio
import time
import traceback
from typing import Union

from itllib import Itl
from itllib.controllers import SyncedResources, ClusterController
from pydantic import BaseModel, ValidationError

from .ratelimited import RateLimitedSession


SESSIONS = {}


class ScraperConfig(BaseModel):
    domain: str
    scrapeInterval: float
    status: Union[bool, int, float, str, list, dict] = None


class Scraper:
    config: ScraperConfig = None
    session: RateLimitedSession = None

    def __init__(self, config, session):
        self.config = config
        self.session = session

    def scrape(self, itl: Itl):
        raise NotImplementedError()


def get_session(domain):
    if domain in SESSIONS:
        return SESSIONS[domain]
    SESSIONS[domain] = RateLimitedSession(1)
    return SESSIONS[domain]


async def scraper_task(
    itl: Itl, task_queue: asyncio.Queue, scraper: Scraper, patch_fn, patch_args
):
    lastScrape = 0

    itl.stream_send("debug", f"Starting scraper for {scraper.config.domain}...")

    while True:
        currentTime = time.time()
        timeToNextScrape = lastScrape + scraper.config.scrapeInterval - currentTime

        # Wait for either a new task or the next scrape time
        # If there's a task, handle it and try again. Only run the scrape when there's
        # pending task.

        try:
            task = await asyncio.wait_for(task_queue.get(), timeout=timeToNextScrape)
        except asyncio.TimeoutError:
            # There's no task, and it's time to scrape.
            itl.stream_send("debug", f"Scraping {scraper.config.domain}...")
            previousStatus = scraper.config.status
            status = await scraper.scrape(itl)
            if status != previousStatus:
                args = patch_args({"spec": {"status": status}})
                await patch_fn(*args)
            else:
                itl.stream_send("debug", f"No new data for {scraper.config.domain}...")

            lastScrape = time.time()
            continue

        command, args = task
        if command == "update-config":
            assert isinstance(
                args, ScraperConfig
            ), f"Expected ScraperConfig, got {type(args)}"
            scraper.config = args
            itl.stream_send("debug", f"Updated config for {scraper.config.domain}...")
            continue
        elif command == "stop":
            itl.stream_send_sync(
                "debug", f"Stopping scraper for {scraper.config.domain}..."
            )
            break
        else:
            itl.stream_send(
                "debug", f"Unknown command {command} for {scraper.config.domain}..."
            )
            continue


class ScraperController(ClusterController):
    config_cls = None
    scraper_cls = None

    def initialize(self, config_cls, scraper_cls, sessions):
        assert issubclass(
            config_cls, ScraperConfig
        ), "config_cls must subclass ScraperConfig"
        assert issubclass(scraper_cls, Scraper), "scraper_cls must subclass Scraper"
        self.config_cls = config_cls
        self.scraper_cls = scraper_cls
        self.sessions = sessions

    async def create_resource(self, config):
        name = config["metadata"]["name"]

        try:
            spec = config["spec"]
            assert isinstance(spec, dict), "spec must be a dictionary, got " + str(
                type(spec)
            )
            config = self.config_cls(**spec)
        except KeyError as e:
            self.itl.stream_send(
                "debug", f"missing key in config PhilomenaScraper/{name}: {e}"
            )
            return None
        except ValidationError as e:
            self.itl.stream_send(
                "debug", f"invalid config PhilomenaScraper/{name}: {e}"
            )
            return None
        except AssertionError as e:
            self.itl.stream_send(
                "debug", f"invalid value in config PhilomenaScraper/{name}: {e}"
            )
            return None

        task_queue = asyncio.Queue()
        session = self.sessions[config.domain]
        scraper = self.scraper_cls(config, session)

        asyncio.create_task(
            scraper_task(
                self.itl, task_queue, scraper, self.patch, lambda patch: (name, patch)
            ),
            name='scraper:' + name,
        )

        self.itl.ondisconnect(lambda: task_queue.put_nowait(("stop", ())))

        return task_queue

    async def update_resource(self, task_queue: asyncio.Queue, config):
        name = config["metadata"]["name"]

        try:
            spec = config["spec"]
            assert isinstance(spec, dict), "spec must be a dictionary, got " + str(
                type(spec)
            )
            config = self.config_cls(**spec)
        except KeyError as e:
            self.itl.stream_send(
                "debug", f"missing key in config PhilomenaScraper/{name}: {e}"
            )
            return None
        except ValidationError as e:
            self.itl.stream_send(
                "debug", f"invalid config PhilomenaScraper/{name}: {e}"
            )
            return None
        except AssertionError as e:
            self.itl.stream_send(
                "debug", f"invalid value in config PhilomenaScraper/{name}: {e}"
            )
            return None

        try:
            task_queue.put_nowait(("update-config", config))
            return task_queue
        except:
            traceback.print_exc()
            raise

    async def delete_resource(self, task_queue: asyncio.Queue):
        task_queue.put_nowait(("stop", ()))
