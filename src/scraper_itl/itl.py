from collections import defaultdict
from itllib import Itl
from itllib.controllers import SyncedResources, ClusterController
from pydantic import BaseModel, ValidationError

from .controller import ScraperController
from .ratelimited import RateLimitedSession


class RateLimit(BaseModel):
    domain: str
    period: int = 1
    limit: int = 1


class RateLimitedSessions(ClusterController):
    @classmethod
    def create_sessions(cls) -> dict[str, RateLimitedSession]:
        return defaultdict(lambda: RateLimitedSession(1))

    def initialize(self, sessions):
        if sessions == None:
            raise ValueError("'sessions' arg is required")
        self._sessions: dict[str, RateLimitedSession] = sessions

    async def create_resource(self, config):
        name = config["metadata"]["name"]

        try:
            spec = config["spec"]
            assert isinstance(spec, dict), "spec must be a dictionary, got " + str(
                type(spec)
            )
            config = RateLimit(**spec)
        except KeyError as e:
            self.itl.stream_send(
                "debug", f"missing key in config RateLimit/{name}: {e}"
            )
            return None
        except ValidationError as e:
            self.itl.stream_send("debug", f"invalid config RateLimit/{name}: {e}")
            return None
        except AssertionError as e:
            self.itl.stream_send(
                "debug", f"invalid value in config RateLimit/{name}: {e}"
            )
            return None

        self._sessions[config.domain].update(config.limit / config.period)
        return self._sessions[config.domain]

    async def update_resource(self, resource, config):
        return await self.create_resource(config)

    async def delete_resource(self, resource):
        self._sessions[resource.domain].update(1)


class ScraperItl(Itl):
    def __init__(self, *args, cluster=None, **kwargs):
        if cluster == None:
            raise ValueError("cluster is required")

        super().__init__(*args, **kwargs)
        self.sessions = RateLimitedSessions.create_sessions()
        self.scraper_configs = SyncedResources(self, cluster)
        self.scraper_configs.register("scrapers.synthbot.ai", "v1", "RateLimit")(
            RateLimitedSessions, self.sessions
        )

    def register_scraper(self, group, version, kind, configCls, scraperCls):
        self.scraper_configs.register(group, version, kind)(
            ScraperController, configCls, scraperCls, self.sessions
        )
        return scraperCls
