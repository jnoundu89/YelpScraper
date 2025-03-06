import asyncio
import random

import scrapling
from playwright._impl._errors import TargetClosedError
from scrapling import StealthyFetcher, PlayWrightFetcher, AsyncFetcher

from utilities.helper import o_logger

FETCHERS = {
    "StealthyFetcher": (StealthyFetcher, "async_fetch",
                        {
                            "timeout": 60000,
                            "network_idle": True,
                            "humanize": True
                        }),
    "PlayWrightFetcher": (PlayWrightFetcher, "async_fetch",
                          {
                              "timeout": 30000,
                              "stealth": True,
                              "disable_resources": True,
                              "real_chrome": True
                          }),
    "AsyncFetcher": (AsyncFetcher, "get",
                     {
                         "timeout": 30000,
                         "stealthy_headers": True,
                         "follow_redirects": True
                     })
}


async def make_request_with_retries(s_url: str, max_retries: int = 3) -> scrapling.Adaptor | None:
    """
    Attempt a request using multiple fetchers with retries in case of failure.
    :param s_url: URL to fetch
    :param max_retries: Number of retries before switching fetchers
    :return: scrapling.Adaptor | None - Response of the request
    """
    for fetcher_name, (fetcher_class, fetch_method, params) in FETCHERS.items():
        for attempt in range(max_retries):
            try:
                o_logger.info(f"Attempt {attempt + 1} using {fetcher_name} for {s_url}")
                fetcher_instance = fetcher_class()
                fetch_fn = getattr(fetcher_instance, fetch_method)

                page = await fetch_fn(s_url, **params)

                if page.status == 200:
                    o_logger.info(f"Request successful ({page.status}) [{fetcher_name}]")
                    return page
            except (AttributeError, TargetClosedError) as e:
                o_logger.warning(f"{fetcher_name} failed on {s_url}: {e}")
            except Exception as e:
                o_logger.error(f"Unexpected error with {fetcher_name}: {e}")

            backoff_time = min(2 ** attempt + random.uniform(1, 3), 20)
            o_logger.info(f"Sleeping for {backoff_time:.2f} seconds before retry...")
            await asyncio.sleep(backoff_time)

        o_logger.info(f"{fetcher_name} failed after {max_retries} retries, switching to next fetcher.")

    o_logger.error(f"All fetchers failed for {s_url}")
    return None
