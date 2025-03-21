import asyncio
import random
import warnings

import scrapling
from playwright._impl._errors import TargetClosedError
from scrapling import StealthyFetcher, PlayWrightFetcher, AsyncFetcher

from utilities.logging_utils import LoggerManager

o_logger = LoggerManager.get_logger(__name__)

FETCHERS = {
    "PlayWrightFetcher": (PlayWrightFetcher, "async_fetch",
                          {
                              "timeout": 30000,
                              "stealth": True,
                              "disable_resources": True,
                              "real_chrome": True
                          }),
    "StealthyFetcher": (StealthyFetcher, "async_fetch",
                        {
                            "timeout": 30000,
                            "network_idle": True,
                            "humanize": True
                        }),
    "AsyncFetcher": (AsyncFetcher, "get",
                     {
                         "timeout": 30000,
                         "stealthy_headers": True,
                         "follow_redirects": True
                     })
}

async def retry_with_backoff(coro, max_retries, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            return await coro(*args, **kwargs)
        except Exception as e:
            backoff_time = min(2 ** attempt + random.uniform(1, 3), 20)
            o_logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {backoff_time:.2f} seconds...")
            await asyncio.sleep(backoff_time)
    o_logger.error(f"All {max_retries} retries failed.")
    return None

async def make_request_with_retries(s_url: str, max_retries: int = 5) -> scrapling.Adaptor | None:
    """
    Attempt a request using multiple fetchers with retries in case of failure.
    :param s_url: URL to fetch
    :param max_retries: Number of retries before switching fetchers
    :return: scrapling.Adaptor | None - Response of the request
    """
    for fetcher_name, (fetcher_class, fetch_method, params) in FETCHERS.items():
        fetcher_instance = fetcher_class()
        fetch_fn = getattr(fetcher_instance, fetch_method)

        first_backoff = random.uniform(1, 5)
        o_logger.info(f"Sleeping for {first_backoff:.2f} seconds before first attempt with {fetcher_name}...")
        await asyncio.sleep(first_backoff)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always", RuntimeWarning)
            page = await retry_with_backoff(fetch_fn, max_retries, s_url, **params)

            if page and page.status == 200:
                o_logger.info(f"Request successful ({page.status}) [{fetcher_name}]")
                return page

            for warning in w:
                if issubclass(warning.category, RuntimeWarning):
                    o_logger.warning(f"RuntimeWarning: {warning.message}")

        o_logger.warning(f"{fetcher_name} failed after {max_retries} retries, switching to next fetcher.")

    o_logger.error(f"All fetchers failed for {s_url}")
    return None