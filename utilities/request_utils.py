import asyncio
import random
import warnings

import scrapling
from scrapling import StealthyFetcher, PlayWrightFetcher, AsyncFetcher

from utilities.logging_utils import LoggerManager

o_logger = LoggerManager.get_logger(__name__)

FETCHERS = {
    "StealthyFetcher": (StealthyFetcher, "async_fetch",
                        {
                            "timeout": 30000,
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
    :param max_retries: Number of total retries before giving up
    :return: scrapling.Adaptor | None - Response of the request
    """
    for attempt in range(max_retries):  # 🔹 Retry the entire process up to max_retries times
        for fetcher_name, (fetcher_class, fetch_method, params) in FETCHERS.items():
            fetcher_instance = fetcher_class()
            fetch_fn = getattr(fetcher_instance, fetch_method)

            # Wait before first attempt (randomized backoff)
            first_backoff = random.uniform(1, 5)
            o_logger.info(
                f"Sleeping for {first_backoff:.2f} seconds before attempt {attempt + 1} with {fetcher_name}...")
            await asyncio.sleep(first_backoff)

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always", RuntimeWarning)

                try:
                    page = await fetch_fn(s_url, **params)

                    if page and hasattr(page, "status") and page.status == 200:
                        o_logger.info(f"Request successful ({page.status}) [{fetcher_name}]")
                        return page

                except Exception as e:
                    o_logger.warning(f"Attempt {attempt + 1}: {fetcher_name} failed with error: {e}")

                for warning in w:
                    if issubclass(warning.category, RuntimeWarning):
                        o_logger.warning(f"RuntimeWarning: {warning.message}")

            o_logger.warning(f"{fetcher_name} failed, switching to next fetcher.")

        # Exponential backoff before retrying entire process
        backoff_time = min(2 ** attempt + random.uniform(1, 3), 20)
        o_logger.warning(f"Attempt {attempt + 1} failed. Retrying entire process in {backoff_time:.2f} seconds...")
        await asyncio.sleep(backoff_time)

    o_logger.error(f"All {max_retries} attempts failed for {s_url}")
    return None
