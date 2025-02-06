import scrapling
from playwright._impl._errors import TargetClosedError
from scrapling import StealthyFetcher, PlayWrightFetcher, AsyncFetcher

from utilities.helper import o_logger


async def make_request_with_retries(s_url: str) -> scrapling.Adaptor | None:
    """
    Make a request with StealthyFetcher, AsyncFetcher and PlaywrightFetcher
    :param s_url: str - URL of the request
    :return: scrapling.Adaptor | None - Response of the request
    """
    try:
        o_response = await make_request_with_stealthy_fetcher(s_url)
    except AttributeError as e:
        o_logger.warning(f"Getting AttributeError for {s_url}: {e}")
        o_response = None
    if o_response is None:
        o_response = await make_request_with_playwright_fetcher(s_url)
    if o_response is None:
        o_response = await make_request_with_async_fetcher(s_url)
    return o_response


async def make_request_with_stealthy_fetcher(s_url: str) -> scrapling.Adaptor | None:
    """
    Make a request with StealthyFetcher
    :param s_url: str - URL of the request
    :return: scrapling.Adaptor | None - Response of the request
    """
    try:
        o_logger.info(f"Making request to {s_url} [StealthyFetcher | async mode]")
        page = await StealthyFetcher().async_fetch(s_url, timeout=30000,
                                                   # disable_resources=True, disable_ads=True,
                                                   network_idle=True, humanize=True
                                                   # , headless=False
                                                   )
        if page.status == 200:
            o_logger.info(f"Request successful ({page.status}) [StealthyFetcher]")
            return page
    except TargetClosedError as e:
        o_logger.error(f"Getting TargetClosedError for {s_url}: {e}")
        await page.unroute_all(behavior='ignoreErrors')
    except Exception as e:
        o_logger.error(f"Error while fetching {s_url} with StealthyFetcher: {e}")
    return None


async def make_request_with_playwright_fetcher(s_url: str) -> scrapling.Adaptor | None:
    """
    Make a request with PlaywrightFetcher
    :param s_url: str - URL of the request
    :return: scrapling.Adaptor | None - Response of the request
    """
    try:
        o_logger.info(f"Making request to {s_url} [PlaywrightFetcher | async mode]")
        page = await PlayWrightFetcher().async_fetch(s_url, timeout=60000, stealth=True, disable_resources=True,
                                                     real_chrome=True)
        if page.status == 200:
            o_logger.info(f"Request successful ({page.status}) [PlaywrightFetcher]")
            return page
    except TargetClosedError as e:
        o_logger.error(f"Getting TargetClosedError for {s_url}: {e}")
        await page.unroute_all(behavior='ignoreErrors')
    except Exception as e:
        o_logger.error(f"Error while fetching {s_url} with PlaywrightFetcher: {e}")
    return None


async def make_request_with_async_fetcher(s_url: str) -> scrapling.Adaptor | None:
    """
    Make a request with AsyncFetcher
    :param s_url: str - URL of the request
    :return: scrapling.Adaptor | None - Response of the request
    """
    try:
        o_logger.info(f"Making request to {s_url} [AsyncFetcher | async mode]")
        page = await AsyncFetcher().get(s_url, timeout=30000, stealthy_headers=True, follow_redirects=True)
        if page.status == 200:
            o_logger.info(f"Request successful ({page.status}) [AsyncFetcher]")
            return page
    except Exception as e:
        o_logger.error(f"Error while fetching {s_url} with AsyncFetcher: {e}")
    return None
