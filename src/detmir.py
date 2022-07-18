import aiohttp
import asyncio
from typing import Any

from config import logger


class DetmirAPI:
    """
    Async context manager is supported

    We can only get 100 products per request
    """

    BASE_ENDPOINT = "https://api.detmir.ru/v2"
    PRODUCTS_ENDPOINT = f"{BASE_ENDPOINT}/products"
    PRODUCTS_FETCH_LIMIT = 100

    def __init__(self, limit: asyncio.Semaphore, rate: float = 0.0) -> None:
        """
        limit - request count limiter
        rate - time period
        """

        self.limit = limit
        self.rate = rate
        self.session = aiohttp.ClientSession()
        # for fake user agent
        # self.fake = faker.Faker()

        # tmp
        # ClientPayloadError: Response payload is not completed
        self.headers = {
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
        }

    async def __aenter__(self):
        self.i = 0
        return self

    async def __aexit__(self, *excinfo):
        self.i = 0
        await self.session.close()

    async def fetch_products(self, **kwargs: Any) -> dict[str, Any]:
        """
        Fetching products via API
        """

        async with self.limit, self.session.get(
            self.PRODUCTS_ENDPOINT,
            **kwargs,
        ) as response:
            logger.info(f"FETCHING: {self.i}")

            self.i += self.PRODUCTS_FETCH_LIMIT

            await asyncio.sleep(self.rate)

            return await response.json()


# filter=categories[].alias:nutrition_feeding;promo:false;withregion:RU-MOW
# meta=*
# limit=100
# offset=0
# sort=popularity:desc
# RU-MOW
# RU-SPE
