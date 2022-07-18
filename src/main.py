import asyncio
import random
import csv
import faker
from typing import Any, Iterable, NamedTuple, Coroutine

from config import logger
from detmir import DetmirAPI


class Product(NamedTuple):
    """
    Product data container
    """

    id: str
    title: str
    price: float
    promo_price: float
    url: str


def dump_to_csv(filename: str, products: Iterable[Product]) -> None:
    """
    Dump products list to csv file with filename file name
    """

    with open(f"{filename}.csv", "w") as ouf:
        writer = csv.writer(ouf)

        writer.writerow(Product._fields)

        for product in products:
            writer.writerow(product)


def parse_product_data(results: list[list[dict[str, Any]]]) -> list[Product]:
    """
    Extract product data from fetched results
    """

    products: list[Product] = []

    for result in results:
        for item in result:
            price = item["price"]
            promo_price = item["old_price"]

            if price is not None:
                price = round(float(price["price"]), 2)
            
            if promo_price is not None:
                promo_price = round(float(promo_price["price"]), 2)

            if item["promo"]:
                price, promo_price = promo_price, price

            product = Product(
                id=int(item["id"]),
                title=item["title"],
                price=price,
                promo_price=promo_price,
                url=item["link"]["web_url"],
            )

            products.append(product)
    
    return products


async def main():
    # specifying category slug/alias (it is present in the url)
    category_alias = "nutrition_feeding" # 24_316 products
    # category_alias = "block" # 3_099 products

    # list of city codes (Moscow and St. Petersburg)
    # city code can be viewed via network tab when products loaded
    region_codes: list[str] = [
        "RU-MOW",
        "RU-SEP",
    ]

    # purchased 10 proxies for 300 rubles
    # there should be a .env file
    proxies: list[str] = [
        "http://2Fk6Uc:bg9bDa@194.28.211.18:9682",
        "http://2Fk6Uc:bg9bDa@194.28.211.210:9031",
        "http://2Fk6Uc:bg9bDa@194.28.211.114:9521",
        "http://2Fk6Uc:bg9bDa@194.28.209.200:9879",
        "http://2Fk6Uc:bg9bDa@194.28.210.138:9727",
        "http://2Fk6Uc:bg9bDa@194.28.210.143:9367",
        "http://2Fk6Uc:bg9bDa@194.28.211.9:9183",
        "http://2Fk6Uc:bg9bDa@194.28.208.138:9391",
        "http://2Fk6Uc:bg9bDa@194.28.208.83:9858",
        "http://2Fk6Uc:bg9bDa@194.28.210.63:9838",
    ]

    fake = faker.Faker()

    async with DetmirAPI(limit=asyncio.Semaphore(10), rate=5.0) as api:
        for region_code in region_codes:
            fltr = f"categories[].alias:{category_alias};withregion:{region_code}"

            # making first API hit in order
            # to get total count of products in the category
            data: dict[str, Any] = await api.fetch_products(params={
                "filter": fltr,
                "meta": "*",
            })

            total_products: int = data["meta"]["length"]
            logger.info(f"Total products: {total_products}")


            tasks: list[Coroutine] = []
            offset = 0

            # fetching by 100 products
            while offset < total_products:
                task = asyncio.create_task(
                    api.fetch_products(
                        proxy=random.choice(proxies),
                        params={
                            "filter": fltr,
                            "limit": DetmirAPI.PRODUCTS_FETCH_LIMIT,
                            "offset": offset,
                        },
                        headers={
                            "Connection": "keep-alive",
                            # "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
                            "User-Agent": fake.user_agent(),
                        },
                    )
                )

                tasks.append(task)

                offset += DetmirAPI.PRODUCTS_FETCH_LIMIT

            results: list[list[dict[str, Any]]] = await asyncio.gather(*tasks)

            products: list[Product] = parse_product_data(results)

            dump_to_csv(f"{region_code}__{category_alias}", products)


if __name__ == "__main__":
    asyncio.run(main())
