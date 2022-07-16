import asyncio
import random
import csv
from typing import Any, Iterable, NamedTuple, Coroutine

from config import logger
from detmir import Parser


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


def extract_products(results: dict[str, Any]) -> list[Product]:
    """
    Extract product data from fetched results
    """

    products: list[Product] = []

    for result in results:
        items = result["items"]

        for item in items:
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


async def main() -> None:
    # specifying category slug/alias (it is present in the url)
    category_alias = "nutrition_feeding"
    # category_alias = "block"

    # list of city codes (Moscow and St. Petersburg)
    # city code can be viewed via network tab when products loaded
    region_codes: list[str] = ["RU-MOW", "RU-SEP"]

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


    async with Parser(limit=asyncio.Semaphore(10), rate=5.0) as p:
        for region_code in region_codes:
            # making first API hit in order
            # to get total count of products in the category
            data: dict[str, Any] = await p.fetch_products(params={
                "category_alias": category_alias,
                "region": region_code,
                "limit": Parser.PRODUCTS_FETCH_LIMIT,
                "offset": 0,
            }, proxy=random.choice(proxies))

            total_products: int = data["meta"]["length"]

            offset = 0
            tasks: list[Coroutine] = []

            logger.info(f"Total products: {total_products}")

            # fetching by 100 products in the loop
            while offset < total_products:
                tasks.append(
                    p.fetch_products(params={
                        "category_alias": category_alias,
                        "region": region_code,
                        "limit": Parser.PRODUCTS_FETCH_LIMIT,
                        "offset": offset,
                    }, proxy=random.choice(proxies))
                )

                offset += Parser.PRODUCTS_FETCH_LIMIT

            results: list[dict[str, Any]] = await asyncio.gather(*tasks)

            products: list[Product] = extract_products(results)

            dump_to_csv(f"{region_code}__{category_alias}", products)


if __name__ == "__main__":
    asyncio.run(main())
