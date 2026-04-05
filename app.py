import asyncio
import os
import random
import re
from urllib.parse import urlencode
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fake_useragent import UserAgent

load_dotenv()


class KufarAutoParser:
    def __init__(self):
        self.ua = UserAgent()
        self.base_url = os.getenv("BASE_URL", "https://www.kufar.by/l/r~belarus")

        self.query = os.getenv("QUERY")
        self.max_pages = int(os.getenv("MAX_PAGES", "20"))
        self.batch_size = int(os.getenv("BATCH_SIZE", "3"))
        self.page_param = os.getenv("PAGE_PARAM", "page")

        self.min_delay = float(os.getenv("MIN_DELAY", "1.5"))
        self.max_delay = float(os.getenv("MAX_DELAY", "3.5"))

        self.max_retries = int(os.getenv("MAX_RETRIES", "5"))
        self.concurrency = int(os.getenv("CONCURRENCY", "2"))
        self.sem = asyncio.Semaphore(self.concurrency)

        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Referer": "https://kufar.by",
        }

    def build_url(self, page: int) -> str:
        params = {
            "ot": 1,
            "query": self.query,
            "rgn": "all",
            "sort": "lst.d",
            self.page_param: page,
        }
        return f"{self.base_url}?{urlencode(params)}"

    async def fetch_page(self, client: httpx.AsyncClient, url: str):
        for attempt in range(1, self.max_retries + 1):
            self.headers["User-Agent"] = self.ua.random

            await asyncio.sleep(random.uniform(0.3, 1.0))

            try:
                async with self.sem:
                    response = await client.get(url, headers=self.headers, timeout=25)

                if response.status_code == 200:
                    return response.text

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = random.uniform(5, 10)
                    else:
                        delay = min(60, (2 ** attempt) + random.uniform(1.0, 3.0))

                    print(f"429 | попытка {attempt}/{self.max_retries} | пауза {delay:.1f} сек | {url}")
                    await asyncio.sleep(delay)
                    continue

                print(f"Ошибка доступа: {response.status_code} | {url}")
                return None

            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as e:
                delay = min(30, (2 ** attempt) + random.uniform(1.0, 2.5))
                print(f"Сетевая ошибка: {e} | попытка {attempt}/{self.max_retries} | пауза {delay:.1f} сек")
                await asyncio.sleep(delay)

            except Exception as e:
                print(f"Ошибка запроса: {e} | {url}")
                return None

        return None

    def parse_total_count(self, soup: BeautifulSoup):
        total_tag = soup.find("span", class_=lambda x: x and "styles_total" in x)
        if not total_tag:
            return None

        text = total_tag.get_text(" ", strip=True)
        digits = re.sub(r"[^\d]", "", text)
        return int(digits) if digits else None

    def parse_items(self, soup: BeautifulSoup):
        items = []

        listings_block = soup.find("div", attrs={"data-name": "listings"})
        if not listings_block:
            return items

        cards = listings_block.find_all("section")
        for card in cards:
            title = card.find("h3", class_=lambda x: x and "styles_title" in x)
            if not title:
                continue

            price = card.find("p", class_=lambda x: x and "styles_price" in x)
            region = card.find("p", class_=lambda x: x and "styles_region" in x)
            date_tag = card.find("div", class_=lambda x: x and "styles_secondary" in x)

            items.append({
                "Название": title.get_text(" ", strip=True),
                "Цена": price.get_text(" ", strip=True) if price else "Договорная",
                "Регион": region.get_text(" ", strip=True) if region else "---",
            })

        return items

    async def run_once(self):
        all_results = []
        total_count = None

        limits = httpx.Limits(
            max_connections=self.concurrency,
            max_keepalive_connections=self.concurrency,
        )

        async with httpx.AsyncClient(http2=True, follow_redirects=True, limits=limits) as client:
            for batch_start in range(1, self.max_pages + 1, self.batch_size):
                batch_end = min(batch_start + self.batch_size - 1, self.max_pages)
                print(f"\n=== Страницы {batch_start}-{batch_end} ===")

                tasks = []
                for page in range(batch_start, batch_end + 1):
                    url = self.build_url(page)
                    tasks.append(self.fetch_page(client, url))

                html_pages = await asyncio.gather(*tasks)

                for page_num, html in enumerate(html_pages, start=batch_start):
                    if not html:
                        print(f"[{page_num}] HTML не получен")
                        continue

                    soup = BeautifulSoup(html, "html.parser")

                    if total_count is None:
                        total_count = self.parse_total_count(soup)

                    items = self.parse_items(soup)
                    all_results.extend(items)

                    print(f"[{page_num}] найдено объявлений: {len(items)}")

                if batch_end < self.max_pages:
                    wait = random.uniform(self.min_delay, self.max_delay)
                    print(f"Пауза {wait:.2f} сек")
                    await asyncio.sleep(wait)

        return all_results, total_count


if __name__ == "__main__":
    parser = KufarAutoParser()

    data, total = asyncio.run(parser.run_once())

    print("\n" + "=" * 50)
    print(f"ВСЕГО В ЗАГОЛОВКЕ: {total}")
    print(f"СОБРАНО: {len(data)}")
    print("=" * 50)

    if data:
        df = pd.DataFrame(data)
        df.to_excel("kufar_check.xlsx", index=False)
        print("Сохранено в kufar_check.xlsx")
    else:
        print("Данные не найдены")