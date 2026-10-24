"""python demo.py — Интернет; python demo.py --local — воспроизводимое сравнение."""

import argparse
import asyncio
import logging
from time import perf_counter

from aiohttp import web

from async_crawler import AsyncCrawler

URLS = [
    "https://example.com",
    "https://www.python.org",
    "https://docs.aiohttp.org/en/stable/",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
]


async def benchmark(urls: list[str], parallel: bool):
    start = perf_counter()
    async with AsyncCrawler(max_concurrent=5) as crawler:
        if parallel:
            results = await crawler.fetch_urls(urls)
        else:
            results = {}
            for url in urls:
                body = await crawler.fetch_url(url)
                if url not in crawler.errors:
                    results[url] = body
        elapsed = perf_counter() - start
        print("\n" + ("Параллельно" if parallel else "Последовательно"))
        for url in urls:
            status = crawler.statuses.get(url, "нет ответа")
            detail = crawler.errors.get(url, f"OK, {len(results.get(url, ''))} символов")
            print(f"  HTTP {status} | {url} | {detail}")
        print(f"Успешно: {len(results)}/{len(urls)}. Время: {elapsed:.3f} с")
    return elapsed


async def main(local: bool = False):
    runner = None
    try:
        urls = URLS
        if local:
            async def page(request: web.Request):
                await asyncio.sleep(0.3)
                return web.Response(text=f"Страница {request.match_info['number']}")

            app = web.Application()
            app.router.add_get("/page/{number}", page)
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            port = runner.addresses[0][1]
            urls = [f"http://127.0.0.1:{port}/page/{i}" for i in range(6)]
        sequential = await benchmark(urls, parallel=False)
        parallel = await benchmark(urls, parallel=True)
        print(f"\nОтношение времён (последовательно / параллельно): {sequential / parallel:.2f}x")
        if parallel >= sequential:
            print("В этом запуске ускорения нет: задержки внешних сайтов могут меняться.")
    finally:
        if runner is not None:
            await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="Локальный сервер с задержкой 0.3 с")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main(args.local))
