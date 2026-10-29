import asyncio
import socket
import unittest
from time import perf_counter

from aiohttp import web

from async_crawler import AsyncCrawler


class CrawlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.active = 0
        self.peak = 0
        self.transports = set()

        async def page(request):
            self.transports.add(request.transport)
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                await asyncio.sleep(float(request.query.get("delay", "0")))
                return web.Response(text=request.query.get("body", "Привет"))
            finally:
                self.active -= 1

        async def error(request):
            return web.Response(status=int(request.match_info["code"]))

        app = web.Application()
        app.router.add_get("/page", page)
        app.router.add_get("/status/{code}", error)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def test_success_empty_duplicates_and_pool(self):
        url = self.base + "/page"
        empty = url + "?body="
        async with AsyncCrawler() as crawler:
            with self.assertLogs("async_crawler", level="INFO") as logs:
                self.assertEqual(await crawler.fetch_url(url), "Привет")
                self.assertEqual(await crawler.fetch_url(url), "Привет")
            self.assertIn("Начало загрузки", " ".join(logs.output))
            self.assertIn("Успешно", " ".join(logs.output))
            self.assertEqual(len(self.transports), 1)
            self.assertEqual(await crawler.fetch_urls([url, empty, url]), {url: "Привет", empty: ""})
            self.assertEqual(await crawler.fetch_urls([]), {})
        self.assertTrue(crawler._session.closed)
        await crawler.close()
        with self.assertRaises(RuntimeError):
            await crawler.fetch_url(url)

    async def test_http_errors_and_invalid_url(self):
        good = self.base + "/page"
        bad = [self.base + "/missing", self.base + "/status/500", "http://"]
        async with AsyncCrawler() as crawler:
            with self.assertLogs("async_crawler", level="WARNING") as logs:
                self.assertEqual(await crawler.fetch_urls([good] + bad), {good: "Привет"})
            self.assertEqual(set(crawler.errors), set(bad))
            self.assertEqual(crawler.statuses[bad[0]], 404)
            self.assertEqual(crawler.statuses[bad[1]], 500)
            self.assertIn("ClientResponseError", " ".join(logs.output))

    async def test_network_error(self):
        # Занятый, но не слушающий порт: без DNS и зависимости от Интернета.
        with socket.socket() as reserved:
            reserved.bind(("127.0.0.1", 0))
            url = f"http://127.0.0.1:{reserved.getsockname()[1]}/"
            async with AsyncCrawler(connect_timeout=3) as crawler:
                self.assertEqual(await crawler.fetch_url(url), "")
                self.assertIn("ClientConnectorError", crawler.errors[url])

    async def test_read_timeout_and_recovery(self):
        slow = self.base + "/page?delay=0.5"
        async with AsyncCrawler(read_timeout=0.15) as crawler:
            self.assertEqual(await crawler.fetch_url(slow), "")
            self.assertIn("Timeout", crawler.errors[slow])
            self.assertEqual(await crawler.fetch_url(self.base + "/page"), "Привет")

    async def test_total_timeout(self):
        async with AsyncCrawler(total_timeout=0.04) as crawler:
            url = self.base + "/page?delay=0.2"
            self.assertEqual(await crawler.fetch_url(url), "")
            self.assertIn("Timeout", crawler.errors[url])

    async def test_concurrency_and_speed(self):
        urls = [self.base + f"/page?delay=0.15&id={i}" for i in range(6)]
        async with AsyncCrawler(max_concurrent=3) as crawler:
            start = perf_counter()
            for url in urls:
                await crawler.fetch_url(url)
            sequential = perf_counter() - start
            self.peak = 0
            start = perf_counter()
            self.assertEqual(len(await crawler.fetch_urls(urls)), 6)
            parallel = perf_counter() - start
            self.assertEqual(self.peak, 3)
            self.assertLess(parallel, sequential * 0.75)

    async def test_validation_and_unused_close(self):
        for value in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                AsyncCrawler(value)
        with self.assertRaises(ValueError):
            AsyncCrawler(read_timeout=0)
        crawler = AsyncCrawler()
        await crawler.close()
        await crawler.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
