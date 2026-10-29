import unittest
from aiohttp import web

from async_crawler import AsyncCrawler


class BasicCrawlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.hits = {}

        async def page(request):
            path = request.path
            self.hits[path] = self.hits.get(path, 0) + 1
            if path == "/":
                html = '<title>Start</title><a href="/a">A</a><a href="/a">A again</a><a href="/missing">Bad</a>'
            elif path == "/a":
                html = '<title>A</title><a href="/">Home</a><a href="/b">B</a>'
            elif path == "/b":
                html = '<title>B</title>'
            else:
                return web.Response(status=404)
            return web.Response(text=html, content_type="text/html")

        app = web.Application()
        app.router.add_get("/{tail:.*}", page)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def test_discovery_errors_and_deduplication(self):
        async with AsyncCrawler() as crawler:
            result = await crawler.crawl([self.base + "/", self.base + "/"], max_pages=10)
            self.assertEqual(set(result), {self.base + "/", self.base + "/a", self.base + "/b"})
            self.assertEqual(crawler.visited_urls, set(result) | {self.base + "/missing"})
            self.assertIn("ClientResponseError", crawler.failed_urls[self.base + "/missing"])
            self.assertEqual(crawler.queue.get_stats(), {
                "total": 4, "pending": 0, "active": 0, "processed": 3, "failed": 1,
            })
        self.assertEqual(self.hits, {"/": 1, "/a": 1, "/missing": 1, "/b": 1})

    async def test_page_budget_and_fresh_run(self):
        async with AsyncCrawler() as crawler:
            first = await crawler.crawl([self.base + "/"], max_pages=1)
            self.assertEqual(set(first), {self.base + "/"})
            self.assertEqual(crawler.queue.get_stats()["pending"], 2)
            second = await crawler.crawl([self.base + "/b"], max_pages=1)
            self.assertEqual(set(second), {self.base + "/b"})
            self.assertEqual(crawler.visited_urls, {self.base + "/b"})

    async def test_invalid_budget(self):
        crawler = AsyncCrawler()
        for value in (0, -1, True, 1.5):
            with self.assertRaises(ValueError):
                await crawler.crawl([self.base + "/"], value)
        await crawler.close()

    async def test_depth_zero_stops_at_start(self):
        async with AsyncCrawler(max_depth=0) as crawler:
            result = await crawler.crawl([self.base + "/"], max_pages=10)
            self.assertEqual(set(result), {self.base + "/"})
            self.assertEqual(crawler.url_depths, {self.base + "/": 0})
            self.assertEqual(crawler.queue.get_stats()["pending"], 0)
        self.assertEqual(self.hits, {"/": 1})

    async def test_depth_one_and_two(self):
        async with AsyncCrawler(max_depth=1) as crawler:
            result = await crawler.crawl([self.base + "/"], max_pages=10)
            self.assertEqual(set(result), {self.base + "/", self.base + "/a"})
            self.assertEqual(crawler.url_depths[self.base + "/a"], 1)
            self.assertNotIn(self.base + "/b", crawler.url_depths)
        self.assertNotIn("/b", self.hits)
        async with AsyncCrawler(max_depth=2) as crawler:
            result = await crawler.crawl([self.base + "/"], max_pages=10)
            self.assertIn(self.base + "/b", result)
            self.assertEqual(crawler.url_depths[self.base + "/b"], 2)

    async def test_invalid_depth(self):
        for value in (-1, True, 1.5, "2"):
            with self.assertRaises(ValueError):
                AsyncCrawler(max_depth=value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
