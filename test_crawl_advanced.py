import asyncio
import unittest

from aiohttp import web

from async_crawler import AsyncCrawler


class AdvancedCrawlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.active = 0
        self.peak = 0
        self.hits = []

        async def page(request):
            self.active += 1
            self.peak = max(self.peak, self.active)
            self.hits.append(request.path)
            try:
                await asyncio.sleep(0.06)
                if request.path == "/":
                    links = '<a href="/a">A</a><a href="/b">B</a><a href="/skip">Skip</a><a href="https://other.example/x">External</a>'
                else:
                    links = '<a href="/">Home</a>'
                return web.Response(text=f"<title>{request.path}</title>{links}", content_type="text/html")
            finally:
                self.active -= 1

        app = web.Application()
        app.router.add_get("/{tail:.*}", page)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        await web.TCPSite(self.runner, "127.0.0.1", 0).start()
        self.base = f"http://127.0.0.1:{self.runner.addresses[0][1]}"

    async def asyncTearDown(self):
        await self.runner.cleanup()

    async def test_filters_limits_progress_and_deduplication(self):
        updates = []
        async with AsyncCrawler(max_concurrent=4, per_domain_limit=2, max_depth=1) as crawler:
            pages = await crawler.crawl(
                [self.base + "/"], max_pages=10, same_domain_only=True,
                exclude_patterns=[r"/skip$"], include_patterns=[r"127\.0\.0\.1"],
                progress_callback=updates.append,
            )
            self.assertEqual(set(pages), {self.base + "/", self.base + "/a", self.base + "/b"})
            self.assertEqual(crawler.visited_urls, set(pages))
            self.assertEqual(crawler.queue.get_stats()["total"], 3)
            self.assertEqual(crawler.crawl_stats["processed"], 3)
            self.assertEqual(crawler.crawl_stats["queued"], 0)
            self.assertEqual(crawler.crawl_stats["failed"], 0)
            self.assertGreater(crawler.crawl_stats["pages_per_second"], 0)
            self.assertEqual(crawler.semaphore_manager.peak_active, 2)
            self.assertEqual(crawler.semaphore_manager.total_active, 0)
            self.assertGreaterEqual(len(updates), 4)
        self.assertEqual(set(self.hits), {"/", "/a", "/b"})
        self.assertEqual(len(self.hits), 3)
        self.assertEqual(self.peak, 2)

    async def test_budget_counts_started_requests(self):
        async with AsyncCrawler(max_concurrent=4, per_domain_limit=4) as crawler:
            pages = await crawler.crawl([self.base + "/"], max_pages=2)
            self.assertEqual(len(pages), 2)
            self.assertEqual(len(crawler.visited_urls), 2)
            self.assertEqual(crawler.queue.get_stats()["pending"], 3)

    async def test_include_filter_and_invalid_seed(self):
        async with AsyncCrawler() as crawler:
            pages = await crawler.crawl([self.base + "/"], include_patterns=[r"/a$"])
            self.assertEqual(pages, {})
            self.assertEqual(crawler.visited_urls, set())
            with self.assertRaises(ValueError):
                await crawler.crawl(["javascript:alert(1)"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
