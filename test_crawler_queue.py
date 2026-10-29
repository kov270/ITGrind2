import asyncio
import unittest

from crawler_queue import CrawlerQueue


class QueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_priority_and_fifo(self):
        queue = CrawlerQueue()
        queue.add_url("https://example.com/later", priority=10)
        queue.add_url("https://example.com/z", priority=0)
        queue.add_url("https://example.com/a", priority=0)
        queue.add_url("https://example.com/first", priority=-1)
        actual = [await queue.get_next() for _ in range(4)]
        self.assertEqual(actual, ["https://example.com/first", "https://example.com/z",
                                  "https://example.com/a", "https://example.com/later"])

    async def test_duplicates_in_every_state(self):
        for failed in (False, True):
            queue = CrawlerQueue()
            url = "https://example.com/"
            self.assertTrue(queue.add_url("  " + url + "  "))
            self.assertFalse(queue.add_url(url, priority=-10))
            self.assertEqual(await queue.get_next(), url)
            self.assertFalse(queue.add_url(url))
            if failed:
                queue.mark_failed(url, "HTTP 404")
            else:
                queue.mark_processed(url)
            self.assertFalse(queue.add_url(url))
            self.assertIsNone(await queue.get_next())
            self.assertEqual(queue.get_stats()["total"], 1)

    async def test_empty_does_not_mean_finished(self):
        queue = CrawlerQueue()
        self.assertIsNone(await queue.get_next())
        queue.add_url("https://example.com/")
        await queue.get_next()
        self.assertIsNone(await queue.get_next())
        self.assertEqual(queue.get_stats()["active"], 1)
        queue.add_url("https://example.com/new")
        self.assertEqual(await queue.get_next(), "https://example.com/new")

    async def test_stats_and_error(self):
        queue = CrawlerQueue()
        for number in range(3):
            queue.add_url(f"https://example.com/{number}")
        first = await queue.get_next()
        second = await queue.get_next()
        queue.mark_processed(first)
        queue.mark_failed(second, "TimeoutError")
        self.assertEqual(queue.get_stats(), {
            "total": 3, "pending": 1, "active": 0, "processed": 1, "failed": 1,
        })
        self.assertEqual(queue._failed[second], "TimeoutError")
        snapshot = queue.get_stats()
        snapshot["total"] = 999
        self.assertEqual(queue.get_stats()["total"], 3)

    async def test_invalid_state_transitions(self):
        queue = CrawlerQueue()
        url = "https://example.com/"
        with self.assertRaises(ValueError):
            queue.mark_failed(url, "unknown")
        queue.add_url(url)
        with self.assertRaises(ValueError):
            queue.mark_processed(url)
        await queue.get_next()
        queue.mark_processed(url)
        with self.assertRaises(ValueError):
            queue.mark_failed(url, "already finished")
        self.assertEqual(queue.get_stats()["processed"], 1)

    async def test_multiple_consumers(self):
        queue = CrawlerQueue()
        for number in range(10):
            queue.add_url(f"https://example.com/{number}")
        results = await asyncio.gather(*(queue.get_next() for _ in range(15)))
        urls = [url for url in results if url is not None]
        self.assertEqual(len(urls), 10)
        self.assertEqual(len(set(urls)), 10)
        self.assertEqual(results.count(None), 5)

    async def test_invalid_inputs(self):
        queue = CrawlerQueue()
        for url in (None, 123, "", "   "):
            with self.assertRaises(ValueError):
                queue.add_url(url)
        for priority in (True, 1.5, "1"):
            with self.assertRaises(ValueError):
                queue.add_url("https://example.com/", priority)
        self.assertEqual(queue.get_stats()["total"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
