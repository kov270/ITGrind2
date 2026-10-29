import asyncio
import unittest

from semaphore_manager import SemaphoreManager


class SemaphoreManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_limits_must_be_positive_integers(self):
        for invalid in (0, -1, 1.5, True, False, "2", None):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    SemaphoreManager(invalid, 2)
                with self.assertRaises(ValueError):
                    SemaphoreManager(2, invalid)
        async with SemaphoreManager(1, 1).limit("example.com"):
            pass

    async def test_busy_domain_does_not_block_other_domain(self):
        manager = SemaphoreManager(2, 1)
        release = asyncio.Event()
        a_started = asyncio.Event()
        b_started = asyncio.Event()

        async def work(key, started):
            async with manager.limit(key):
                started.set()
                await release.wait()

        tasks = [asyncio.create_task(work("a", a_started))]
        try:
            await asyncio.wait_for(a_started.wait(), 1)
            tasks.append(asyncio.create_task(work("a", asyncio.Event())))
            await asyncio.sleep(0)
            tasks.append(asyncio.create_task(work("b", b_started)))
            await asyncio.wait_for(b_started.wait(), 1)
            self.assertEqual(manager.active_by_key(), {"a": 1, "b": 1})
            self.assertEqual(manager.total_active, 2)
        finally:
            release.set()
            await asyncio.gather(*tasks)
        self.assertEqual(manager.total_active, 0)
        self.assertEqual(manager.peak_active, 2)

    async def test_cancel_while_waiting_global_releases_domain(self):
        manager = SemaphoreManager(1, 1)
        async with manager.limit("a"):
            waiting = asyncio.create_task(manager._acquire("b"))
            await asyncio.sleep(0)
            self.assertFalse(waiting.done())
            waiting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiting
            self.assertEqual(manager.active_by_key(), {"a": 1})
        await asyncio.wait_for(manager._acquire("b"), 1)
        manager._release("b")
        self.assertEqual(manager.total_active, 0)

    async def test_cancel_while_waiting_domain(self):
        manager = SemaphoreManager(2, 1)
        async with manager.limit("a"):
            waiting = asyncio.create_task(manager._acquire("a"))
            await asyncio.sleep(0)
            waiting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiting
            await asyncio.wait_for(manager._acquire("b"), 1)
            manager._release("b")
            self.assertEqual(manager.total_active, 1)
        self.assertEqual(manager.total_active, 0)

    async def test_body_exception_releases_both_limits(self):
        manager = SemaphoreManager(1, 1)
        with self.assertRaises(ValueError):
            async with manager.limit("a"):
                raise ValueError("test")
        self.assertEqual(manager.active_by_key(), {})
        await asyncio.wait_for(manager._acquire("a"), 1)
        manager._release("a")
        self.assertEqual(manager.total_active, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
