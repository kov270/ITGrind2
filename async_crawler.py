"""Базовый асинхронный HTTP-клиент (Python 3.10+)."""

import asyncio
import logging
import re
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp
from html_parser import HTMLParser
from crawler_queue import CrawlerQueue
from semaphore_manager import SemaphoreManager

logger = logging.getLogger(__name__)


class AsyncCrawler:
    """Используйте экземпляр в одном event loop; close завершает его работу."""

    def __init__(
        self,
        max_concurrent: int = 10,
        *,
        connect_timeout: float = 5,
        read_timeout: float = 10,
        total_timeout: float = 30,
        max_depth: int | None = None,
        per_domain_limit: int = 2,
    ):
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent < 1:
            raise ValueError("max_concurrent должен быть положительным целым числом")
        if min(connect_timeout, read_timeout, total_timeout) <= 0:
            raise ValueError("Таймауты должны быть положительными")
        if max_depth is not None and (isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0):
            raise ValueError("max_depth должен быть неотрицательным целым числом или None")
        if isinstance(per_domain_limit, bool) or not isinstance(per_domain_limit, int) or per_domain_limit < 1:
            raise ValueError("per_domain_limit должен быть положительным целым числом")
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.per_domain_limit = per_domain_limit
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
            sock_connect=connect_timeout,
            sock_read=read_timeout,
        )
        self._session: aiohttp.ClientSession | None = None
        self._closed = False
        self.errors: dict[str, str] = {}
        self.statuses: dict[str, int] = {}
        self.final_urls = {}
        self.parser = HTMLParser()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.queue = None
        self.url_depths = {}
        self.semaphore_manager = None
        self.crawl_stats = {}

    def _get_session(self):
        if self._closed:
            raise RuntimeError("AsyncCrawler уже закрыт")
        if self._session is None:
            # Создаём сессию лениво, внутри работающего event loop.
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                connector=aiohttp.TCPConnector(limit=self.max_concurrent),
            )
        return self._session

    async def fetch_url(self, url):
        """Вернуть текст или '' при ошибке; детали доступны в errors/statuses."""
        async with self._semaphore:
            session = self._get_session()
            self.errors.pop(url, None)
            self.statuses.pop(url, None)
            self.final_urls.pop(url, None)
            logger.info("Начало загрузки: %s", url)
            try:
                async with session.get(url) as response:
                    self.statuses[url] = response.status
                    self.final_urls[url] = str(response.url)
                    response.raise_for_status()
                    body = await response.text(errors="replace")
                    logger.info("Успешно: %s | HTTP %s | %s символов", url, response.status, len(body))
                    return body
            except aiohttp.ClientResponseError as exc:
                self._record_error(url, exc)
            except asyncio.TimeoutError as exc:
                self._record_error(url, exc)
            except aiohttp.ClientError as exc:
                self._record_error(url, exc)
            return ""

    def _record_error(self, url: str, exc: Exception):
        detail = f"{type(exc).__name__}: {str(exc) or 'истёк таймаут'}"
        self.errors[url] = detail
        logger.warning("Ошибка: %s | %s", url, detail)

    async def fetch_urls(self, urls: list[str]):
        """Загрузить уникальные URL; исключить ошибки, сохранить пустые успешные ответы."""
        unique_urls = list(dict.fromkeys(urls))
        bodies = await asyncio.gather(*(self.fetch_url(url) for url in unique_urls))
        return {url: body for url, body in zip(unique_urls, bodies) if url not in self.errors}

    async def fetch_and_parse(self, url):
        """Загрузить HTML и извлечь данные; при ошибке сохранить схему и поле error."""
        html = await self.fetch_url(url)
        if url in self.errors:
            result = self.parser.empty_result(url)
            result["error"] = self.errors[url]
            return result
        result = await self.parser.parse_html(html, self.final_urls.get(url, url))
        result["url"] = url
        return result

    async def crawl(self, start_urls: list[str], max_pages: int = 100,
                    same_domain_only=False, exclude_patterns=None,
                    include_patterns=None, progress_callback=None):
        """Обойти страницы совместно; max_pages считает попытки, включая ошибки."""
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 1:
            raise ValueError("max_pages должен быть положительным целым числом")
        if self._closed:
            raise RuntimeError("AsyncCrawler уже закрыт")
        if not isinstance(start_urls, list):
            raise ValueError("start_urls должен быть списком URL")
        excluded = [re.compile(pattern) for pattern in (exclude_patterns or [])]
        included = [re.compile(pattern) for pattern in (include_patterns or [])]
        roots = set()
        for url in start_urls:
            if self.parser._absolute_url(url, url) != url:
                raise ValueError(f"Некорректный стартовый URL: {url}")
            roots.add(urlsplit(url).hostname)

        def allowed(url):
            if self.parser._absolute_url(url, url) != url:
                return False
            if same_domain_only and urlsplit(url).hostname not in roots:
                return False
            if any(pattern.search(url) for pattern in excluded):
                return False
            if included and not any(pattern.search(url) for pattern in included):
                return False
            return True

        self.queue = CrawlerQueue()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.url_depths = {}
        self.semaphore_manager = SemaphoreManager(self.max_concurrent, self.per_domain_limit)
        started = perf_counter()

        def report():
            done = len(self.processed_urls) + len(self.failed_urls)
            stats = self.queue.get_stats()
            elapsed = max(perf_counter() - started, 1e-9)
            self.crawl_stats = {
                "processed": len(self.processed_urls), "queued": stats["pending"],
                "failed": len(self.failed_urls), "active": stats["active"],
                "pages_per_second": done / elapsed,
            }
            logger.info("Обход: обработано=%s в очереди=%s ошибок=%s скорость=%.2f стр/с",
                        self.crawl_stats["processed"], self.crawl_stats["queued"],
                        self.crawl_stats["failed"], self.crawl_stats["pages_per_second"])
            if progress_callback is not None:
                progress_callback(self.crawl_stats.copy())

        for url in start_urls:
            if allowed(url) and self.queue.add_url(url):
                self.url_depths[url.strip()] = 0
        report()

        async def process(url):
            try:
                async with self.semaphore_manager.limit(urlsplit(url).hostname):
                    page = await self.fetch_and_parse(url)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                self.failed_urls[url] = error
                self.queue.mark_failed(url, error)
                logger.warning("Обход %s: %s", url, error)
                report()
                return
            if "error" in page:
                self.failed_urls[url] = page["error"]
                self.queue.mark_failed(url, page["error"])
                report()
                return
            self.processed_urls[url] = page
            self.queue.mark_processed(url)
            next_depth = self.url_depths[url] + 1
            if self.max_depth is None or next_depth <= self.max_depth:
                for link in page["links"]:
                    if allowed(link) and self.queue.add_url(link, priority=next_depth):
                        self.url_depths[link] = next_depth
            report()

        running = set()
        try:
            while True:
                while len(running) < self.max_concurrent and len(self.visited_urls) < max_pages:
                    url = await self.queue.get_next()
                    if url is None:
                        break
                    self.visited_urls.add(url)
                    running.add(asyncio.create_task(process(url)))
                if not running:
                    break
                completed, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
                for task in completed:
                    task.result()
        finally:
            if running:
                for task in running:
                    task.cancel()
                await asyncio.gather(*running, return_exceptions=True)

        return self.processed_urls.copy()

    async def close(self):
        """Закрыть сессию и пул; повторный вызов безопасен. Дождитесь запросов перед close."""
        self._closed = True
        if self._session is not None:
            await self._session.close()

    async def __aenter__(self):
        self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()
