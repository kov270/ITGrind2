"""Базовый асинхронный HTTP-клиент (Python 3.10+)."""

import asyncio
import logging

import aiohttp

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
    ):
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int) or max_concurrent < 1:
            raise ValueError("max_concurrent должен быть положительным целым числом")
        if min(connect_timeout, read_timeout, total_timeout) <= 0:
            raise ValueError("Таймауты должны быть положительными")
        self.max_concurrent = max_concurrent
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
            logger.info("Начало загрузки: %s", url)
            try:
                async with session.get(url) as response:
                    self.statuses[url] = response.status
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
