"""Очередь URL для одного обхода в одном event loop."""

import heapq
from itertools import count


class CrawlerQueue:
    def __init__(self):
        self._pending = []
        self._counter = count()
        self._seen = set()
        self._active = set()
        self._processed = set()
        self._failed = {}

    def add_url(self, url, priority=0):
        """Добавить новый URL. Меньше priority — раньше выдача. Вернуть True/False."""
        if not isinstance(url, str) or not url.strip():
            raise ValueError("URL должен быть непустой строкой")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("Приоритет должен быть целым числом")
        url = url.strip()
        if url in self._seen:
            return False
        # Номер добавления сохраняет порядок при одинаковом приоритете.
        heapq.heappush(self._pending, (priority, next(self._counter), url))
        self._seen.add(url)
        return True

    async def get_next(self):
        """Выдать URL сразу или None, если очередь сейчас пуста; не ждать пополнения."""
        if not self._pending:
            return None
        priority, number, url = heapq.heappop(self._pending)
        self._active.add(url)
        return url

    def mark_processed(self, url):
        """Завершить ранее выданный URL успешно."""
        self._require_active(url)
        self._active.remove(url)
        self._processed.add(url)

    def mark_failed(self, url, error):
        """Завершить ранее выданный URL с ошибкой."""
        self._require_active(url)
        self._active.remove(url)
        self._failed[url] = str(error)

    def _require_active(self, url):
        if url not in self._active:
            raise ValueError(f"URL не находится в работе: {url}")

    def get_stats(self):
        """Снимок состояния. total = pending + active + processed + failed."""
        return {
            "total": len(self._seen),
            "pending": len(self._pending),
            "active": len(self._active),
            "processed": len(self._processed),
            "failed": len(self._failed),
        }
