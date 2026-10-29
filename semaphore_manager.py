import asyncio
from collections import defaultdict


class SemaphoreManager:
    """
    Управляет двумя уровнями ограничения конкурентности:
      - глобальный лимит (не более N задач одновременно во всей системе)
      - лимит на ключ/домен (не более M задач к одному домену)

    Плюс ведёт статистику активных задач: всего и по каждому ключу.
    """

    def __init__(self, global_limit: int, per_key_limit: int):
        for limit in (global_limit, per_key_limit):
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
                raise ValueError("Лимиты должны быть положительными целыми числами")

        self._global_limit = global_limit
        self._per_key_limit = per_key_limit

        # Два уровня семафоров.
        self._global_sem = asyncio.Semaphore(global_limit)
        self._key_sems: dict[str, asyncio.Semaphore] = {}

        # Статистика.
        self._active_by_key: dict[str, int] = defaultdict(int)
        self._total_active = 0
        self._peak_active = 0

    # ---------- внутреннее ----------

    def _get_key_sem(self, key: str) -> asyncio.Semaphore:
        # В одном event loop между get и созданием нет await —
        # гонки нет, lock не нужен.
        sem = self._key_sems.get(key)
        if sem is None:
            sem = asyncio.Semaphore(self._per_key_limit)
            self._key_sems[key] = sem
        return sem

    async def _acquire(self, key: str) -> None:
        # Ожидание занятого домена не должно занимать глобальное место.
        key_sem = self._get_key_sem(key)
        await key_sem.acquire()
        try:
            await self._global_sem.acquire()
        except BaseException:
            # В том числе при отмене ожидания возвращаем доменное место.
            key_sem.release()
            raise

        # Оба места заняты — обновляем статистику.
        self._total_active += 1
        self._active_by_key[key] += 1
        if self._total_active > self._peak_active:
            self._peak_active = self._total_active

    def _release(self, key: str) -> None:
        # Сначала статистика, потом семафоры — порядок не критичен,
        # но так «активных» уменьшается до пробуждения ожидающих.
        self._total_active -= 1
        self._active_by_key[key] -= 1
        if self._active_by_key[key] == 0:
            del self._active_by_key[key]

        self._get_key_sem(key).release()
        self._global_sem.release()

    # ---------- публичный API ----------

    def limit(self, key: str) -> "_Limit":
        """async with manager.limit(domain): ..."""
        return _Limit(self, key)

    # ---------- статистика ----------

    @property
    def total_active(self) -> int:
        return self._total_active

    @property
    def peak_active(self) -> int:
        return self._peak_active

    def active_by_key(self) -> dict[str, int]:
        return dict(self._active_by_key)

    def stats(self) -> dict:
        return {
            "total_active": self._total_active,
            "peak_active": self._peak_active,
            "active_by_key": dict(self._active_by_key),
            "global_limit": self._global_limit,
            "per_key_limit": self._per_key_limit,
        }


class _Limit:
    """Хелпер для `async with manager.limit(key):`."""

    __slots__ = ("_manager", "_key")

    def __init__(self, manager: SemaphoreManager, key: str):
        self._manager = manager
        self._key = key

    async def __aenter__(self) -> "_Limit":
        await self._manager._acquire(self._key)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._manager._release(self._key)
