# ITGrind2
week 2

## День 3 — обход сайта

`await crawler.crawl(start_urls, max_pages=100)` берёт URL из
`CrawlerQueue`, загружает и разбирает страницу, сохраняет результат и добавляет
найденные ссылки. `max_pages` считает все попытки, включая HTTP-ошибки. Повторные
адреса не добавляются. Каждый вызов `crawl` начинает обход заново; состояние
доступно в `visited_urls`, `processed_urls`, `failed_urls` и `queue.get_stats()`.

```python
async with AsyncCrawler() as crawler:
    pages = await crawler.crawl(["https://example.com"], max_pages=5)
    print(crawler.queue.get_stats())
```

`max_depth` задаётся при создании клиента: стартовые URL имеют глубину 0,
их ссылки — 1 и так далее. При `max_depth=0` обрабатываются только стартовые URL.
`None` по умолчанию означает отсутствие ограничения; глубины добавленных URL
доступны в `crawler.url_depths`. `max_concurrent` ограничивает все одновременные
загрузки, `per_domain_limit` (по умолчанию 2) — загрузки одного hostname.
`SemaphoreManager` предоставляет статистику активных задач через
`crawler.semaphore_manager.stats()`.

`same_domain_only=True` разрешает ссылки только на hostname стартовых URL.
`include_patterns` и `exclude_patterns` — списки регулярных выражений для
полного URL; приоритет у исключения. Стартовые URL проходят те же фильтры.
`max_pages` считает начатые попытки, включая ошибки. `progress_callback`
вызывается при старте и после каждой страницы и получает `processed`, `queued`,
`failed`, `active`, `pages_per_second`; снимок также есть в `crawl_stats`.
Для внешних сайтов выбирайте небольшой `max_pages`.

```powershell
.\.venv\Scripts\python.exe demo_day3.py --local
.\.venv\Scripts\python.exe demo_day3.py https://example.com --depth 1 --max-pages 5
```

Демо выводит прогресс в реальном времени и сохраняет полные данные в
`results_day3.json`. Локальный режим работает без Интернета.

## День 2 — Парсинг HTML

Реализация продолжает проект первого дня в той же папке.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe demo_day2.py --local --output results.json
.\.venv\Scripts\python.exe demo_day2.py
.\.venv\Scripts\python.exe -m unittest -v
```

`html_parser.py` содержит `HTMLParser`. `await crawler.fetch_and_parse(url)`
возвращает `url`, `title`, `text`, `links`, `metadata`, `images`, `headings`,
`tables`, `lists`, `warnings`. При ошибке загрузки возвращается та же схема
с пустыми данными и дополнительным `error`. `url` остаётся исходным адресом,
а относительные ссылки разрешаются относительно конечного адреса после редиректов
и первого тега `<base href>`, если он задан.

```python
async with AsyncCrawler() as crawler:
    page = await crawler.fetch_and_parse("https://example.com")
    print(page["title"])
    print(page["links"])
```

Методы `extract_links`, `extract_text`, `extract_metadata`, `extract_images`,
`extract_headings`, `extract_tables`, `extract_lists` можно использовать отдельно
с объектом BeautifulSoup. `extract_text(soup, selector=".content")` поддерживает
CSS-селектор. Без селектора выбирается первый `main`, затем `article`, затем
`body`; скрипты и стили исключаются. Это простая эвристика, а не универсальный
алгоритм определения статьи. JavaScript не исполняется.

Ссылки уникальны, сохраняют порядок, query и фрагменты. Допускаются HTTP/HTTPS
с непустым хостом и корректным портом, без пробелов и встроенных логина/пароля;
`mailto`, `javascript`, `data` пропускаются. Это проверка формы URL, не доступности.
`HTMLParser(internal_only=True)` оставляет ссылки с тем же hostname (поддомены
считаются внешними). Для клиента: `crawler.parser = HTMLParser(internal_only=True)`.

Изображения — записи `src` (абсолютный HTTP/HTTPS URL) и `alt` (пустой при отсутствии).
Изображения без корректного `src`, включая data URI и lazy-loading только в data-src,
пропускаются. Заголовки — записи `level` и `text`. Таблицы — списки строк ячеек
в порядке документа; rowspan/colspan не разворачиваются. Каждый ul/ol — запись
`type` и `items`; вложенные списки выдаются отдельно.

BeautifulSoup/lxml восстанавливает многие ошибки разметки автоматически, поэтому
не каждый незакрытый тег вызывает предупреждение. Пустой ввод, отсутствие title,
сбой разбора или извлекателя логируются и попадают в `warnings`. При сбое lxml
пробуем встроенный html.parser; при сбое отдельного извлекателя сохраняем остальные
поля. Синхронный разбор запускается через `asyncio.to_thread`.

Демонстрация печатает статистику и первые пять ссылок/заголовков; `--output`
сохраняет все данные в JSON через aiofiles. Локальный режим показывает корректную
страницу, повреждённый HTML и HTTP 404. Тесты не требуют Интернета.

Документация: [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/),
[urllib.parse](https://docs.python.org/3/library/urllib.parse.html).

## День 1 — Асинхронный HTTP-клиент

Python 3.10+. Все команды выполняются из этой папки в PowerShell.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe demo.py
.\.venv\Scripts\python.exe demo.py --local
.\.venv\Scripts\python.exe -m unittest -v
```

`demo.py` загружает шесть различных внешних URL последовательно и параллельно,
выводит HTTP-статусы, ошибки, количество символов, общее время и отношение времён.
Один URL намеренно возвращает 404. Скорость внешних сайтов непостоянна;
`--local` запускает временный локальный сервер с шестью URL и задержками 0.3 с.

## Использование

```python
import asyncio
from async_crawler import AsyncCrawler

async def main():
    async with AsyncCrawler(max_concurrent=5) as crawler:
        results = await crawler.fetch_urls([
            "https://example.com",
            "https://httpbin.org/delay/1",
            "https://httpbin.org/delay/2",
        ])
        print(f"Загружено {len(results)} страниц")
        print(crawler.errors)

asyncio.run(main())
```

При ручном создании клиента вызывайте `await crawler.close()` в `finally`.
Сначала дождитесь запросов, затем закрывайте клиент. После закрытия нужен новый экземпляр.

## Поведение

- Одна лениво создаваемая `ClientSession` повторно использует соединения.
- `Semaphore` ограничивает выполняющиеся загрузки, `TCPConnector` — число соединений.
- Таймауты по умолчанию: подключение 5 с, ожидание очередной порции данных 10 с,
  весь HTTP-запрос 30 с. Ожидание семафора в этот срок не входит.
- `fetch_url` возвращает текст либо пустую строку при сетевой/HTTP-ошибке или таймауте.
  Для различения ошибки и корректного пустого ответа проверяйте `url in crawler.errors`.
- `fetch_urls` возвращает только успешные ответы, включая пустые; дубликаты URL
  загружаются один раз. `errors` и `statuses` содержат последнее состояние URL.
  При отсутствии HTTP-ответа статус отсутствует. Не запускайте перекрывающиеся
  операции над одним URL на одном экземпляре: они разделяют эти словари.
- HTTP 4xx/5xx проверяются через `raise_for_status`. Тип исключения и URL попадают в лог.
- Логирование настраивает вызывающий скрипт; пример настройки есть в `demo.py`.
- `aiofiles` установлен по заданию; для загрузки в память он пока не используется.

Тесты работают на локальном HTTP-сервере: успешные и пустые ответы, 404/500,
невалидный URL, отказ соединения, таймаут чтения/общий таймаут, повторное использование
соединения, закрытие сессии, ограничение конкурентности и сравнение скорости.

Справочник: [официальная документация aiohttp](https://docs.aiohttp.org/en/stable/client_quickstart.html).
