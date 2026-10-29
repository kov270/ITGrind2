"""День 2: python demo_day2.py [--local] [--output results.json]."""

import argparse
import asyncio
import json
import logging

import aiofiles
from aiohttp import web

from async_crawler import AsyncCrawler

SAMPLE_HTML = """<!doctype html><html><head><title>Учебная страница</title>
<meta name="description" content="Демонстрация парсинга">
<meta name="keywords" content="python, html"></head><body><main>
<h1>День 2</h1><h2>Данные</h2><h3>Пример</h3><p>Привет, мир!</p>
<a href="../next">Следующая</a><a href="https://example.com">Внешняя</a>
<img src="/image.png" alt="Пример изображения">
<table><tr><th>Язык</th><th>Год</th></tr><tr><td>Python</td><td>1991</td></tr></table>
<ul><li>Загрузить</li><li>Разобрать</li></ul><ol><li>Проверить</li></ol>
<script>secret_script()</script></main></body></html>"""


async def main(local=False, output=None):
    runner = None
    try:
        urls = ["https://example.com", "https://www.python.org",
                "https://docs.aiohttp.org/en/stable/"]
        if local:
            async def page(request):
                if request.match_info["name"] == "broken":
                    return web.Response(text="<h1>Битая страница<p>Текст<a href='/next'>Ссылка",
                                        content_type="text/html")
                return web.Response(text=SAMPLE_HTML, content_type="text/html")

            app = web.Application()
            app.router.add_get("/pages/{name}", page)
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            base = f"http://127.0.0.1:{runner.addresses[0][1]}"
            urls = [base + "/pages/good", base + "/pages/broken", base + "/missing"]

        async with AsyncCrawler(max_concurrent=3) as crawler:
            results = await asyncio.gather(*(crawler.fetch_and_parse(url) for url in urls))
        for result in results:
            summary = {
                "url": result["url"], "title": result["title"],
                "text_length": len(result["text"]), "links_count": len(result["links"]),
                "links": result["links"][:5], "images_count": len(result["images"]),
                "headings": result["headings"][:5], "tables_count": len(result["tables"]),
                "lists_count": len(result["lists"]), "metadata": result["metadata"],
                "warnings": result["warnings"],
            }
            if "error" in result:
                summary["error"] = result["error"]
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        if output:
            async with aiofiles.open(output, "w", encoding="utf-8") as file:
                await file.write(json.dumps(results, ensure_ascii=False, indent=2))
            print(f"Полные результаты сохранены: {output}")
    finally:
        if runner is not None:
            await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--output", help="Путь для сохранения полных результатов JSON")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    asyncio.run(main(args.local, args.output))
