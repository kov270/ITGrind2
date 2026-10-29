"""День 3: обход сайта с прогрессом и сохранением JSON."""

import argparse
import asyncio
import json
import logging

import aiofiles
from aiohttp import web

from async_crawler import AsyncCrawler


async def main(args):
    runner = None
    try:
        urls = args.urls
        if args.local:
            async def page(request):
                name = request.match_info["name"]
                await asyncio.sleep(0.1)
                links = {"start": ["a", "b", "missing"], "a": ["c", "start"],
                         "b": ["c"], "c": []}
                if name not in links:
                    return web.Response(status=404)
                anchors = "".join(f'<a href="/{link}">{link}</a>' for link in links[name])
                return web.Response(text=f"<title>{name}</title><main><h1>{name}</h1>{anchors}</main>",
                                    content_type="text/html")

            app = web.Application()
            app.router.add_get("/{name}", page)
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            urls = [f"http://127.0.0.1:{runner.addresses[0][1]}/start"]

        def progress(stats):
            print(f"Обработано: {stats['processed']} | В очереди: {stats['queued']} | "
                  f"Ошибок: {stats['failed']} | Скорость: {stats['pages_per_second']:.2f} стр/с",
                  flush=True)

        async with AsyncCrawler(max_concurrent=args.concurrent, max_depth=args.depth,
                                per_domain_limit=args.per_domain) as crawler:
            pages = await crawler.crawl(
                urls, max_pages=args.max_pages, same_domain_only=True,
                exclude_patterns=args.exclude, include_patterns=args.include,
                progress_callback=progress,
            )
            output = {
                "pages": pages, "failed_urls": crawler.failed_urls,
                "visited_urls": sorted(crawler.visited_urls),
                "url_depths": crawler.url_depths, "stats": crawler.crawl_stats,
            }
            async with aiofiles.open(args.output, "w", encoding="utf-8") as file:
                await file.write(json.dumps(output, ensure_ascii=False, indent=2))
            print(f"Результаты сохранены: {args.output}")
    finally:
        if runner is not None:
            await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("urls", nargs="*", help="Стартовые HTTP/HTTPS адреса")
    parser.add_argument("--local", action="store_true", help="Обойти локальный учебный сайт")
    parser.add_argument("--depth", type=int, default=2)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--concurrent", type=int, default=5)
    parser.add_argument("--per-domain", type=int, default=2)
    parser.add_argument("--include", action="append", default=[], help="Регулярное выражение URL")
    parser.add_argument("--exclude", action="append", default=[], help="Регулярное выражение URL")
    parser.add_argument("--output", default="results_day3.json")
    args = parser.parse_args()
    if not args.local and not args.urls:
        parser.error("Укажите стартовый URL или --local")
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    asyncio.run(main(args))
