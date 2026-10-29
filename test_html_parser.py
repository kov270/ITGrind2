import unittest
from unittest.mock import patch

from aiohttp import web
from bs4 import BeautifulSoup

from async_crawler import AsyncCrawler
from demo_day2 import SAMPLE_HTML
from html_parser import HTMLParser


class ParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_document(self):
        data = await HTMLParser().parse_html(SAMPLE_HTML, "https://example.org/pages/one")
        self.assertEqual(data["title"], "Учебная страница")
        self.assertEqual(data["metadata"]["description"], "Демонстрация парсинга")
        self.assertEqual(data["metadata"]["keywords"], "python, html")
        self.assertIn("Привет, мир!", data["text"])
        self.assertNotIn("secret_script", data["text"])
        self.assertEqual(data["links"], ["https://example.org/next", "https://example.com"])
        self.assertEqual(data["images"], [{"src": "https://example.org/image.png", "alt": "Пример изображения"}])
        self.assertEqual([h["level"] for h in data["headings"]], [1, 2, 3])
        self.assertEqual(data["tables"], [[["Язык", "Год"], ["Python", "1991"]]])
        self.assertEqual(data["lists"], [{"type": "ul", "items": ["Загрузить", "Разобрать"]},
                                          {"type": "ol", "items": ["Проверить"]}])

    async def test_links_validation_base_and_filter(self):
        soup = BeautifulSoup('''<base href="/docs/"><a href="one">1</a><a href="one">dup</a>
            <a href="../two">2</a><a href="//other.org/path">3</a><a href="#part">4</a>
            <a href="?x=1">5</a><a href="mailto:a@b.com">mail</a><a href="javascript:alert(1)">js</a>
            <a href="https://">bad</a><a href="http://x:99999">port</a><a href="http://[bad">bad</a>
            <a href="http://bad host/">space</a><a href="">empty</a><a>missing</a>''', "lxml")
        links = HTMLParser().extract_links(soup, "https://example.org/page")
        self.assertEqual(links, ["https://example.org/docs/one", "https://example.org/two",
                                "https://other.org/path", "https://example.org/docs/#part",
                                "https://example.org/docs/?x=1"])
        self.assertNotIn("https://other.org/path", HTMLParser(True).extract_links(soup, "https://example.org/page"))

    async def test_broken_and_empty_html(self):
        with self.assertLogs("html_parser", level="WARNING"):
            data = await HTMLParser().parse_html("<h1>Привет<p>Текст<a href='/a'>A", "https://example.org")
        self.assertIn("Привет", data["text"])
        self.assertEqual(data["links"], ["https://example.org/a"])
        for html in ("", None):
            with self.assertLogs("html_parser", level="WARNING"):
                data = await HTMLParser().parse_html(html, "https://example.org")
            self.assertEqual(data["links"], [])
            self.assertTrue(data["warnings"])

    async def test_selector_and_non_mutation(self):
        soup = BeautifulSoup('<title>Title</title><p class="x">One <b>two</b></p><p>Three</p>', "lxml")
        before = str(soup)
        parser = HTMLParser()
        self.assertEqual(parser.extract_text(soup, ".x"), "One two")
        self.assertEqual(parser.extract_text(soup, ".missing"), "")
        self.assertEqual(str(soup), before)

    async def test_partial_results(self):
        parser = HTMLParser()
        with patch.object(parser, "extract_tables", side_effect=ValueError("broken table")):
            with self.assertLogs("html_parser", level="WARNING"):
                data = await parser.parse_html(SAMPLE_HTML, "https://example.org")
        self.assertEqual(data["title"], "Учебная страница")
        self.assertTrue(data["links"])
        self.assertEqual(data["tables"], [])
        self.assertIn("tables", data["warnings"][0])

    async def test_fallback(self):
        real_soup = BeautifulSoup
        def build(html, backend):
            if backend == "lxml":
                raise ValueError("parser failure")
            return real_soup(html, backend)
        with patch("html_parser.BeautifulSoup", side_effect=build):
            with self.assertLogs("html_parser", level="WARNING"):
                result = await HTMLParser().parse_html("<title>Recovered</title><a href='/x'>X</a>", "https://example.org")
        self.assertEqual(result["title"], "Recovered")
        self.assertEqual(result["links"], ["https://example.org/x"])

    async def test_nested_lists_and_missing_alt(self):
        soup = BeautifulSoup('<ul><li>A<ul><li>B</li></ul></li></ul><img src="/x">', "lxml")
        parser = HTMLParser()
        self.assertEqual(parser.extract_lists(soup), [{"type": "ul", "items": ["A"]}, {"type": "ul", "items": ["B"]}])
        self.assertEqual(parser.extract_images(soup, "https://example.org")[0]["alt"], "")


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_parse_redirect_and_failure(self):
        async def redirect(request):
            raise web.HTTPFound("/folder/page")
        async def page(request):
            return web.Response(text='<title>Test</title><a href="next">Next</a>', content_type="text/html")
        app = web.Application()
        app.router.add_get("/redirect", redirect)
        app.router.add_get("/folder/page", page)
        runner = web.AppRunner(app)
        await runner.setup()
        try:
            await web.TCPSite(runner, "127.0.0.1", 0).start()
            base = f"http://127.0.0.1:{runner.addresses[0][1]}"
            async with AsyncCrawler() as crawler:
                result = await crawler.fetch_and_parse(base + "/redirect")
                self.assertEqual(result["url"], base + "/redirect")
                self.assertEqual(result["links"], [base + "/folder/next"])
                self.assertEqual(result["title"], "Test")
                failed = await crawler.fetch_and_parse(base + "/missing")
                self.assertIn("error", failed)
                self.assertEqual(failed["text"], "")
        finally:
            await runner.cleanup()


if __name__ == "__main__":
    unittest.main(verbosity=2)
