"""День 2: извлечение структурированных данных из HTML."""

import asyncio
import logging
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class HTMLParser:
    def __init__(self, internal_only=False):
        self.internal_only = internal_only

    @staticmethod
    def empty_result(url):
        return {
            "url": url, "title": "", "text": "", "links": [],
            "metadata": {"title": "", "description": "", "keywords": ""},
            "images": [], "headings": [], "tables": [], "lists": [],
            "warnings": [],
        }

    async def parse_html(self, html, url):
        # BeautifulSoup синхронный: переносим разбор в поток, чтобы не блокировать event loop.
        return await asyncio.to_thread(self._parse_html, html, url)

    def _parse_html(self, html, url):
        result = self.empty_result(url)

        def warn(message):
            result["warnings"].append(message)
            logger.warning("Парсинг %s: %s", url, message)

        if not isinstance(html, str):
            warn("Ожидалась строка HTML")
            return result
        if not html.strip():
            warn("Пустой HTML")
            return result
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as exc:
            warn(f"lxml: {type(exc).__name__}: {exc}; пробуем html.parser")
            try:
                soup = BeautifulSoup(html, "html.parser")
            except Exception as fallback_error:
                warn(f"HTML недоступен: {type(fallback_error).__name__}: {fallback_error}")
                return result

        if soup.title is None:
            warn("Отсутствует title; доступные данные будут сохранены")

        # Ошибка одного извлекателя не должна уничтожать остальные результаты.
        extractors = {
            "metadata": lambda: self.extract_metadata(soup),
            "text": lambda: self.extract_text(soup),
            "links": lambda: self.extract_links(soup, url),
            "images": lambda: self.extract_images(soup, url),
            "headings": lambda: self.extract_headings(soup),
            "tables": lambda: self.extract_tables(soup),
            "lists": lambda: self.extract_lists(soup),
        }
        for field, extract in extractors.items():
            try:
                result[field] = extract()
            except Exception as exc:
                warn(f"{field}: {type(exc).__name__}: {exc}")
        result["title"] = result["metadata"]["title"]
        return result

    def _absolute_url(self, value, base_url):
        if not isinstance(value, str) or not value.strip():
            return None
        value = value.strip()
        if any(char.isspace() or ord(char) < 32 for char in value) or "\\" in value:
            return None
        try:
            raw = urlsplit(value)
            if raw.scheme and (raw.scheme not in ("http", "https") or not raw.hostname):
                return None
            absolute = urljoin(base_url, value)
            parts = urlsplit(absolute)
            if parts.scheme not in ("http", "https") or not parts.hostname:
                return None
            if parts.username is not None or parts.password is not None:
                return None
            # Обращение к port проверяет формат и диапазон порта.
            parts.port
            if any(char.isspace() for char in parts.netloc):
                return None
            return absolute
        except (ValueError, UnicodeError):
            return None

    def _base_url(self, soup, url):
        base = soup.find("base", href=True)
        return (self._absolute_url(base["href"], url) if base else None) or url

    def extract_links(self, soup, base_url):
        effective_base = self._base_url(soup, base_url)
        links = []
        seen = set()
        for tag in soup.find_all("a", href=True):
            absolute = self._absolute_url(tag["href"], effective_base)
            if absolute is None or absolute in seen:
                continue
            if self.internal_only and urlsplit(absolute).hostname != urlsplit(base_url).hostname:
                continue
            seen.add(absolute)
            links.append(absolute)
        return links

    def extract_text(self, soup, selector=None):
        roots = soup.select(selector) if selector is not None else [
            soup.find("main") or soup.find("article") or soup.body or soup
        ]
        chunks = []
        for root in roots:
            # Копия: удаление скриптов не меняет исходный soup для других методов.
            clean = BeautifulSoup(str(root), "lxml")
            for tag in clean.select("script, style, noscript, template, head"):
                tag.decompose()
            chunks.append(clean.get_text(" ", strip=True))
        return " ".join(" ".join(chunks).split())

    def extract_metadata(self, soup):
        metadata = {
            "title": soup.title.get_text(" ", strip=True) if soup.title else "",
            "description": "", "keywords": "",
        }
        for tag in soup.find_all("meta"):
            name = str(tag.get("name", "")).lower()
            if name in ("description", "keywords") and not metadata[name]:
                metadata[name] = str(tag.get("content", "")).strip()
        return metadata

    def extract_images(self, soup, base_url):
        base_url = self._base_url(soup, base_url)
        images = []
        for tag in soup.find_all("img"):
            src = self._absolute_url(tag.get("src"), base_url)
            if src:
                images.append({"src": src, "alt": tag.get("alt", "")})
        return images

    def extract_headings(self, soup):
        return [{"level": int(tag.name[1]), "text": tag.get_text(" ", strip=True)}
                for tag in soup.find_all(["h1", "h2", "h3"])]

    def extract_tables(self, soup):
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for row in table.find_all("tr"):
                if row.find_parent("table") is table:
                    rows.append([cell.get_text(" ", strip=True)
                                 for cell in row.find_all(["th", "td"], recursive=False)])
            tables.append(rows)
        return tables

    def extract_lists(self, soup):
        lists = []
        for tag in soup.find_all(["ul", "ol"]):
            items = []
            for item in tag.find_all("li", recursive=False):
                # Вложенные списки выдаём отдельными записями без повторения их текста.
                copy = BeautifulSoup(str(item), "lxml")
                for nested in copy.find_all(["ul", "ol"]):
                    nested.decompose()
                items.append(copy.get_text(" ", strip=True))
            lists.append({"type": tag.name, "items": items})
        return lists
