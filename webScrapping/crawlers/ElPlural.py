# webScrapping/crawlers/ElPlural.py
import re
import time
import uuid
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from crawlers.Crawler import Crawler


class ElPlural(Crawler):
    ARTICLE_RE = re.compile(r"_\d{6,}$")  # ..._380459102

    def __init__(self, url: str):
        super().__init__(url)
        self.newspaper = "ElPlural"

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0 Safari/537.36"
            ),
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        })

    def _soup(self, url: str, timeout: int = 20) -> BeautifulSoup | None:
        try:
            r = self.session.get(url, timeout=timeout)
            if r.status_code != 200:
                return None
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException:
            return None

    def _is_article(self, href: str) -> bool:
        if not href:
            return False
        href = href.strip()
        if href.startswith("#") or href.lower().startswith("javascript:"):
            return False

        abs_url = urljoin(self.url, href)
        p = urlparse(abs_url)

        # si viene absoluto, asegúrate de que sea el dominio
        if p.netloc and "elplural.com" not in p.netloc.lower():
            return False

        path = p.path or ""
        if not path or path == "/":
            return False

        # corta 4 cosas típicas y ya
        low = path.lower()
        if any(x in low for x in ("/registro", "/buscador", "/alta-newsletter", "/tus-datos")):
            return False

        return bool(self.ARTICLE_RE.search(path))

    def _home_links(self, soup: BeautifulSoup) -> list[str]:
        urls, seen = [], set()
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if not self._is_article(href):
                continue
            u = urljoin(self.url, href)
            if u not in seen:
                seen.add(u)
                urls.append(u)
        return urls

    def _title(self, soup: BeautifulSoup) -> str:
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t:
                return t

        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            return og["content"].strip()

        t = soup.find("title")
        return t.get_text(strip=True) if t else ""

    def _body(self, soup: BeautifulSoup) -> str:
        container = (
            soup.select_one('[itemprop="articleBody"]')
            or soup.find("article")
            or soup.find("main")
        )
        if not container:
            return ""

        # limpieza mínima (lo que más molesta)
        for tag in container.select("script,style,noscript,header,footer,nav,form,aside,figure"):
            tag.decompose()

        parts = []
        for p in container.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt and len(txt) >= 25:
                parts.append(txt)

        body = "\n\n".join(parts).strip()
        # si sale demasiado corto, probablemente pillaste un contenedor malo o nota/teaser
        return body if len(body) >= 250 else body

    def crawl(self, max_news: int = 100, sleep_s: float = 0.25) -> list[dict]:
        home = self._soup(self.url)
        if not home:
            return []

        urls = self._home_links(home)[:max_news]
        out = []

        for u in urls:
            time.sleep(sleep_s)
            s = self._soup(u)
            if not s:
                continue

            title = self._title(s)
            body = self._body(s)
            if not title or not body:
                continue

            out.append({
                "id": str(uuid.uuid4()),
                "newspaper": self.newspaper,
                "date": self.fecha,
                "url": u,
                "title": title,
                "body": body,
            })

        return out
