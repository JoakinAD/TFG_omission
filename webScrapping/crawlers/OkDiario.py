# webScrapping/crawlers/OkDiario.py
import re
import time
import uuid
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from crawlers.Crawler import Crawler


class OkDiario(Crawler):
    ARTICLE_RE = re.compile(r"-\d{6,}$")  # ...-16182949

    def __init__(self, url: str):
        super().__init__(url)
        self.newspaper = "OkDiario"

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

    def _clean(self, text: str) -> str:
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _is_article(self, href: str) -> bool:
        if not href:
            return False
        href = href.strip()
        if href.startswith("#") or href.lower().startswith("javascript:"):
            return False

        abs_url = urljoin(self.url, href)
        p = urlparse(abs_url)

        if p.netloc.lower() != "okdiario.com":
            return False

        path = (p.path or "").lower()
        if any(x in path for x in ("/login", "/registro", "/suscripcion", "/newsletter", "/okclub", "/okshopping")):
            return False

        return bool(self.ARTICLE_RE.search(path))

    def _home_links(self, soup: BeautifulSoup) -> list[str]:
        """
        Mantiene el comportamiento de tu crawler:
        recorre secciones rowContent y se para al ver el cintillo "OK AL DÍA".
        """
        urls, seen = [], set()

        for sec in soup.select("section.rowContent"):
            header = sec.select_one("header.cintillo")
            if header:
                txt = header.get_text(" ", strip=True).lower()
                if "ok al día" in txt or "ok al dia" in txt:
                    break

            for a in sec.select("a[href]"):
                href = a.get("href", "").strip()
                if not self._is_article(href):
                    continue
                u = urljoin(self.url, href)
                if u not in seen:
                    seen.add(u)
                    urls.append(u)

        # fallback: si por cambios del DOM no pillas suficiente
        if len(urls) < 10:
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
        root = (
            soup.select_one('[itemprop="articleBody"]')
            or soup.select_one(".entry-content")
            or soup.find("article")
            or soup.find("main")
        )
        if not root:
            return ""

        for tag in root.select("script,style,noscript,header,footer,nav,form,aside,figure,iframe"):
            tag.decompose()

        ps = [p.get_text(" ", strip=True) for p in root.find_all("p")]
        ps = [t for t in ps if t and len(t) >= 25]
        body = self._clean("\n\n".join(ps))

        # fallback corto si salió raro/corto: prueba con <article> completo
        if len(body) < 250:
            art = soup.find("article")
            if art and art is not root:
                for tag in art.select("script,style,noscript,header,footer,nav,form,aside,figure,iframe"):
                    tag.decompose()
                ps2 = [p.get_text(" ", strip=True) for p in art.find_all("p")]
                ps2 = [t for t in ps2 if t and len(t) >= 25]
                alt = self._clean("\n\n".join(ps2))
                if len(alt) > len(body):
                    body = alt

        return body

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
