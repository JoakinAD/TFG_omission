# webScrapping/crawlers/ElMundo.py
import json
import re
import time
import uuid
from urllib.parse import urljoin, urlparse
import html

import requests
from bs4 import BeautifulSoup

from crawlers.Crawler import Crawler


class ElMundo(Crawler):
    """
    Scraper para El Mundo usando:
      - https://www.elmundo.es/espana.html
      - https://www.elmundo.es/internacional.html

    Filtra opinión (clase ue-c-cover-content--is-opinion y/o URL con /opinion/).
    Extrae cuerpo preferentemente vía JSON-LD (NewsArticle.articleBody).
    """

    # URLs de secciones "limpias"
    SECTION_URLS = (
        "https://www.elmundo.es/espana.html",
        "https://www.elmundo.es/internacional.html",
    )

    # Patrón típico de noticia:
    # https://www.elmundo.es/espana/2026/02/04/69825562fdddffa76b8b456d.html
    # (id hex 24 chars)
    ARTICLE_RE = re.compile(r"/\d{4}/\d{2}/\d{2}/[0-9a-f]{24}\.html$", re.IGNORECASE)

    def __init__(self, url: str):
        # url esperado: "https://www.elmundo.es/"
        super().__init__(url)
        self.newspaper = "ELMUNDO"

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0 Safari/537.36"
                ),
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            }
        )

    # ---------------------------
    # Helpers
    # ---------------------------
    def _soup(self, url: str, timeout: int = 20) -> BeautifulSoup | None:
        try:
            r = self.session.get(url, timeout=timeout)
            if r.status_code != 200:
                return None
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException:
            return None

    def _is_article_url(self, href: str) -> bool:
        if not href:
            return False
        href = href.strip()
        if href.startswith("#") or href.lower().startswith("javascript:"):
            return False

        abs_url = urljoin(self.url, href)
        p = urlparse(abs_url)

        # dominio
        if "elmundo.es" not in p.netloc:
            return False

        path = p.path.lower()

        # excluir opinión por URL (por si se cuela)
        if "/opinion/" in path:
            return False

        # excluir ruido típico
        deny = ("/autor/", "/autores/", "/suscripcion", "/newsletter", "/tags/", "/tag/")
        if any(d in path for d in deny):
            return False

        return bool(self.ARTICLE_RE.search(path))

    @staticmethod
    def _clean_text(text: str) -> str:
        # Decodifica entidades HTML: &laquo; -> «, &nbsp; -> espacio, etc.
        text = html.unescape(text)

        # Normaliza NBSP (a veces queda como \xa0)
        text = text.replace("\xa0", " ")

        # Limpieza de espacios / saltos
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _extract_links_from_section(self, soup: BeautifulSoup) -> list[str]:
        """
        En las secciones, los items suelen venir en <article class="ue-c-cover-content ...">
        y el enlace principal es <a class="ue-c-cover-content__link" href="...">.

        Filtramos artículos con clase de opinión.
        """
        urls: list[str] = []
        seen: set[str] = set()

        for art in soup.select("article.ue-c-cover-content"):
            cls = " ".join(art.get("class", []))
            if "ue-c-cover-content--is-opinion" in cls:
                continue

            a = art.select_one("a.ue-c-cover-content__link[href]")
            if not a:
                # fallback: link whole content
                a = art.select_one("a.ue-c-cover-content__link-whole-content[href]")
            if not a:
                continue

            href = a.get("href", "").strip()
            if not self._is_article_url(href):
                continue

            u = urljoin(self.url, href)
            if u not in seen:
                seen.add(u)
                urls.append(u)

        return urls

    def _extract_title(self, soup: BeautifulSoup) -> str:
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

    def _body_from_jsonld(self, soup: BeautifulSoup) -> str:
        """
        Intenta extraer articleBody desde JSON-LD (NewsArticle).
        Mucho más estable que depender de clases del DOM.
        """
        scripts = soup.select('script[type="application/ld+json"]')
        for s in scripts:
            raw = s.string
            if not raw:
                continue
            raw = raw.strip()
            if not raw:
                continue

            try:
                data = json.loads(raw)
            except Exception:
                continue

            # puede venir como dict o lista
            candidates = data if isinstance(data, list) else [data]
            for obj in candidates:
                if not isinstance(obj, dict):
                    continue

                # a veces viene como @graph
                if "@graph" in obj and isinstance(obj["@graph"], list):
                    candidates.extend([x for x in obj["@graph"] if isinstance(x, dict)])

                typ = obj.get("@type") or obj.get("type")
                if isinstance(typ, list):
                    typ = typ[0] if typ else None

                if typ in ("NewsArticle", "Article", "ReportageNewsArticle"):
                    body = obj.get("articleBody")
                    if isinstance(body, str) and body.strip():
                        return self._clean_text(body)

        return ""

    def _body_from_dom(self, soup: BeautifulSoup) -> str:
        """
        Fallback DOM: intenta selectores frecuentes de El Mundo.
        """
        # selectores típicos (pueden cambiar)
        root = (
            soup.select_one("div.ue-c-article__body")
            or soup.select_one("div.ue-c-article__body-content")
            or soup.select_one("article")
        )
        if not root:
            return ""

        # limpia basura
        for tag in root.select("script,style,noscript,header,footer,nav,form,aside,figure,iframe"):
            tag.decompose()

        parts: list[str] = []
        for node in root.select("h2, h3, p, blockquote"):
            txt = node.get_text(" ", strip=True)
            if not txt:
                continue
            low = txt.lower()

            # filtros típicos de paywall/cta
            if "suscríbete" in low or "hazte suscriptor" in low or "inicia sesión" in low:
                continue
            if len(txt) < 35:
                continue

            parts.append(txt)

        return self._clean_text("\n\n".join(parts))

    def _extract_body(self, soup: BeautifulSoup) -> str:
        body = self._body_from_jsonld(soup)
        if body and len(body) >= 300:
            return body

        body = self._body_from_dom(soup)
        return body

    # ---------------------------
    # Main crawl
    # ---------------------------
    def crawl(self, max_news: int = 100, sleep_s: float = 0.25) -> list[dict]:
        # 1) recoger URLs desde secciones
        urls: list[str] = []
        seen: set[str] = set()

        for sec in self.SECTION_URLS:
            s = self._soup(sec)
            if not s:
                continue
            for u in self._extract_links_from_section(s):
                if u not in seen:
                    seen.add(u)
                    urls.append(u)

        if not urls:
            return []

        urls = urls[:max_news]

        # 2) scrapear artículos
        out: list[dict] = []
        for u in urls:
            time.sleep(sleep_s)
            s = self._soup(u)
            if not s:
                continue

            title = self._extract_title(s)
            body = self._extract_body(s)

            if not title or not body:
                continue
            if len(body) < 300:
                # evita entradas "vacías" típicas de bloqueos/DOM raro
                continue

            out.append(
                {
                    "id": str(uuid.uuid4()),
                    "newspaper": self.newspaper,
                    "date": self.fecha,
                    "url": u,
                    "title": title,
                    "body": body,
                }
            )

        return out
