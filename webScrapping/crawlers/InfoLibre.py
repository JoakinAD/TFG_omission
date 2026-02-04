# webScrapping/crawlers/InfoLibre.py
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import uuid

from crawlers.Crawler import Crawler



class InfoLibre(Crawler):
    def __init__(self, url: str):
        super().__init__(url)
        self.newspaper = "INFOLIBRE"

        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0 Safari/537.36"
                ),
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            }
        )

    def _clean_text(self, text: str) -> str:
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()
    
    # ---------------------------
    # Helpers
    # ---------------------------
    def _get_soup(self, url: str, timeout: int = 20) -> BeautifulSoup | None:
        try:
            r = self._session.get(url, timeout=timeout)
            if r.status_code != 200:
                return None
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException:
            return None

    def _is_valid_article_url(self, href: str) -> bool:
        if not href:
            return False
        href = href.strip()

        if href.startswith("#") or href.lower().startswith("javascript:"):
            return False

        abs_url = urljoin(self.url, href)
        parsed = urlparse(abs_url)

        # Debe ser del propio dominio
        if "infolibre.es" not in parsed.netloc:
            return False

        path = parsed.path.lower()

        # Bloqueos típicos
        deny_prefixes = (
            "/autores/",
            "/tags/",
            "/tag/",
            "/busqueda",
            "/buscar",
            "/rss",
            "/newsletter",
            "/suscripcion",
            "/login",
            "/registro",
        )
        if path.startswith(deny_prefixes):
            return False

        # Si NO quieres mezclar suplementos/partners, descomenta:
        deny_sections = (
            "/tintalibre/",
            "/mediapart/",
        )
        if any(path.startswith(s) for s in deny_sections):
            return False

        # Patrón típico de noticia en infoLibre:
        # /politica/xxxxx_1_2138453.html
        # /opinion/.../xxxxx_129_2138317.html
        # /como-lo-ve/xxxxx_7_2137673.html
        # (es decir: _<canal>_<id>.html)
        if re.search(r"_[0-9]+_[0-9]+\.html$", path):
            return True

        return False

    def _extract_home_links(self, soup: BeautifulSoup) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        def add(href: str):
            if not href:
                return
            if not self._is_valid_article_url(href):
                return
            u = urljoin(self.url, href)
            if u not in seen:
                seen.add(u)
                urls.append(u)

        # Titulares principales
        for a in soup.select("h1.ni-title a[href], h2.ni-title a[href]"):
            add(a.get("href", "").strip())

        # Subtitulares (muchas veces enlazan a otra noticia)
        for a in soup.select("aside.ni-subtitle a[href]"):
            add(a.get("href", "").strip())

        return urls


    def _extract_title(self, soup: BeautifulSoup) -> str:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(" ", strip=True)

        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            return og["content"].strip()

        t = soup.find("title")
        if t and t.get_text(strip=True):
            return t.get_text(strip=True)

        return ""

    def _extract_body(self, soup: BeautifulSoup) -> str:
        body_root = soup.select_one("div.article-body#article-body div.body-content")
        if not body_root:
            # fallback por si cambian IDs/clases
            body_root = soup.select_one("div.article-body div.body-content") or soup.select_one("div.article-body")
        if not body_root:
            return ""

        # 1) Eliminar bloques no editoriales
        for sel in [
            "script", "style", "noscript",
            "cs-ads",
            ".c-banner", ".c-banner--all", ".c-banner__mobile", ".c-banner__300x300", ".c-banner__300x600",
            ".ads-to-swipe",
            ".addoor-widget",
            ".OUTBRAIN",
            ".c-cta-member",
            "iframe",
        ]:
            for tag in body_root.select(sel):
                tag.decompose()

        parts: list[str] = []

        # 2) Capturar contenido en orden: headings y párrafos
        for node in body_root.select("h2, h3, h4, p, blockquote"):
            txt = node.get_text(" ", strip=True)
            if not txt:
                continue

            low = txt.lower()

            # filtros anti-CTA / basura residual (ajusta a gusto)
            if "hazte socio" in low or "apoya nuestro trabajo" in low or "navega sin publicidad" in low:
                continue

            parts.append(txt)

        return self._clean_text("\n\n".join(parts))


    def _is_probably_bad_extraction(self, body: str) -> bool:
        return len(body) < 300

    # ---------------------------
    # Main crawl
    # ---------------------------
    def crawl(self, max_news: int = 30, sleep_s: float = 0.25) -> list[dict]:
        home_soup = self._get_soup(self.url)
        if not home_soup:
            return []

        article_urls = self._extract_home_links(home_soup)
        if not article_urls:
            return []

        article_urls = article_urls[:max_news]

        results: list[dict] = []

        for u in article_urls:
            time.sleep(sleep_s)
            art_soup = self._get_soup(u)
            if not art_soup:
                continue

            title = self._extract_title(art_soup)
            body = self._extract_body(art_soup)

            # Fallback: coger todos los <p> de toda la página si falló el selector
            if self._is_probably_bad_extraction(body):
                for tag in art_soup.select("script, style, noscript, header, footer, nav, form, aside"):
                    tag.decompose()
                ps = [p.get_text(" ", strip=True) for p in art_soup.find_all("p")]
                ps = [t for t in ps if t and len(t) > 30]
                alt = self._clean_text("\n\n".join(ps))
                if len(alt) > len(body):
                    body = alt

            if not title or not body:
                continue

            results.append(
                {
                    "id": str(uuid.uuid4()),
                    "newspaper": self.newspaper,
                    "date": self.fecha,
                    "url": u,
                    "title": title,
                    "body": body,
                }
            )

        return results
