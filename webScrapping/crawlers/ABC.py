# webScrapping/crawlers/ABC.py
import re
import time
from urllib.parse import urljoin, urlparse
import uuid

import requests
from bs4 import BeautifulSoup

from crawlers.Crawler import Crawler


class ABC(Crawler):
    def __init__(self, url: str):
        super().__init__(url)
        self.newspaper = "ABC"

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
        """
        ABC suele usar URLs con sufijos tipo:
        - ...-nt.html
        - ...-nts.html
        - ...-di.html (directos)
        etc.
        Queremos quedarnos con noticias/artículos, evitando secciones, login, etc.
        """
        if not href:
            return False

        # Normaliza
        href = href.strip()

        # Excluye anchors / JS
        if href.startswith("#") or href.lower().startswith("javascript:"):
            return False

        # Acepta relativas/absolutas pero dentro de abc.es
        abs_url = urljoin(self.url, href)
        parsed = urlparse(abs_url)
        if "abc.es" not in parsed.netloc:
            return False

        # Excluir rutas claramente no-noticia
        deny_substrings = [
            "/miabc",
            "/micuenta",
            "/suscripcion",
            "/identidad",
            "/newsletters",
            "/favorito",
            "/podcast",
            "/pasatiempos",
            "/servicios",
            "/kioskoymas",
            "/archivo/",  # si quieres incluir archivo, quítalo
        ]
        for d in deny_substrings:
            if d in parsed.path.lower():
                return False

        # Patrón típico de noticia (muy frecuente en ABC)
        # Ej: ...-20260202134421-nt.html
        if re.search(r"-\d{14}-(nt|nts|di)\.html$", parsed.path):
            return True

        return False

    def _extract_home_links(self, soup: BeautifulSoup) -> list[str]:
        """
        Portada ABC (solo racks concretos):
        - Extrae enlaces de titulares (h2 a)
        - Opcional: incluye subenlaces "supps" (ul.voc-list--supps a)
        - Mantiene orden de aparición (no usa set() para ordenar)
        """

        # 1) Racks permitidos (ajusta si alguno cambia)
        allowed_racks = soup.select(
            "section.voc-grid.voc-grid--bdr-r-c.voc-rack, "
            "section.voc-grid.voc-rack, "
            "section.voc-grid.voc-grid--bdr-r-c.voc-rack.voc-rack--8-c"
        )

        urls: list[str] = []
        seen: set[str] = set()

        def add_url(href: str):
            if not href:
                return
            if not self._is_valid_article_url(href):
                return
            u = urljoin(self.url, href)
            if u not in seen:
                seen.add(u)
                urls.append(u)

        # 2) Dentro de esos racks: titulares y (opcional) supps
        for rack in allowed_racks:
            # Titulares principales
            for a in rack.select("h2 a[href]"):
                add_url(a.get("href", "").strip())

            # Subenlaces tipo "¿Cómo me afecta...?"
            for a in rack.select("ul.voc-list--supps a[href]"):
                add_url(a.get("href", "").strip())

        return urls


    def _clean_text(self, text: str) -> str:
        text = re.sub(r"\s+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _extract_title(self, soup: BeautifulSoup) -> str:
        # 1) h1
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            return h1.get_text(" ", strip=True)

        # 2) og:title
        og = soup.find("meta", attrs={"property": "og:title"})
        if og and og.get("content"):
            return og["content"].strip()

        # 3) title tag
        t = soup.find("title")
        if t and t.get_text(strip=True):
            return t.get_text(strip=True)

        return ""

    def _extract_body(self, soup: BeautifulSoup) -> str:
        article = soup.select_one("article.voc-d__article") or soup.find("article")
        if not article:
            return ""

        # 1) Quitar basura “conocida” (aunque luego no la uses, evita efectos colaterales)
        for sel in [
            "script", "style", "noscript",
            "header", "footer", "nav", "form",
            "figure",
            ".voc-advertising",
            ".voc-d-c-related-news",
            ".voc-topics",
            ".voc-most-read",
            ".voc-ints",
            ".voc-list",
            ".voc-cope-mod",
            ".voc-ob-wrapper",
            "ev-engagement",
            "ev-em-product-selection",
            "[data-voc-vam]",
            ".voc-save-news",
            ".voc-author__social",
        ]:
            for tag in article.select(sel):
                tag.decompose()

        parts: list[str] = []

        # 2) Preferir párrafos editoriales (ABC usa voc-p)
        ps = article.select("p.voc-p")
        if ps:
            for p in ps:
                txt = p.get_text(" ", strip=True)
                if not txt:
                    continue
                # Filtros anti-muro / anti-suscripción (ajusta a tu gusto)
                low = txt.lower()
                if "suscríbete" in low or "súmate" in low or "esta funcionalidad es sólo" in low:
                    continue
                parts.append(txt)

            # 3) Añadir listas útiles (opcional)
            for li in article.select(".voc-d-c-list__paragraph"):
                t = li.get_text(" ", strip=True)
                if t:
                    parts.append(t)

            return self._clean_text("\n\n".join(parts))

        # 4) Fallback genérico (si no hay voc-p)
        for p in article.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt and len(txt) > 40:
                parts.append(txt)

        return self._clean_text("\n\n".join(parts))

    def _is_probably_paywalled(self, body: str) -> bool:
        # Heurística simple: si el cuerpo es demasiado corto, probablemente paywall o fallo de selector
        return len(body) < 300

    # ---------------------------
    # Main crawl
    # ---------------------------
    def crawl(self, max_news: int = 100, sleep_s: float = 0.25) -> list[dict]:
        """
        Devuelve lista de dicts:
        {
          "newspaper": "ABC",
          "date": "dd/mm/yy",
          "url": "...",
          "title": "...",
          "body": "..."
        }
        """
        home_soup = self._get_soup(self.url)
        if not home_soup:
            return []

        article_urls = self._extract_home_links(home_soup)
        if not article_urls:
            return []

        # Limitar
        article_urls = article_urls[:max_news]

        results: list[dict] = []

        for u in article_urls:
            time.sleep(sleep_s)
            art_soup = self._get_soup(u)
            if not art_soup:
                continue

            title = self._extract_title(art_soup)
            body = self._extract_body(art_soup)

            # Si parece paywall o extracción mala, intenta fallback alternativo:
            # coger todos los <p> dentro del <article> si existe
            if self._is_probably_paywalled(body):
                article = art_soup.find("article")
                if article:
                    for tag in article.select(
                        "script, style, noscript, aside, figure, footer, header, form, nav"
                    ):
                        tag.decompose()
                    ps = [p.get_text(" ", strip=True) for p in article.find_all("p")]
                    ps = [t for t in ps if t and len(t) > 30]
                    alt_body = self._clean_text("\n\n".join(ps))
                    if len(alt_body) > len(body):
                        body = alt_body

            # Requisitos mínimos para dataset
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
