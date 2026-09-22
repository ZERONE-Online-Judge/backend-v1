"""Serve crawlable public content in the same document used by the React app."""
from functools import lru_cache
from html import escape
from html.parser import HTMLParser
import json
from pathlib import Path
import re

from app.settings import settings
from app.services.seo import public_origin


class _HeadWithoutMetadata(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=False)
        self.parts: list[str] = []
        self.skipping: str | None = None

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "title" or (tag == "script" and (
            values.get("type") == "application/ld+json"
            or values.get("id") == "zoj-seo-data"
        )):
            self.skipping = tag
        if self.skipping:
            return
        name = (values.get("name") or "").lower()
        prop = (values.get("property") or "").lower()
        if tag == "meta" and (
            name in {"description", "robots"}
            or name.startswith("twitter:") or prop.startswith("og:")
            or (name == "google-site-verification" and settings.google_site_verification)
            or (name == "naver-site-verification" and settings.naver_site_verification)
        ):
            return
        if tag == "link" and values.get("rel") == "canonical":
            return
        self.parts.append(self.get_starttag_text())

    handle_startendtag = handle_starttag

    def handle_endtag(self, tag):
        if self.skipping:
            if tag == self.skipping:
                self.skipping = None
            return
        self.parts.append(f"</{tag}>")

    def handle_data(self, data):
        if not self.skipping:
            self.parts.append(data)

    def handle_entityref(self, name):
        self.handle_data(f"&{name};")

    def handle_charref(self, name):
        self.handle_data(f"&#{name};")

    def handle_comment(self, data):
        if not self.skipping:
            self.parts.append(f"<!--{data}-->")


def _json_for_script(data) -> str:
    # JSON is script text, not HTML: entity escaping would corrupt it.
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace(
        "<", "\\u003c"
    ).replace(">", "\\u003e").replace("&", "\\u0026").replace(
        "\u2028", "\\u2028"
    ).replace("\u2029", "\\u2029")


def _meta(key: str, value: str, *, property: bool = False) -> str:
    attr = "property" if property else "name"
    return f'<meta {attr}="{escape(key, quote=True)}" content="{escape(value, quote=True)}" />'


def _head(metadata: dict) -> str:
    canonical = metadata["canonical"]
    origin = public_origin()
    image = f"{origin}/og-logo.png"
    tags = [
        f'<title>{escape(metadata["title"])}</title>',
        _meta("description", metadata["description"]),
        _meta("robots", metadata["robots"]),
        f'<link rel="canonical" href="{escape(canonical, quote=True)}" />' if canonical else "",
        _meta("og:locale", "ko_KR", property=True),
        _meta("og:type", "website", property=True),
        _meta("og:site_name", "ZOJ", property=True),
        _meta("og:title", metadata["title"], property=True),
        _meta("og:description", metadata["description"], property=True),
        _meta("og:url", canonical, property=True) if canonical else "",
        _meta("og:image", image, property=True),
        _meta("og:image:width", "1200", property=True),
        _meta("og:image:height", "630", property=True),
        _meta("og:image:alt", "ZOJ · Zerone Online Judge", property=True),
        _meta("twitter:card", "summary_large_image"),
        _meta("twitter:title", metadata["title"]),
        _meta("twitter:description", metadata["description"]),
        _meta("twitter:image", image),
        _meta("twitter:image:alt", "ZOJ · Zerone Online Judge"),
    ]
    if metadata["structured_data"]:
        tags.append('<script type="application/ld+json" id="zoj-structured-data">'
                    + _json_for_script(metadata["structured_data"]) + '</script>')
    for name, value in (
        ("google-site-verification", settings.google_site_verification),
        ("naver-site-verification", settings.naver_site_verification),
    ):
        if value:
            tags.append(_meta(name, value))
    tags.append('<script type="application/json" id="zoj-seo-data">'
                + _json_for_script(metadata) + '</script>')
    return "\n".join(tags)


def _public_content(metadata: dict) -> str:
    """Visible, accessible fallback, replaced when the application renders."""
    paragraphs = "".join(f"<p>{escape(text)}</p>" for text in metadata["paragraphs"])
    links = "".join(
        f'<li><a href="{escape(link["href"], quote=True)}">{escape(link["label"])}</a></li>'
        for link in metadata["links"]
        if link["href"].startswith("/") and not link["href"].startswith("//")
        and "\\" not in link["href"]
    )
    return (
        '<main id="zoj-public-summary" style="max-width:72rem;margin:auto;padding:3rem 1.5rem;'
        'font-family:system-ui,sans-serif;line-height:1.8;color:#172033">'
        '<a href="/" aria-label="ZOJ 홈">ZOJ · Zerone Online Judge</a>'
        f'<h1>{escape(metadata["heading"])}</h1>{paragraphs}'
        f'<nav aria-label="관련 페이지"><ul>{links}</ul></nav></main>'
    )


def render_document(template: str, metadata: dict) -> str:
    head = re.search(r"<head\b[^>]*>(.*?)</head\s*>", template, re.I | re.S)
    root = re.search(r'<div\s+id=[\'"]root[\'"]\s*>\s*</div>', template, re.I)
    if not head or not root:
        raise ValueError("Frontend template must contain a head and an empty React root")
    parser = _HeadWithoutMetadata()
    parser.feed(head.group(1))
    clean_head = "".join(parser.parts)
    document = template[:head.start(1)] + clean_head + _head(metadata) + template[head.end(1):]
    return re.sub(
        r'<div\s+id=[\'"]root[\'"]\s*>\s*</div>',
        lambda match: '<div id="root">' + _public_content(metadata) + '</div>',
        document,
        count=1,
        flags=re.I,
    )


@lru_cache(maxsize=2)
def _read_template(path: str, modified_ns: int, size: int) -> str:
    return Path(path).read_text(encoding="utf-8-sig")


def frontend_template() -> str:
    path = Path(settings.frontend_html_path)
    stat = path.stat()
    return _read_template(str(path), stat.st_mtime_ns, stat.st_size)
