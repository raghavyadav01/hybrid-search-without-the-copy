"""Shared paths and the product text recipe for the ESCI evaluation.

Text recipe (the `text` column; the same string is embedded and BM25-indexed):

    {title}
    Brand: {brand}            (line omitted when brand is null/empty)
    Color: {color}            (line omitted when color is null/empty)
    {bullets[:BULLET_CHARS]}  (bullet lines joined with "; ", whitespace collapsed)
    {description[:DESC_CHARS]} (HTML tags and entities stripped, whitespace collapsed)

Each piece is cut at a word boundary. The embedding model additionally truncates at
EMBED_MAX_TOKENS word-pieces, so in practice the vector sees title + brand + color +
the first ~150-200 words of bullets/description.
"""
import html
import re
from pathlib import Path

EVAL_DIR = Path(__file__).resolve().parent
DATA_DIR = EVAL_DIR / "data"
RESULTS_DIR = EVAL_DIR / "results"
DOWNLOAD_DIR = DATA_DIR / "download_esci"
PRODUCTS_SRC = DOWNLOAD_DIR / "shopping_queries_dataset_products.parquet"
EXAMPLES_SRC = DOWNLOAD_DIR / "shopping_queries_dataset_examples.parquet"

PRODUCTS_US = DATA_DIR / "products_us.parquet"          # one file, no embeddings
EMB_DIR = DATA_DIR / "embeddings"                        # sharded doc embeddings
QUERY_EMB = DATA_DIR / "query_emb_test_us.parquet"
QRELS = DATA_DIR / "qrels_test_us.parquet"
TABLE_DIR = DATA_DIR / "product_table"                   # ~128 MB files with embeddings

MODEL = "BAAI/bge-base-en-v1.5"
DIM = 768
# bge-en-v1.5 recommended instruction for short query -> passage retrieval.
QUERY_INSTRUCTION = "Represent this sentence for searching relevant passages: "
EMBED_MAX_TOKENS = 256

BULLET_CHARS = 600
DESC_CHARS = 400

LOCALE = "us"
GAINS = {"E": 1.0, "S": 0.1, "C": 0.01, "I": 0.0}

_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def _clean(s):
    if s is None:
        return ""
    s = str(s)
    if s.lower() in ("nan", "none"):
        return ""
    return _WS.sub(" ", s).strip()


def _cut(s, n):
    if len(s) <= n:
        return s
    cut = s[:n]
    sp = cut.rfind(" ")
    return cut[:sp] if sp > n // 2 else cut


def strip_html(s):
    return html.unescape(_TAG.sub(" ", s or ""))


def product_text(title, brand, color, bullets, description):
    parts = [_clean(title)]
    b = _clean(brand)
    if b:
        parts.append(f"Brand: {b}")
    c = _clean(color)
    if c:
        parts.append(f"Color: {c}")
    if bullets is not None and str(bullets).lower() not in ("nan", "none"):
        bl = "; ".join(x.strip() for x in str(bullets).split("\n") if x.strip())
        bl = _cut(_clean(bl), BULLET_CHARS)
        if bl:
            parts.append(bl)
    d = _cut(_clean(strip_html(_clean(description))), DESC_CHARS)
    if d:
        parts.append(d)
    return "\n".join(p for p in parts if p)
