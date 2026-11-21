"""Step 1: filter ESCI products + examples to the US locale, build `text`, build test qrels.

Run: .venv/bin/python -I prep_products.py
Writes data/products_us.parquet, data/qrels_test_us.parquet, results/dataset_counts.json.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common import (EXAMPLES_SRC, LOCALE, PRODUCTS_SRC, PRODUCTS_US, QRELS, RESULTS_DIR,
                    product_text)


def main():
    p = pq.read_table(PRODUCTS_SRC).to_pandas()
    p = p[p.product_locale == LOCALE].reset_index(drop=True)
    p = p.rename(columns={
        "product_title": "title", "product_description": "description",
        "product_bullet_point": "bullet_point", "product_brand": "brand",
        "product_color": "color", "product_locale": "locale"})
    dup = int(p.product_id.duplicated().sum())
    p = p.drop_duplicates("product_id").reset_index(drop=True)
    p["text"] = [product_text(*r) for r in zip(p.title, p.brand, p.color, p.bullet_point,
                                                p.description)]
    p = p[["product_id", "title", "description", "bullet_point", "brand", "color", "locale",
           "text"]]
    pq.write_table(pa.Table.from_pandas(p, preserve_index=False), PRODUCTS_US,
                   compression="zstd")

    e = pq.read_table(EXAMPLES_SRC).to_pandas()
    e = e[e.product_locale == LOCALE]
    t = e[e.split == "test"][["query_id", "query", "product_id", "esci_label", "small_version",
                              "large_version"]].reset_index(drop=True)
    pq.write_table(pa.Table.from_pandas(t, preserve_index=False), QRELS)

    in_corpus = t.product_id.isin(set(p.product_id)).mean()
    counts = {
        "locale": LOCALE,
        "products_us": int(len(p)),
        "products_us_duplicate_ids_dropped": dup,
        "text_chars_median": float(p.text.str.len().median()),
        "text_chars_p90": float(p.text.str.len().quantile(0.9)),
        "test_queries": int(t.query_id.nunique()),
        "test_judgments": int(len(t)),
        "test_queries_small_version (Task 1)": int(t[t.small_version == 1].query_id.nunique()),
        "test_judgments_small_version (Task 1)": int((t.small_version == 1).sum()),
        "test_label_counts": {k: int(v) for k, v in t.esci_label.value_counts().items()},
        "test_queries_with_exact": int(t[t.esci_label == "E"].query_id.nunique()),
        "test_judged_products_in_corpus_frac": float(in_corpus),
        "train_queries": int(e[e.split == "train"].query_id.nunique()),
        "train_judgments": int((e.split == "train").sum()),
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / "dataset_counts.json").write_text(json.dumps(counts, indent=2))
    print(json.dumps(counts, indent=2))
    print("example text:\n" + p.text.iloc[3])


if __name__ == "__main__":
    main()
