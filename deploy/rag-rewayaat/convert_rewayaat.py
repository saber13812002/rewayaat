#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Convert a rewayaat corpus_sync raw snapshot into drag-provider input.

Produces, under --data-dir (mounted at /data/rewayaat in the container):
  blocks/<book>/<sid>.jsonl   one Word-Factory blocks file per hadith
  rewayaat_manifest.json      drag ingest-manifest input

Design (idempotent top-up):
  * document_id == source_id == the hadith's stable corpus source_id
    (e.g. "Al-Kafi-Volume-2-Kulayni:955").
  * drag chunk_id = sha1(f"{source_id}|{document_id}|{start}|{end}|{version}"),
    so re-ingesting the same hadith is a no-op (ON CONFLICT DO NOTHING) and
    new hadiths from later snapshots simply add new documents/chunks.
  * blocks per hadith:
      heading1 : "book \u203a part \u203a chapter"   (structural breadcrumb)
      paragraph: arabic text
      paragraph: english translation
      paragraph: persian translation   (only once enrichment provides it)

Usage:
  python3 convert_rewayaat.py \
      --raw data/raw/snapshot-XXXX.jsonl \
      --enrich data/out/enrich/snapshot-XXXX/enrichment.jsonl \
      --data-dir /home/saber/dedicated-rag/data/rewayaat \
      [--collection rewaat] [--work-id w-XXXXXXXX]
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import quote


def _clean(s: str | None) -> str:
    return (s or "").strip()


def _book_slug(book: str) -> str:
    slug = quote(_clean(book) or "unknown", safe="")
    return slug[:48] or "unknown"


def _doc_key(sid: str) -> str:
    return quote(sid, safe="")[:120] or "doc"


def load_enrichment(path: Path | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not path or not path.is_file():
        return out
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            sid = rec.get("source_id")
            if sid:
                out[sid] = rec
    return out


def make_blocks(line: dict, enr: dict | None) -> list[dict]:
    raw = line.get("raw") or {}
    sid = _clean(line.get("source_id"))
    book = _clean(raw.get("book")) or _clean(line.get("book")) or "Rewayat"
    part = _clean(raw.get("part"))
    chapter = _clean(raw.get("chapter"))

    crumbs = [book]
    if part:
        crumbs.append(part)
    if chapter and chapter.lower() not in ("no name", "none"):
        crumbs.append(chapter)
    heading = " \u203a ".join(crumbs)

    blocks: list[dict] = [{
        "type": "heading", "style": "heading 1", "text": heading,
        "raw_text": heading, "block_id": "b0",
    }]
    idx = 1
    for key in ("arabic", "english"):
        text = _clean(raw.get(key))
        if text:
            blocks.append({"type": "paragraph", "text": text,
                           "raw_text": text, "block_id": f"b{idx}"})
            idx += 1
    if enr:
        persian = _clean(enr.get("persian"))
        if persian:
            blocks.append({"type": "paragraph", "text": persian,
                           "raw_text": persian, "block_id": f"b{idx}"})
            idx += 1
    return blocks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", required=True, type=Path)
    ap.add_argument("--enrich", type=Path, default=None)
    ap.add_argument("--data-dir", required=True, type=Path)
    ap.add_argument("--collection", default="rewaat")
    ap.add_argument("--source-id", default="rewaat")
    ap.add_argument("--work-id", default="w-9c1f2a7d")
    ap.add_argument("--name", default="\u0648\u0631\u0648\u0627\u06cc\u0627\u062a (Al-K\u0101f\u012b \u0648 \u062d\u0627\u062f\u06cc\u062b\u0634\u062f\u0647\u200c\u0647\u0627\u06cc \u0633\u0646\u062f)")
    args = ap.parse_args()

    blocks_root = args.data_dir / "blocks"
    blocks_root.mkdir(parents=True, exist_ok=True)
    enr_map = load_enrichment(args.enrich)

    entries: list[dict] = []
    seen: set[str] = set()
    n_persian = 0
    with open(args.raw, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            sid = _clean(rec.get("source_id"))
            if not sid or sid in seen:
                continue
            seen.add(sid)
            enr = enr_map.get(sid)
            blocks = make_blocks(rec, enr)
            if len(blocks) <= 1:  # no text at all -> skip
                continue
            if enr and _clean(enr.get("persian")):
                n_persian += 1
            book = (rec.get("raw") or {}).get("book") or rec.get("book") or "unknown"
            out_path = blocks_root / _book_slug(book) / f"{_doc_key(sid)}.jsonl"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w", encoding="utf-8") as bo:
                for b in blocks:
                    bo.write(json.dumps(b, ensure_ascii=False) + "\n")
            # blocks_path is resolved INSIDE the container (drag.cli
            # _read_blocks), so emit the in-container absolute path:
            # --data-dir is mounted at /data -> /data/blocks/<book>/<key>.jsonl
            rel = out_path.relative_to(args.data_dir).as_posix()
            entries.append({
                "document_id": sid,
                "source_id": sid,
                "source_file": book,
                "source_hash": hashlib.sha1(sid.encode("utf-8")).hexdigest(),
                "blocks_path": "/data/" + rel,
            })

    manifest = {
        "collection": {
            "collection_id": args.collection,
            "source_id": args.source_id,
            "name": args.name,
            "work_id": args.work_id,
        },
        "documents": entries,
    }
    mpath = args.data_dir / "rewaat_manifest.json"
    with open(mpath, "w", encoding="utf-8") as mo:
        json.dump(manifest, mo, ensure_ascii=False, indent=1)

    print(json.dumps({
        "ok": True,
        "documents": len(entries),
        "manifest": str(mpath),
        "persian_blocks": n_persian,
        "enriched_total": len(enr_map),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
