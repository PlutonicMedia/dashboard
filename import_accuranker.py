"""
Accuranker CSV Import Script
Plutonic Media ApS

Importerer historisk data fra Accuranker CSV-eksport til Supabase.
Opretter:
  1. Keywords i keywords-tabellen (hvis de ikke allerede findes)
  2. To rankings_history rækker per keyword:
     - Initial dato + initial rank
     - Dagens dato + nuværende rank

Brug:
  pip install supabase pandas python-dotenv
  python import_accuranker.py --csv AccuRanker_footstore.csv --domain footstore.dk

Eller med .env fil:
  SUPABASE_URL=...
  SUPABASE_SERVICE_KEY=...
"""

import argparse
import os
import sys
import re
import logging
from datetime import datetime, timezone

import pandas as pd
from supabase import create_client

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Domæne-normalisering (samme som scheduler) ───────────────────────────────
def normalize_domain(raw: str) -> str:
    if not raw:
        return ""
    d = raw.lower().strip()
    d = re.sub(r'^[a-z]+://', '', d)
    d = re.split(r'[/?#]', d)[0]
    d = re.sub(r':\d+$', '', d)
    d = re.sub(r'^(www\d*|m|mobile)\.', '', d)
    return d.rstrip('.').strip()


def parse_rank(val) -> int | None:
    """Konverterer Accuranker rank-værdier til int eller None."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s in ("Not in top 100", "No rank for date", "", "-"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def parse_date(val) -> str | None:
    """Parser dato til ISO-format med UTC timezone."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s or s in ("", "-"):
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(hour=12, tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue
    log.warning(f"Kunne ikke parse dato: '{s}'")
    return None


def main():
    parser = argparse.ArgumentParser(description="Importer Accuranker CSV til Supabase")
    parser.add_argument("--csv",    required=True, help="Sti til Accuranker CSV-fil")
    parser.add_argument("--domain", required=True, help="Domæne f.eks. footstore.dk")
    parser.add_argument("--dry-run", action="store_true", help="Vis hvad der ville ske uden at skrive")
    args = parser.parse_args()

    # ── Supabase ──────────────────────────────────────────────────────────────
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = os.environ.get("SUPABASE_SERVICE_KEY", "")

    if not supabase_url or not supabase_key:
        log.error("Sæt SUPABASE_URL og SUPABASE_SERVICE_KEY som miljøvariabler")
        log.error("Eksempel: export SUPABASE_URL=https://xxx.supabase.co")
        sys.exit(1)

    if not args.dry_run:
        supabase = create_client(supabase_url, supabase_key)
        log.info("✓ Supabase forbundet")

    # ── Læs CSV ───────────────────────────────────────────────────────────────
    log.info(f"Læser CSV: {args.csv}")
    try:
        df = pd.read_csv(args.csv, low_memory=False)
    except Exception as e:
        log.error(f"Kunne ikke læse CSV: {e}")
        sys.exit(1)

    log.info(f"  {len(df)} rækker fundet")
    log.info(f"  Kolonner: {list(df.columns[:10])}...")

    # ── Find projekt i Supabase ────────────────────────────────────────────────
    target_domain = normalize_domain(args.domain)
    log.info(f"Søger efter projekt med domæne: '{target_domain}'")

    if not args.dry_run:
        projects_resp = supabase.table("projects").select("id, domain, client_id").execute()
        projects = projects_resp.data or []

        project = None
        for p in projects:
            if normalize_domain(p["domain"]) == target_domain:
                project = p
                break

        if not project:
            log.error(f"Ingen projekt fundet med domæne '{target_domain}' i Supabase")
            log.error("Opret kunden i Admin-panelet først")
            sys.exit(1)

        project_id = project["id"]
        log.info(f"✓ Projekt fundet: {project_id}")

        # Hent eksisterende keywords for dette projekt
        existing_resp = supabase.table("keywords").select("id, keyword").eq("project_id", project_id).execute()
        existing_kws  = {kw["keyword"].lower(): kw["id"] for kw in (existing_resp.data or [])}
        log.info(f"  {len(existing_kws)} eksisterende keywords i Supabase")
    else:
        project_id    = "DRY-RUN-UUID"
        existing_kws  = {}

    # ── Processer CSV ─────────────────────────────────────────────────────────
    # Accuranker kolonnenavne
    COL_KEYWORD      = "Keyword"
    COL_TAGS         = "Tags"
    COL_DATE         = "Date"
    COL_RANK         = "Rank"
    COL_INITIAL_DATE = "Initial date"
    COL_INITIAL_RANK = "Initial rank"
    COL_URL          = "URL"
    COL_VOLUME       = "Search volume"

    # Verificer kolonner
    required_cols = [COL_KEYWORD, COL_DATE, COL_RANK, COL_INITIAL_DATE, COL_INITIAL_RANK]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log.error(f"Manglende kolonner i CSV: {missing}")
        log.error(f"Tilgængelige kolonner: {list(df.columns)}")
        sys.exit(1)

    new_keywords     = []
    rankings_to_insert = []
    skipped          = 0
    processed        = 0

    for _, row in df.iterrows():
        keyword_text = str(row.get(COL_KEYWORD, "")).strip()
        if not keyword_text:
            continue

        # Søgevolumen
        volume = None
        try:
            v = row.get(COL_VOLUME)
            if not pd.isna(v):
                volume = int(float(v))
        except (ValueError, TypeError):
            pass

        # Tags
        tags = str(row.get(COL_TAGS, "")).strip() if not pd.isna(row.get(COL_TAGS, "")) else ""

        # URL
        url = str(row.get(COL_URL, "")).strip() if not pd.isna(row.get(COL_URL, "")) else None
        if url == "nan" or url == "":
            url = None

        # ── Keyword ID ────────────────────────────────────────────────────────
        kw_lower = keyword_text.lower()
        if kw_lower not in existing_kws:
            # Nyt keyword — tilføj til bulk-insert liste
            new_keywords.append({
                "project_id": project_id,
                "keyword":    keyword_text,
            })
            # Placeholder — erstattes med rigtig ID efter insert
            existing_kws[kw_lower] = f"NEW:{keyword_text}"

        # ── Dagens ranking ────────────────────────────────────────────────────
        today_date = parse_date(row.get(COL_DATE))
        today_rank = parse_rank(row.get(COL_RANK))

        if today_date:
            rankings_to_insert.append({
                "_keyword_text": kw_lower,
                "rank":          today_rank,
                "url":           url,
                "search_volume": volume,
                "recorded_at":   today_date,
            })

        # ── Initial ranking ───────────────────────────────────────────────────
        initial_date = parse_date(row.get(COL_INITIAL_DATE))
        initial_rank = parse_rank(row.get(COL_INITIAL_RANK))

        if initial_date and initial_date != today_date:
            rankings_to_insert.append({
                "_keyword_text": kw_lower,
                "rank":          initial_rank,
                "url":           None,   # URL kendes ikke for historisk dato
                "search_volume": volume,
                "recorded_at":   initial_date,
            })

        processed += 1

    log.info(f"\nKlar til import:")
    log.info(f"  Nye keywords:    {len(new_keywords)}")
    log.info(f"  Eksist. keywords: {len([k for k in existing_kws.values() if not str(k).startswith('NEW:')])}")
    log.info(f"  Rankings rækker: {len(rankings_to_insert)}")

    if args.dry_run:
        log.info("\n[DRY RUN] Ingen data skrevet. Kør uden --dry-run for at importere.")
        # Vis eksempel
        log.info("\nEksempel på første 3 keywords der ville importeres:")
        for kw in new_keywords[:3]:
            log.info(f"  '{kw['keyword']}'")
        return

    # ── Indsæt nye keywords ───────────────────────────────────────────────────
    if new_keywords:
        log.info(f"\nIndsætter {len(new_keywords)} nye keywords...")
        CHUNK = 500
        inserted_keywords = []
        for i in range(0, len(new_keywords), CHUNK):
            chunk  = new_keywords[i:i+CHUNK]
            result = supabase.table("keywords").insert(chunk).execute()
            inserted_keywords.extend(result.data or [])
            log.info(f"  Batch {i//CHUNK + 1}: {len(chunk)} keywords indsat")

        # Opdater existing_kws med rigtige IDs
        for kw in inserted_keywords:
            existing_kws[kw["keyword"].lower()] = kw["id"]

        log.info(f"✓ {len(inserted_keywords)} keywords indsat")

    # ── Indsæt rankings ───────────────────────────────────────────────────────
    log.info(f"\nIndsætter {len(rankings_to_insert)} rankings...")

    # Erstat _keyword_text med rigtige keyword_ids
    final_rankings = []
    skipped_no_id  = 0
    for r in rankings_to_insert:
        kw_text = r.pop("_keyword_text")
        kw_id   = existing_kws.get(kw_text)
        if not kw_id or str(kw_id).startswith("NEW:"):
            skipped_no_id += 1
            continue
        r["keyword_id"] = kw_id
        final_rankings.append(r)

    if skipped_no_id:
        log.warning(f"  {skipped_no_id} rankings sprunget over (intet keyword ID)")

    # Bulk insert i chunks
    CHUNK = 500
    total_inserted = 0
    for i in range(0, len(final_rankings), CHUNK):
        chunk = final_rankings[i:i+CHUNK]
        try:
            supabase.table("rankings_history").insert(chunk).execute()
            total_inserted += len(chunk)
            log.info(f"  Batch {i//CHUNK + 1}: {len(chunk)} rækker indsat")
        except Exception as e:
            log.error(f"  Fejl ved batch {i//CHUNK + 1}: {e}")

    log.info(f"\n=== Import færdig ===")
    log.info(f"  Keywords indsat:  {len(new_keywords)}")
    log.info(f"  Rankings indsat:  {total_inserted}")
    log.info(f"  Kørselsdato:      {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
