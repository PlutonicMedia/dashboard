"""
SEO Portal — Data Scheduler v2
Plutonic Media ApS

Kører via GitHub Actions hver 3. dag.
Henter data fra DataForSEO + Ahrefs og skriver til Supabase.

GitHub Secrets påkrævet:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY   ← service_role key (bypass RLS)
  DATAFORSEO_LOGIN
  DATAFORSEO_PASSWORD
  AHREFS_API_TOKEN       (valgfri — kun til Site Audit)
"""

import os
import sys
import json
import time
import logging
import base64
from datetime import datetime, timezone
from typing import Optional

import httpx
from supabase import create_client, Client

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config med tydelige fejlmeldinger ──────────────────────────────────────
def get_env(key: str, required: bool = True) -> str:
    val = os.environ.get(key, "")
    if required and not val:
        log.error(f"MANGLER miljøvariabel: {key}")
        log.error("Tjek at GitHub Secret er sat korrekt under Settings → Secrets → Actions")
        sys.exit(1)
    return val

SUPABASE_URL         = get_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = get_env("SUPABASE_SERVICE_KEY")
DATAFORSEO_LOGIN     = get_env("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD  = get_env("DATAFORSEO_PASSWORD")
AHREFS_TOKEN         = get_env("AHREFS_API_TOKEN", required=False)

DATAFORSEO_BASE = "https://api.dataforseo.com/v3"
BATCH_SIZE = 100
BATCH_DELAY_SECONDS = 2

# ─── Startup check ───────────────────────────────────────────────────────────
log.info("=== SEO Portal Scheduler v2 starter ===")
log.info(f"SUPABASE_URL: {SUPABASE_URL[:40]}...")
log.info(f"DATAFORSEO_LOGIN: {DATAFORSEO_LOGIN}")
log.info(f"AHREFS aktiveret: {'Ja' if AHREFS_TOKEN else 'Nej'}")


# ─── Supabase client ─────────────────────────────────────────────────────────
def get_supabase() -> Client:
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✓ Supabase forbundet")
        return client
    except Exception as e:
        log.error(f"Kunne ikke forbinde til Supabase: {e}")
        sys.exit(1)


# ─── DataForSEO helpers ──────────────────────────────────────────────────────
def dataforseo_headers() -> dict:
    creds = base64.b64encode(
        f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()
    ).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Content-Type": "application/json",
    }


def test_dataforseo_connection() -> bool:
    """Test at DataForSEO credentials virker."""
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(
                f"{DATAFORSEO_BASE}/appendix/user_data",
                headers=dataforseo_headers(),
            )
            data = resp.json()
            if data.get("status_code") == 20000:
                log.info(f"✓ DataForSEO forbundet — konto: {data.get('tasks', [{}])[0].get('result', [{}])[0].get('login', 'ukendt')}")
                return True
            else:
                log.error(f"DataForSEO auth fejl: {data.get('status_message')} (kode: {data.get('status_code')})")
                log.error("Tjek DATAFORSEO_LOGIN og DATAFORSEO_PASSWORD i GitHub Secrets")
                return False
    except Exception as e:
        log.error(f"DataForSEO forbindelsesfejl: {e}")
        return False


def fetch_serp_rankings(keywords: list[dict], location_code: int, domain: str) -> list[dict]:
    """
    Kalder DataForSEO SERP Google Organic Live endpoint.
    """
    tasks = [
        {
            "keyword": kw["keyword"],
            "location_code": location_code,
            "language_code": "da",
            "device": "desktop",
            "os": "windows",
            "depth": 100,
        }
        for kw in keywords
    ]

    results = []

    try:
        with httpx.Client(timeout=120) as client:
            response = client.post(
                f"{DATAFORSEO_BASE}/serp/google/organic/live/regular",
                headers=dataforseo_headers(),
                json=tasks,
            )

        if response.status_code != 200:
            log.error(f"DataForSEO HTTP fejl: {response.status_code} — {response.text[:200]}")
            return results

        data = response.json()

        if data.get("status_code") != 20000:
            log.error(f"DataForSEO API fejl: {data.get('status_message')} (kode: {data.get('status_code')})")
            return results

    except httpx.TimeoutException:
        log.error("DataForSEO timeout — prøv igen eller reducer batch-størrelse")
        return results
    except Exception as e:
        log.error(f"Uventet fejl ved DataForSEO kald: {e}")
        return results

    # Rens domæne for sammenligning
    clean_domain = domain.replace("www.", "").replace("https://", "").replace("http://", "").split("/")[0].lower()

    for i, task in enumerate(data.get("tasks", [])):
        kw_id = keywords[i]["keyword_id"]
        rank = None
        url = None

        if task.get("status_code") == 20000:
            items = (
                task.get("result", [{}])[0].get("items", [])
            ) if task.get("result") else []

            for item in items:
                if item.get("type") == "organic":
                    item_domain = item.get("domain", "").replace("www.", "").lower()
                    if item_domain == clean_domain:
                        rank = item.get("rank_absolute")
                        url = item.get("url")
                        break
        else:
            log.warning(f"Keyword task fejl: {task.get('status_message')} for keyword_id={kw_id}")

        results.append({
            "keyword_id": kw_id,
            "rank": rank,
            "url": url,
        })

    found = sum(1 for r in results if r["rank"] is not None)
    log.info(f"  → Fandt {found}/{len(results)} placeringer for {domain}")

    return results


def fetch_search_volumes(keywords: list[dict], location_code: int) -> dict:
    """
    Henter søgevolumener fra DataForSEO Keywords Data.
    """
    keyword_texts = [kw["keyword"] for kw in keywords]

    try:
        with httpx.Client(timeout=60) as client:
            response = client.post(
                f"{DATAFORSEO_BASE}/keywords_data/google_ads/search_volume/live",
                headers=dataforseo_headers(),
                json=[{
                    "keywords": keyword_texts,
                    "location_code": location_code,
                    "language_code": "da",
                }],
            )

        data = response.json()

    except Exception as e:
        log.error(f"Fejl ved søgevolumen-kald: {e}")
        return {}

    volumes = {}
    for task in data.get("tasks", []):
        if task.get("status_code") == 20000:
            for item in (task.get("result") or []):
                kw = item.get("keyword", "")
                vol = item.get("search_volume")
                if kw:
                    volumes[kw.lower()] = vol

    log.info(f"  → Søgevolumener hentet for {len(volumes)}/{len(keywords)} keywords")
    return volumes


# ─── Supabase writes ─────────────────────────────────────────────────────────
def insert_rankings(supabase: Client, rows: list[dict]):
    """Indsæt rækker i rankings_history."""
    if not rows:
        log.warning("Ingen rækker at indsætte")
        return

    try:
        result = supabase.table("rankings_history").insert(rows).execute()
        log.info(f"  ✓ Indsat {len(rows)} rækker i rankings_history")
        return result
    except Exception as e:
        log.error(f"Fejl ved indsætning i rankings_history: {e}")
        raise


# ─── Main ────────────────────────────────────────────────────────────────────
def run():
    # Test DataForSEO forbindelse
    if not test_dataforseo_connection():
        log.error("Afbryder — DataForSEO credentials er ugyldige")
        sys.exit(1)

    supabase = get_supabase()
    now = datetime.now(timezone.utc).isoformat()

    # Hent alle projekter
    log.info("Henter projekter fra Supabase...")
    try:
        projects_resp = supabase.table("projects").select("id, domain, location_code, client_id").execute()
        projects = projects_resp.data or []
    except Exception as e:
        log.error(f"Kunne ikke hente projekter: {e}")
        sys.exit(1)

    if not projects:
        log.warning("Ingen projekter fundet i Supabase — er der oprettet kunder i Admin-panelet?")
        sys.exit(0)

    log.info(f"Fundet {len(projects)} projekter")

    total_rankings = 0

    for project in projects:
        project_id = project["id"]
        domain = project["domain"]
        location_code = project.get("location_code") or 2208

        log.info(f"\n▶ Behandler: {domain} (location_code: {location_code})")

        # Hent keywords
        try:
            kw_resp = (
                supabase.table("keywords")
                .select("id, keyword")
                .eq("project_id", project_id)
                .execute()
            )
            keywords = [{"keyword_id": kw["id"], "keyword": kw["keyword"]} for kw in (kw_resp.data or [])]
        except Exception as e:
            log.error(f"Kunne ikke hente keywords for {domain}: {e}")
            continue

        if not keywords:
            log.info(f"  Ingen keywords for {domain} — spring over")
            continue

        log.info(f"  {len(keywords)} keywords at behandle")

        # Søgevolumener
        volume_map = {}
        for i in range(0, len(keywords), BATCH_SIZE):
            batch = keywords[i:i + BATCH_SIZE]
            batch_vols = fetch_search_volumes(batch, location_code)
            volume_map.update(batch_vols)
            if i + BATCH_SIZE < len(keywords):
                time.sleep(BATCH_DELAY_SECONDS)

        # Rankings
        all_rows = []
        for i in range(0, len(keywords), BATCH_SIZE):
            batch = keywords[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(keywords) - 1) // BATCH_SIZE + 1
            log.info(f"  Batch {batch_num}/{total_batches} rankings...")

            rankings = fetch_serp_rankings(batch, location_code, domain)

            for r in rankings:
                kw_text = next(
                    (kw["keyword"] for kw in batch if kw["keyword_id"] == r["keyword_id"]),
                    ""
                ).lower()
                search_volume = volume_map.get(kw_text)

                all_rows.append({
                    "keyword_id": r["keyword_id"],
                    "rank": r["rank"],
                    "url": r["url"],
                    "search_volume": search_volume,
                    "recorded_at": now,
                })

            if i + BATCH_SIZE < len(keywords):
                time.sleep(BATCH_DELAY_SECONDS)

        # Gem
        insert_rankings(supabase, all_rows)
        total_rankings += len(all_rows)

    log.info(f"\n=== Færdig! {total_rankings} rankings gemt i alt ===")


if __name__ == "__main__":
    run()
