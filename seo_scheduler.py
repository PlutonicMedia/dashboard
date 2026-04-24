"""
SEO Portal — Data Scheduler
Plutonic Media ApS

Kører via GitHub Actions hver 3. dag.
Henter data fra DataForSEO + Ahrefs og skriver til Supabase.

Påkrævede miljøvariabler (GitHub Secrets):
  SUPABASE_URL
  SUPABASE_SERVICE_KEY   ← brug service_role key (bypass RLS)
  DATAFORSEO_LOGIN
  DATAFORSEO_PASSWORD
  AHREFS_API_TOKEN
"""

import os
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

# ─── Config ─────────────────────────────────────────────────────────────────
SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
DATAFORSEO_LOGIN     = os.environ["DATAFORSEO_LOGIN"]
DATAFORSEO_PASSWORD  = os.environ["DATAFORSEO_PASSWORD"]
AHREFS_TOKEN         = os.environ.get("AHREFS_API_TOKEN", "")

DATAFORSEO_BASE = "https://api.dataforseo.com/v3"
AHREFS_BASE     = "https://api.ahrefs.com/v3"

# DataForSEO sender max 100 keywords pr. request
BATCH_SIZE = 100
# Pause mellem batches for at undgå rate limiting
BATCH_DELAY_SECONDS = 2


# ─── Supabase client ─────────────────────────────────────────────────────────
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


# ─── DataForSEO helpers ──────────────────────────────────────────────────────
def dataforseo_headers() -> dict:
    creds = base64.b64encode(
        f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()
    ).decode()
    return {
        "Authorization": f"Basic {creds}",
        "Content-Type": "application/json",
    }


def fetch_serp_rankings(keywords: list[dict], location_code: int, domain: str) -> list[dict]:
    """
    Kalder DataForSEO SERP Google Organic Live endpoint.
    keywords: [{"keyword_id": uuid, "keyword": str}, ...]
    Returnerer: [{"keyword_id": uuid, "rank": int|None, "url": str|None}, ...]
    """
    tasks = [
        {
            "keyword": kw["keyword"],
            "location_code": location_code,
            "language_code": "da",
            "device": "desktop",
            "os": "windows",
            "depth": 100,  # hent top 100 resultater
        }
        for kw in keywords
    ]

    results = []
    with httpx.Client(timeout=60) as client:
        response = client.post(
            f"{DATAFORSEO_BASE}/serp/google/organic/live/regular",
            headers=dataforseo_headers(),
            json=tasks,
        )
        response.raise_for_status()
        data = response.json()

    if data.get("status_code") != 20000:
        log.error(f"DataForSEO error: {data.get('status_message')}")
        return results

    for i, task in enumerate(data.get("tasks", [])):
        kw_id = keywords[i]["keyword_id"]
        rank = None
        url = None

        if task.get("status_code") == 20000:
            items = (
                task.get("result", [{}])[0]
                .get("items", [])
            )
            for item in items:
                if item.get("type") == "organic":
                    item_domain = item.get("domain", "").replace("www.", "")
                    target_domain = domain.replace("www.", "").replace("https://", "").replace("http://", "").split("/")[0]
                    if item_domain == target_domain:
                        rank = item.get("rank_absolute")
                        url = item.get("url")
                        break

        results.append({
            "keyword_id": kw_id,
            "rank": rank,
            "url": url,
        })

    return results


def fetch_search_volumes(keywords: list[dict], location_code: int) -> dict:
    """
    Henter søgevolumener fra DataForSEO Keywords Data.
    Returnerer: {keyword_text: search_volume}
    """
    keyword_texts = [kw["keyword"] for kw in keywords]

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
        response.raise_for_status()
        data = response.json()

    volumes = {}
    for task in data.get("tasks", []):
        if task.get("status_code") == 20000:
            for item in task.get("result", []):
                kw = item.get("keyword", "")
                vol = item.get("search_volume")
                if kw:
                    volumes[kw.lower()] = vol

    return volumes


# ─── Ahrefs helpers ──────────────────────────────────────────────────────────
def fetch_ahrefs_site_audit(domain: str) -> Optional[dict]:
    """
    Henter Site Audit data fra Ahrefs.
    Returnerer dict med health_score, crawled_urls, issues.
    """
    if not AHREFS_TOKEN:
        log.warning("AHREFS_API_TOKEN ikke sat — springer Site Audit over")
        return None

    headers = {
        "Authorization": f"Bearer {AHREFS_TOKEN}",
        "Accept": "application/json",
    }

    try:
        with httpx.Client(timeout=30) as client:
            # Hent liste af site audit projekter
            resp = client.get(
                f"{AHREFS_BASE}/site-audit/projects",
                headers=headers,
            )
            resp.raise_for_status()
            projects = resp.json().get("projects", [])

        # Find projektet der matcher domænet
        project_id = None
        for proj in projects:
            if domain.replace("www.", "") in proj.get("domain", ""):
                project_id = proj.get("id")
                break

        if not project_id:
            log.warning(f"Intet Ahrefs Site Audit projekt fundet for {domain}")
            return None

        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{AHREFS_BASE}/site-audit/projects/{project_id}/issues",
                headers=headers,
                params={"limit": 20},
            )
            resp.raise_for_status()
            issues_data = resp.json()

        return {
            "project_id": project_id,
            "health_score": issues_data.get("health_score"),
            "crawled_urls": issues_data.get("crawled_urls_count"),
            "issues": issues_data.get("issues", [])[:10],  # top 10 issues
        }

    except Exception as e:
        log.error(f"Ahrefs API fejl for {domain}: {e}")
        return None


# ─── Supabase writes ─────────────────────────────────────────────────────────
def upsert_rankings(supabase: Client, rows: list[dict]):
    """
    Indsætter rækker i rankings_history.
    Bruger insert (ikke upsert) — vi vil gemme historik.
    """
    if not rows:
        return
    result = supabase.table("rankings_history").insert(rows).execute()
    return result


def upsert_site_audit(supabase: Client, project_id: str, audit_data: dict):
    """
    Upsert Site Audit data i site_audit_snapshots tabellen.
    Opretter tabellen hvis den ikke eksisterer (via migration).
    """
    row = {
        "project_id": project_id,
        "health_score": audit_data.get("health_score"),
        "crawled_urls": audit_data.get("crawled_urls"),
        "issues": json.dumps(audit_data.get("issues", [])),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("site_audit_snapshots").upsert(row, on_conflict="project_id").execute()


# ─── Main ────────────────────────────────────────────────────────────────────
def run():
    log.info("=== SEO Portal Scheduler starter ===")
    supabase = get_supabase()
    now = datetime.now(timezone.utc).isoformat()

    # Hent alle projekter med tilhørende keywords
    log.info("Henter projekter fra Supabase...")
    projects_resp = supabase.table("projects").select("id, domain, location_code, client_id").execute()
    projects = projects_resp.data or []
    log.info(f"Fundet {len(projects)} projekter")

    for project in projects:
        project_id = project["id"]
        domain = project["domain"]
        location_code = project.get("location_code", 2208)

        log.info(f"▶ Behandler projekt: {domain} (location: {location_code})")

        # Hent keywords for dette projekt
        kw_resp = (
            supabase.table("keywords")
            .select("id, keyword")
            .eq("project_id", project_id)
            .execute()
        )
        keywords = [{"keyword_id": kw["id"], "keyword": kw["keyword"]} for kw in (kw_resp.data or [])]

        if not keywords:
            log.info(f"  Ingen keywords fundet for {domain} — springer over")
            continue

        log.info(f"  {len(keywords)} keywords at opdatere")

        # Hent søgevolumener (én gang for alle keywords i projektet)
        log.info(f"  Henter søgevolumener fra DataForSEO...")
        volume_map = {}
        for i in range(0, len(keywords), BATCH_SIZE):
            batch = keywords[i:i + BATCH_SIZE]
            batch_volumes = fetch_search_volumes(batch, location_code)
            volume_map.update(batch_volumes)
            if i + BATCH_SIZE < len(keywords):
                time.sleep(BATCH_DELAY_SECONDS)

        # Hent rankings i batches
        all_ranking_rows = []
        for i in range(0, len(keywords), BATCH_SIZE):
            batch = keywords[i:i + BATCH_SIZE]
            log.info(f"  Henter rankings batch {i // BATCH_SIZE + 1}/{(len(keywords) - 1) // BATCH_SIZE + 1}...")

            rankings = fetch_serp_rankings(batch, location_code, domain)

            for r in rankings:
                kw_text = next(
                    (kw["keyword"] for kw in batch if kw["keyword_id"] == r["keyword_id"]),
                    ""
                ).lower()
                search_volume = volume_map.get(kw_text)

                all_ranking_rows.append({
                    "keyword_id": r["keyword_id"],
                    "rank": r["rank"],
                    "url": r["url"],
                    "search_volume": search_volume,
                    "recorded_at": now,
                })

            if i + BATCH_SIZE < len(keywords):
                time.sleep(BATCH_DELAY_SECONDS)

        # Gem rankings i Supabase
        log.info(f"  Gemmer {len(all_ranking_rows)} rankings i Supabase...")
        upsert_rankings(supabase, all_ranking_rows)

        # Site Audit via Ahrefs
        if AHREFS_TOKEN:
            log.info(f"  Henter Ahrefs Site Audit for {domain}...")
            audit = fetch_ahrefs_site_audit(domain)
            if audit:
                upsert_site_audit(supabase, project_id, audit)
                log.info(f"  Site Audit gemt (health score: {audit.get('health_score')})")

        log.info(f"  ✓ {domain} færdig")

    log.info("=== Scheduler færdig ===")


if __name__ == "__main__":
    run()
