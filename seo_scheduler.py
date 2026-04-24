"""
SEO Portal — Data Scheduler v5
Plutonic Media ApS

Fix v5:
- DataForSEO live/regular API tillader KUN 1 keyword per request
- Sender nu ét keyword ad gangen med kort pause imellem
- Bulletproof domæne-normalisering bevaret fra v4
"""

import os
import sys
import re
import time
import logging
import base64
from datetime import datetime, timezone

import httpx
from supabase import create_client, Client

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
def get_env(key: str, required: bool = True) -> str:
    val = os.environ.get(key, "")
    if required and not val:
        log.error(f"MANGLER miljøvariabel: {key}")
        log.error("Tjek GitHub Secrets under Settings → Secrets → Actions")
        sys.exit(1)
    return val

SUPABASE_URL         = get_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = get_env("SUPABASE_SERVICE_KEY")
DATAFORSEO_LOGIN     = get_env("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD  = get_env("DATAFORSEO_PASSWORD")
AHREFS_TOKEN         = get_env("AHREFS_API_TOKEN", required=False)

DATAFORSEO_BASE    = "https://api.dataforseo.com/v3"
KEYWORD_DELAY      = 0.5   # sekunder mellem hvert keyword-kald


# ─── Bulletproof domæne-normalisering ────────────────────────────────────────
def normalize_domain(raw: str) -> str:
    """
    Virker uanset om input er:
      https://www.minfranskevinimportor.dk/vin  →  minfranskevinimportor.dk
      www.plutonic.dk                           →  plutonic.dk
      HTTPS://WWW.KUNDE.DK/                     →  kunde.dk
    """
    if not raw:
        return ""
    d = raw.lower().strip()
    d = re.sub(r'^[a-z]+://', '', d)       # fjern protokol
    d = re.split(r'[/?#]', d)[0]           # fjern sti
    d = re.sub(r':\d+$', '', d)            # fjern port
    d = re.sub(r'^(www\d*|m|mobile)\.', '', d)  # fjern www/m præfiks
    return d.rstrip('.').strip()


# ─── Supabase ────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✓ Supabase forbundet")
        return client
    except Exception as e:
        log.error(f"Supabase forbindelsesfejl: {e}")
        sys.exit(1)


def auto_normalize_domains(supabase: Client):
    """Normaliserer alle domæner i Supabase ved opstart."""
    log.info("Auto-normaliserer domæner i Supabase...")
    try:
        projects = supabase.table("projects").select("id, domain").execute().data or []
        fixed = 0
        for p in projects:
            original   = p["domain"]
            normalized = normalize_domain(original)
            if original != normalized:
                supabase.table("projects").update({"domain": normalized}).eq("id", p["id"]).execute()
                log.info(f"  '{original}' → '{normalized}'")
                fixed += 1
        log.info(f"  {fixed} domæner normaliseret, {len(projects)-fixed} allerede korrekte")
    except Exception as e:
        log.warning(f"Auto-normalisering fejlede (ikke kritisk): {e}")


# ─── DataForSEO ──────────────────────────────────────────────────────────────
def dfs_headers() -> dict:
    creds = base64.b64encode(
        f"{DATAFORSEO_LOGIN}:{DATAFORSEO_PASSWORD}".encode()
    ).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


def test_dataforseo() -> bool:
    try:
        with httpx.Client(timeout=15) as c:
            r = c.get(f"{DATAFORSEO_BASE}/appendix/user_data", headers=dfs_headers())
        data = r.json()
        if data.get("status_code") == 20000:
            log.info("✓ DataForSEO forbundet")
            return True
        log.error(f"DataForSEO auth fejl: {data.get('status_message')} (kode {data.get('status_code')})")
        return False
    except Exception as e:
        log.error(f"DataForSEO forbindelsesfejl: {e}")
        return False


def fetch_single_keyword(keyword: str, location_code: int, target_domain: str) -> tuple[int | None, str | None]:
    """
    Henter SERP-placering for ét enkelt keyword.
    Returnerer (rank, url) eller (None, None) hvis ikke fundet.

    NOTE: DataForSEO live/regular tillader KUN 1 task per request.
    """
    try:
        with httpx.Client(timeout=30) as c:
            r = c.post(
                f"{DATAFORSEO_BASE}/serp/google/organic/live/regular",
                headers=dfs_headers(),
                json=[{
                    "keyword":       keyword,
                    "location_code": location_code,
                    "language_code": "da",
                    "device":        "desktop",
                    "os":            "windows",
                    "depth":         100,
                }],
            )

        if r.status_code != 200:
            log.warning(f"  HTTP {r.status_code} for '{keyword}'")
            return None, None

        data = r.json()

        if data.get("status_code") != 20000:
            log.warning(f"  API fejl for '{keyword}': {data.get('status_message')}")
            return None, None

        tasks = data.get("tasks", [])
        if not tasks or tasks[0].get("status_code") != 20000:
            task_msg = tasks[0].get("status_message") if tasks else "ingen tasks"
            log.warning(f"  Task fejl for '{keyword}': {task_msg}")
            return None, None

        result = tasks[0].get("result") or []
        items  = result[0].get("items", []) if result else []

        target = normalize_domain(target_domain)
        for item in items:
            if item.get("type") != "organic":
                continue
            if normalize_domain(item.get("domain", "")) == target:
                rank = item.get("rank_absolute")
                url  = item.get("url")
                return rank, url

        return None, None

    except httpx.TimeoutException:
        log.warning(f"  Timeout for '{keyword}' — springer over")
        return None, None
    except Exception as e:
        log.warning(f"  Fejl for '{keyword}': {e}")
        return None, None


def fetch_search_volumes(keywords: list[str], location_code: int) -> dict:
    """
    Henter søgevolumener for op til 1000 keywords ad gangen.
    Returnerer {keyword_lower: volume}
    """
    volumes = {}
    # Google Ads API tillader 1000 keywords per request
    CHUNK = 1000
    for i in range(0, len(keywords), CHUNK):
        chunk = keywords[i:i+CHUNK]
        try:
            with httpx.Client(timeout=60) as c:
                r = c.post(
                    f"{DATAFORSEO_BASE}/keywords_data/google_ads/search_volume/live",
                    headers=dfs_headers(),
                    json=[{
                        "keywords":      chunk,
                        "location_code": location_code,
                        "language_code": "da",
                    }],
                )
            data = r.json()
            for task in data.get("tasks", []):
                if task.get("status_code") == 20000:
                    for item in (task.get("result") or []):
                        kw = item.get("keyword", "")
                        if kw:
                            volumes[kw.lower()] = item.get("search_volume")
        except Exception as e:
            log.error(f"Søgevolumen fejl: {e}")

    log.info(f"  Søgevolumener: {len(volumes)}/{len(keywords)} hentet")
    return volumes


# ─── Main ────────────────────────────────────────────────────────────────────
def run():
    log.info("=== SEO Portal Scheduler v5 starter ===")

    if not test_dataforseo():
        sys.exit(1)

    supabase = get_supabase()
    auto_normalize_domains(supabase)

    now = datetime.now(timezone.utc).isoformat()

    # Hent projekter
    try:
        projects = supabase.table("projects").select("id, domain, location_code, client_id").execute().data or []
    except Exception as e:
        log.error(f"Kan ikke hente projekter: {e}")
        sys.exit(1)

    if not projects:
        log.warning("Ingen projekter — opret kunder i Admin-panelet først")
        sys.exit(0)

    log.info(f"Fundet {len(projects)} projekter")
    total_saved = 0

    for project in projects:
        pid      = project["id"]
        domain   = project["domain"]
        location = project.get("location_code") or 2208

        log.info(f"\n▶ {domain} (location: {location})")

        # Keywords
        try:
            kws_raw  = supabase.table("keywords").select("id, keyword").eq("project_id", pid).execute().data or []
            keywords = [{"id": kw["id"], "keyword": kw["keyword"]} for kw in kws_raw]
        except Exception as e:
            log.error(f"Kan ikke hente keywords for {domain}: {e}")
            continue

        if not keywords:
            log.info("  Ingen keywords — springer over")
            continue

        log.info(f"  {len(keywords)} keywords at behandle")

        # Søgevolumener — ét samlet kald
        kw_texts   = [kw["keyword"] for kw in keywords]
        volume_map = fetch_search_volumes(kw_texts, location)

        # Rankings — ét keyword ad gangen
        rows  = []
        found = 0

        for idx, kw in enumerate(keywords, 1):
            kw_id   = kw["id"]
            kw_text = kw["keyword"]

            rank, url = fetch_single_keyword(kw_text, location, domain)

            if rank:
                log.info(f"  [{idx}/{len(keywords)}] ✓ '{kw_text}' → #{rank}")
                found += 1
            else:
                log.info(f"  [{idx}/{len(keywords)}] — '{kw_text}' ikke i top 100")

            rows.append({
                "keyword_id":    kw_id,
                "rank":          rank,
                "url":           url,
                "search_volume": volume_map.get(kw_text.lower()),
                "recorded_at":   now,
            })

            # Lille pause for ikke at overbelaste API
            time.sleep(KEYWORD_DELAY)

        # Gem alle på én gang
        try:
            supabase.table("rankings_history").insert(rows).execute()
            log.info(f"  ✓ Gemt: {found} med placering, {len(rows)-found} ikke i top 100")
            total_saved += len(rows)
        except Exception as e:
            log.error(f"Fejl ved gemning for {domain}: {e}")

    log.info(f"\n=== Færdig! {total_saved} rankings gemt i alt ===")


if __name__ == "__main__":
    run()
