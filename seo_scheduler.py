"""
SEO Portal — Data Scheduler v4
Plutonic Media ApS

Bulletproof domæne-håndtering:
- Virker uanset om domæne er sat op med https://, http://, www., eller ingenting
- Normaliserer BEGGE sider (Supabase-domæne + DataForSEO-svar) før sammenligning
- Auto-normaliserer domæner i Supabase ved opstart
- Eksempler der alle matcher korrekt:
    https://www.minfranskevinimportor.dk/vin  →  minfranskevinimportor.dk
    www.minfranskevinimportor.dk              →  minfranskevinimportor.dk
    minfranskevinimportor.dk                  →  minfranskevinimportor.dk
    HTTPS://WWW.PLUTONIC.DK/                  →  plutonic.dk
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

# ─── Logging ────────────────────────────────────────────────────────────────
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

DATAFORSEO_BASE     = "https://api.dataforseo.com/v3"
BATCH_SIZE          = 10   # Øg til 50 når det kører stabilt
BATCH_DELAY_SECONDS = 1


# ─── Bulletproof domæne-normalisering ────────────────────────────────────────
def normalize_domain(raw: str) -> str:
    """
    Normaliserer ETH vilkårligt domæne-input til bare roddomænet.

    Håndterer:
      - Protokoller:  https://, http://, ftp://
      - Præfikser:    www., www2., m.
      - Stier:        /vin/hvid/chardonnay
      - Query params: ?ref=google
      - Fragments:    #sektion
      - Port:         :8080
      - Whitespace og store bogstaver
      - Trailing punktum

    Eksempler:
      https://www.PLUTONIC.dk/om-os?ref=1  →  plutonic.dk
      WWW.minfranskevinimportor.dk/        →  minfranskevinimportor.dk
      http://m.example.dk:8080/path        →  example.dk
      example.dk.                          →  example.dk
    """
    if not raw:
        return ""

    d = raw.lower().strip()

    # Fjern protokol
    d = re.sub(r'^[a-z]+://', '', d)

    # Fjern alt efter første / ? eller #
    d = re.split(r'[/?#]', d)[0]

    # Fjern port (:8080)
    d = re.sub(r':\d+$', '', d)

    # Fjern mobile/www præfikser (www., www2., m., mobile.)
    d = re.sub(r'^(www\d*|m|mobile)\.', '', d)

    # Fjern trailing punktum
    d = d.rstrip('.')

    return d.strip()


def domains_match(domain_a: str, domain_b: str) -> bool:
    """Returnerer True hvis to domæner er det samme efter normalisering."""
    return normalize_domain(domain_a) == normalize_domain(domain_b)


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
    """
    Gennemgår alle projekter og normaliserer domæner in Supabase
    så de er konsistente (ingen www., https:// etc.)
    Kører ved hver opstart — idempotent og sikker.
    """
    log.info("Auto-normaliserer domæner i Supabase...")
    try:
        projects = supabase.table("projects").select("id, domain").execute().data or []
        fixed = 0
        for p in projects:
            original = p["domain"]
            normalized = normalize_domain(original)
            if original != normalized:
                supabase.table("projects").update({"domain": normalized}).eq("id", p["id"]).execute()
                log.info(f"  Normaliseret: '{original}' → '{normalized}'")
                fixed += 1
        if fixed == 0:
            log.info(f"  Alle {len(projects)} domæner er allerede normaliserede ✓")
        else:
            log.info(f"  ✓ {fixed} domæner normaliseret")
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
        log.error("Tjek DATAFORSEO_LOGIN og DATAFORSEO_PASSWORD i GitHub Secrets")
        return False
    except Exception as e:
        log.error(f"DataForSEO forbindelsesfejl: {e}")
        return False


def fetch_serp_batch(keywords: list[dict], location_code: int, raw_domain: str) -> list[dict]:
    """
    Henter SERP-placeringer for en batch keywords.
    Bruger normalize_domain() på BEGGE sider — virker uanset domæne-format.
    """
    target = normalize_domain(raw_domain)
    log.info(f"  Matcher mod normaliseret domæne: '{target}'")

    tasks = [
        {
            "keyword":       kw["keyword"],
            "location_code": location_code,
            "language_code": "da",
            "device":        "desktop",
            "os":            "windows",
            "depth":         100,
        }
        for kw in keywords
    ]

    results = []

    try:
        with httpx.Client(timeout=120) as c:
            r = c.post(
                f"{DATAFORSEO_BASE}/serp/google/organic/live/regular",
                headers=dfs_headers(),
                json=tasks,
            )

        if r.status_code != 200:
            log.error(f"DataForSEO HTTP {r.status_code}: {r.text[:300]}")
            return [{"keyword_id": kw["keyword_id"], "rank": None, "url": None} for kw in keywords]

        data = r.json()

        if data.get("status_code") != 20000:
            log.error(f"DataForSEO API fejl: {data.get('status_message')}")
            return [{"keyword_id": kw["keyword_id"], "rank": None, "url": None} for kw in keywords]

    except httpx.TimeoutException:
        log.error("DataForSEO timeout — reducér BATCH_SIZE")
        return [{"keyword_id": kw["keyword_id"], "rank": None, "url": None} for kw in keywords]
    except Exception as e:
        log.error(f"DataForSEO kald fejlede: {e}")
        return [{"keyword_id": kw["keyword_id"], "rank": None, "url": None} for kw in keywords]

    for i, task in enumerate(data.get("tasks", [])):
        kw_id   = keywords[i]["keyword_id"]
        kw_text = keywords[i]["keyword"]
        rank    = None
        url     = None

        if task.get("status_code") != 20000:
            log.warning(f"    Task fejl for '{kw_text}': {task.get('status_message')}")
            results.append({"keyword_id": kw_id, "rank": None, "url": None})
            continue

        task_result = task.get("result") or []
        items = task_result[0].get("items", []) if task_result else []

        # Debug: vis top-3 domæner for første keyword i batchen
        if i == 0:
            organic = [it for it in items if it.get("type") == "organic"][:3]
            top = [normalize_domain(it.get("domain", "")) for it in organic]
            log.info(f"  Debug '{kw_text}' — top domæner: {top}")

        # Match med normalisering på begge sider
        for item in items:
            if item.get("type") != "organic":
                continue
            item_domain = normalize_domain(item.get("domain", ""))
            if item_domain == target:
                rank = item.get("rank_absolute")
                url  = item.get("url")
                log.info(f"    ✓ '{kw_text}' → #{rank}  {url}")
                break

        if rank is None:
            log.info(f"    — '{kw_text}' ikke i top 100")

        results.append({"keyword_id": kw_id, "rank": rank, "url": url})

    found = sum(1 for r in results if r["rank"] is not None)
    log.info(f"  Batch: {found}/{len(results)} placeringer fundet")
    return results


def fetch_search_volumes(keywords: list[dict], location_code: int) -> dict:
    """Henter søgevolumener. Returnerer {keyword_lower: volume}"""
    try:
        with httpx.Client(timeout=60) as c:
            r = c.post(
                f"{DATAFORSEO_BASE}/keywords_data/google_ads/search_volume/live",
                headers=dfs_headers(),
                json=[{
                    "keywords":      [kw["keyword"] for kw in keywords],
                    "location_code": location_code,
                    "language_code": "da",
                }],
            )
        data = r.json()
    except Exception as e:
        log.error(f"Søgevolumen fejl: {e}")
        return {}

    volumes = {}
    for task in data.get("tasks", []):
        if task.get("status_code") == 20000:
            for item in (task.get("result") or []):
                kw = item.get("keyword", "")
                if kw:
                    volumes[kw.lower()] = item.get("search_volume")

    log.info(f"  Søgevolumener: {len(volumes)}/{len(keywords)} hentet")
    return volumes


# ─── Main ────────────────────────────────────────────────────────────────────
def run():
    log.info("=== SEO Portal Scheduler v4 starter ===")

    if not test_dataforseo():
        sys.exit(1)

    supabase = get_supabase()

    # Auto-normaliser alle domæner i Supabase
    auto_normalize_domains(supabase)

    now = datetime.now(timezone.utc).isoformat()

    # Hent projekter (domæner er nu normaliserede)
    try:
        projects = supabase.table("projects").select("id, domain, location_code, client_id").execute().data or []
    except Exception as e:
        log.error(f"Kan ikke hente projekter: {e}")
        sys.exit(1)

    if not projects:
        log.warning("Ingen projekter — opret kunder i Admin-panelet først")
        sys.exit(0)

    log.info(f"Fundet {len(projects)} projekter")
    total_rankings = 0

    for project in projects:
        pid      = project["id"]
        domain   = project["domain"]   # Allerede normaliseret
        location = project.get("location_code") or 2208

        log.info(f"\n▶ {domain} (location: {location})")

        # Keywords
        try:
            kws_raw  = supabase.table("keywords").select("id, keyword").eq("project_id", pid).execute().data or []
            keywords = [{"keyword_id": kw["id"], "keyword": kw["keyword"]} for kw in kws_raw]
        except Exception as e:
            log.error(f"Kan ikke hente keywords for {domain}: {e}")
            continue

        if not keywords:
            log.info("  Ingen keywords — springer over")
            continue

        log.info(f"  {len(keywords)} keywords")

        # Søgevolumener
        volume_map = {}
        for i in range(0, len(keywords), 100):
            batch_vols = fetch_search_volumes(keywords[i:i+100], location)
            volume_map.update(batch_vols)
            if i + 100 < len(keywords):
                time.sleep(BATCH_DELAY_SECONDS)

        # Rankings
        all_rows = []
        for i in range(0, len(keywords), BATCH_SIZE):
            batch    = keywords[i:i+BATCH_SIZE]
            batch_no = i // BATCH_SIZE + 1
            total_b  = (len(keywords) - 1) // BATCH_SIZE + 1
            log.info(f"  Batch {batch_no}/{total_b}...")

            rankings = fetch_serp_batch(batch, location, domain)

            for r in rankings:
                kw_text = next(
                    (kw["keyword"] for kw in batch if kw["keyword_id"] == r["keyword_id"]), ""
                ).lower()
                all_rows.append({
                    "keyword_id":    r["keyword_id"],
                    "rank":          r["rank"],
                    "url":           r["url"],
                    "search_volume": volume_map.get(kw_text),
                    "recorded_at":   now,
                })

            if i + BATCH_SIZE < len(keywords):
                time.sleep(BATCH_DELAY_SECONDS)

        # Gem
        try:
            supabase.table("rankings_history").insert(all_rows).execute()
            found = sum(1 for r in all_rows if r["rank"] is not None)
            not_found = len(all_rows) - found
            log.info(f"  ✓ Gemt: {found} med placering, {not_found} ikke i top 100")
            total_rankings += len(all_rows)
        except Exception as e:
            log.error(f"Fejl ved gemning for {domain}: {e}")

    log.info(f"\n=== Færdig! {total_rankings} rankings gemt i alt ===")


if __name__ == "__main__":
    run()
