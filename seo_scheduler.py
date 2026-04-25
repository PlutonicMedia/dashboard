"""
SEO Portal — Data Scheduler v8
Plutonic Media ApS

Henter fra:
  - DataForSEO: keyword rankings + søgevolumener
  - Ahrefs:     trafik-historik, top sider, konkurrenter, domain overview

GitHub Secrets påkrævet:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  DATAFORSEO_LOGIN
  DATAFORSEO_PASSWORD
  AHREFS_API_TOKEN

Ændringer v7 → v8:
  - Fix: /site-explorer/metrics manglede obligatorisk 'date' parameter
  - Fix: /site-explorer/top-pages manglede obligatorisk 'date' parameter
  - Fix: /site-explorer/organic-competitors manglede obligatorisk 'date' parameter
  - Fix: metrics-history fjerner 'org_keywords' (ikke tilgængeligt felt)
         Tilgængelige felter: date, org_traffic, org_cost, paid_cost, paid_traffic
"""

import os
import sys
import re
import time
import logging
import base64
from datetime import datetime, timezone, date
from dateutil.relativedelta import relativedelta

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
        sys.exit(1)
    return val

SUPABASE_URL         = get_env("SUPABASE_URL")
SUPABASE_SERVICE_KEY = get_env("SUPABASE_SERVICE_KEY")
DATAFORSEO_LOGIN     = get_env("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD  = get_env("DATAFORSEO_PASSWORD")
AHREFS_TOKEN         = get_env("AHREFS_API_TOKEN", required=False)

DATAFORSEO_BASE  = "https://api.dataforseo.com/v3"
AHREFS_BASE      = "https://api.ahrefs.com/v3"
KEYWORD_DELAY    = 0.5


# ─── Domæne-normalisering ────────────────────────────────────────────────────
def normalize_domain(raw: str) -> str:
    if not raw:
        return ""
    d = raw.lower().strip()
    d = re.sub(r'^[a-z]+://', '', d)
    d = re.split(r'[/?#]', d)[0]
    d = re.sub(r':\d+$', '', d)
    d = re.sub(r'^(www\d*|m|mobile)\.', '', d)
    return d.rstrip('.').strip()


# ─── Supabase ────────────────────────────────────────────────────────────────
def get_supabase() -> Client:
    try:
        client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        log.info("✓ Supabase forbundet")
        return client
    except Exception as e:
        log.error(f"Supabase fejl: {e}")
        sys.exit(1)


def auto_normalize_domains(supabase: Client):
    log.info("Auto-normaliserer domæner...")
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
        log.warning(f"Auto-normalisering fejlede: {e}")


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
        if r.json().get("status_code") == 20000:
            log.info("✓ DataForSEO forbundet")
            return True
        log.error(f"DataForSEO auth fejl: {r.json().get('status_message')}")
        return False
    except Exception as e:
        log.error(f"DataForSEO forbindelsesfejl: {e}")
        return False


def fetch_single_keyword(keyword: str, location_code: int, target_domain: str) -> tuple:
    """Henter SERP-placering for ét keyword. Returnerer (rank, url)."""
    target = normalize_domain(target_domain)
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
        data = r.json()
        if data.get("status_code") != 20000:
            return None, None
        tasks = data.get("tasks", [])
        if not tasks or tasks[0].get("status_code") != 20000:
            return None, None
        items = (tasks[0].get("result") or [{}])[0].get("items", [])
        for item in items:
            if item.get("type") == "organic":
                if normalize_domain(item.get("domain", "")) == target:
                    return item.get("rank_absolute"), item.get("url")
        return None, None
    except Exception as e:
        log.warning(f"  Fejl for '{keyword}': {e}")
        return None, None


def fetch_search_volumes(keywords: list[str], location_code: int) -> dict:
    volumes = {}
    for i in range(0, len(keywords), 1000):
        chunk = keywords[i:i+1000]
        try:
            with httpx.Client(timeout=60) as c:
                r = c.post(
                    f"{DATAFORSEO_BASE}/keywords_data/google_ads/search_volume/live",
                    headers=dfs_headers(),
                    json=[{"keywords": chunk, "location_code": location_code, "language_code": "da"}],
                )
            for task in r.json().get("tasks", []):
                if task.get("status_code") == 20000:
                    for item in (task.get("result") or []):
                        kw = item.get("keyword", "")
                        if kw:
                            volumes[kw.lower()] = item.get("search_volume")
        except Exception as e:
            log.error(f"Søgevolumen fejl: {e}")
    log.info(f"  Søgevolumener: {len(volumes)}/{len(keywords)}")
    return volumes


# ─── Ahrefs ──────────────────────────────────────────────────────────────────
def ahrefs_headers() -> dict:
    return {
        "Authorization": f"Bearer {AHREFS_TOKEN}",
        "Accept": "application/json",
    }


def today_str() -> str:
    """Returnerer dags dato som YYYY-MM-DD — bruges som 'date' i Ahrefs-kald."""
    return date.today().strftime("%Y-%m-%d")


def test_ahrefs() -> bool:
    if not AHREFS_TOKEN:
        log.warning("AHREFS_API_TOKEN ikke sat — springer Ahrefs over")
        return False
    try:
        with httpx.Client(timeout=15) as c:
            r = c.get(
                f"{AHREFS_BASE}/subscription-info/limits-and-usage",
                headers=ahrefs_headers(),
            )
        if r.status_code == 200:
            log.info("✓ Ahrefs forbundet")
            return True
        log.error(f"Ahrefs auth fejl: {r.status_code} — {r.text[:200]}")
        return False
    except Exception as e:
        log.error(f"Ahrefs forbindelsesfejl: {e}")
        return False


def fetch_ahrefs_domain_overview(domain: str) -> dict | None:
    """
    Henter domain overview: DR, backlinks, referring domains, org traffic.

    FIX v8: Tilføjet obligatorisk 'date' parameter (dagens dato).
    Tilgængelige select-felter verificeret mod Ahrefs API v3:
      domain_rating, ahrefs_rank, backlinks, referring_domains,
      org_keywords, org_traffic
    """
    try:
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/metrics",
                headers=ahrefs_headers(),
                params={
                    "target": domain,
                    "mode":   "domain",
                    "date":   today_str(),  # FIX: obligatorisk parameter
                    "select": "domain_rating,ahrefs_rank,backlinks,referring_domains,org_keywords,org_traffic",
                },
            )
        if r.status_code != 200:
            log.warning(f"  Ahrefs overview HTTP {r.status_code}")
            log.warning(f"  Response: {r.text[:300]}")
            return None
        metrics = r.json().get("metrics", {}) or {}
        return {
            "domain_rating":     metrics.get("domain_rating"),
            "ahrefs_rank":       metrics.get("ahrefs_rank"),
            "backlinks":         metrics.get("backlinks"),
            "referring_domains": metrics.get("referring_domains"),
            "organic_keywords":  metrics.get("org_keywords"),
            "organic_traffic":   metrics.get("org_traffic"),
        }
    except Exception as e:
        log.error(f"  Ahrefs overview fejl for {domain}: {e}")
        return None


def fetch_ahrefs_traffic_history(domain: str) -> list[dict]:
    """
    Henter månedlig organisk trafik-historik via Ahrefs Site Explorer.
    Returnerer liste af {month, organic_traffic, traffic_value}

    FIX v8: Fjernet 'org_keywords' fra select — ikke tilgængeligt i metrics-history.
            Bekræftede tilgængelige felter: date, org_traffic, org_cost, paid_cost, paid_traffic
    NOTE: organic_keywords gemmes som NULL i databasen — hentes fra ahrefs_overview i stedet.
    """
    try:
        date_to   = today_str()
        date_from = (date.today() - relativedelta(years=2)).strftime("%Y-%m-%d")

        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/metrics-history",
                headers=ahrefs_headers(),
                params={
                    "target":           domain,
                    "mode":             "domain",
                    "date_from":        date_from,
                    "date_to":          date_to,
                    "history_grouping": "monthly",
                    # FIX: 'org_keywords' eksisterer ikke i dette endpoint — fjernet
                    "select":           "date,org_traffic,org_cost",
                },
            )

        if r.status_code != 200:
            log.warning(f"  Ahrefs traffic history HTTP {r.status_code} for {domain}")
            log.warning(f"  Response: {r.text[:300]}")
            return []

        data = r.json()
        metrics = data.get("metrics", []) or []

        result = []
        for m in metrics:
            raw_date = m.get("date", "")
            if not raw_date:
                continue
            month = raw_date[:7] + "-01"
            result.append({
                "month":            month,
                "organic_traffic":  m.get("org_traffic"),
                "organic_keywords": None,   # Ikke tilgængeligt i metrics-history
                "traffic_value":    m.get("org_cost"),
            })

        log.info(f"  Trafik historik: {len(result)} måneder hentet")
        return result

    except Exception as e:
        log.error(f"  Ahrefs traffic history fejl for {domain}: {e}")
        return []


def fetch_ahrefs_top_pages(domain: str) -> list[dict]:
    """
    Henter top sider via Ahrefs Site Explorer top-pages endpoint.
    Returnerer liste af {url, top_keyword, position, traffic, keyword_count, search_volume}

    FIX v8: Tilføjet obligatorisk 'date' parameter (dagens dato).
    FIX v7: 'top_keyword_best_position' → 'top_keyword_best_pos' (fortsat korrekt)
    """
    try:
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/top-pages",
                headers=ahrefs_headers(),
                params={
                    "target":   domain,
                    "mode":     "domain",
                    "date":     today_str(),  # FIX: obligatorisk parameter
                    "select":   "url,top_keyword,top_keyword_best_pos,sum_traffic,keywords_count,top_keyword_volume",
                    "order_by": "sum_traffic:desc",
                    "limit":    20,
                },
            )

        if r.status_code != 200:
            log.warning(f"  Ahrefs top pages HTTP {r.status_code} for {domain}")
            log.warning(f"  Response: {r.text[:300]}")
            return []

        pages = r.json().get("pages", []) or []

        result = []
        for p in pages:
            url = p.get("url", "")
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                url_path = parsed.path or "/"
            except Exception:
                url_path = url

            result.append({
                "url":           url_path,
                "top_keyword":   p.get("top_keyword"),
                "position":      p.get("top_keyword_best_pos"),
                "traffic":       p.get("sum_traffic"),
                "keyword_count": p.get("keywords_count"),
                "search_volume": p.get("top_keyword_volume"),
            })

        log.info(f"  Top sider: {len(result)} hentet")
        return result

    except Exception as e:
        log.error(f"  Ahrefs top pages fejl for {domain}: {e}")
        return []


def fetch_ahrefs_competitors(domain: str) -> list[dict]:
    """
    Henter organiske konkurrenter via Ahrefs.
    Returnerer liste af {domain, domain_rating, organic_traffic, common_keywords, is_self}

    FIX v8: Tilføjet obligatorisk 'date' parameter (dagens dato).
    FIX v7: select-felt 'competitor' → 'domain' + response-parsing rettet (fortsat korrekt)
    """
    try:
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/organic-competitors",
                headers=ahrefs_headers(),
                params={
                    "target":   domain,
                    "mode":     "domain",
                    "date":     today_str(),  # FIX: obligatorisk parameter
                    "select":   "domain,domain_rating,org_traffic,common_keywords",
                    "order_by": "org_traffic:desc",
                    "limit":    10,
                },
            )

        if r.status_code != 200:
            log.warning(f"  Ahrefs competitors HTTP {r.status_code} for {domain}")
            log.warning(f"  Response: {r.text[:300]}")
            return []

        competitors = r.json().get("competitors", []) or []

        result = []
        for comp in competitors:
            comp_domain = normalize_domain(comp.get("domain", ""))
            result.append({
                "domain":          comp_domain,
                "domain_rating":   comp.get("domain_rating"),
                "organic_traffic": comp.get("org_traffic"),
                "common_keywords": comp.get("common_keywords"),
                "is_self":         comp_domain == normalize_domain(domain),
            })

        log.info(f"  Konkurrenter: {len(result)} hentet")
        return result

    except Exception as e:
        log.error(f"  Ahrefs competitors fejl for {domain}: {e}")
        return []


# ─── Ahrefs Supabase writes ───────────────────────────────────────────────────
def upsert_traffic_history(supabase: Client, project_id: str, rows: list[dict]):
    if not rows:
        return
    data = [{"project_id": project_id, **r} for r in rows]
    try:
        supabase.table("ahrefs_traffic_history").upsert(
            data, on_conflict="project_id,month"
        ).execute()
        log.info(f"  ✓ Trafik historik: {len(data)} måneder gemt")
    except Exception as e:
        log.error(f"  Fejl ved gem af trafik historik: {e}")


def upsert_top_pages(supabase: Client, project_id: str, rows: list[dict]):
    if not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    data = [{"project_id": project_id, "recorded_at": now, **r} for r in rows]
    try:
        supabase.table("ahrefs_top_pages").upsert(
            data, on_conflict="project_id,url"
        ).execute()
        log.info(f"  ✓ Top sider: {len(data)} sider gemt")
    except Exception as e:
        log.error(f"  Fejl ved gem af top sider: {e}")


def upsert_competitors(supabase: Client, project_id: str, rows: list[dict]):
    if not rows:
        return
    now = datetime.now(timezone.utc).isoformat()
    data = [{"project_id": project_id, "recorded_at": now, **r} for r in rows]
    try:
        supabase.table("ahrefs_competitors").upsert(
            data, on_conflict="project_id,domain"
        ).execute()
        log.info(f"  ✓ Konkurrenter: {len(data)} gemt")
    except Exception as e:
        log.error(f"  Fejl ved gem af konkurrenter: {e}")


def upsert_ahrefs_overview(supabase: Client, project_id: str, overview: dict):
    now = datetime.now(timezone.utc).isoformat()
    try:
        supabase.table("ahrefs_overview").upsert(
            {"project_id": project_id, "updated_at": now, **overview},
            on_conflict="project_id"
        ).execute()
        log.info(f"  ✓ Domain overview gemt (DR: {overview.get('domain_rating')})")
    except Exception as e:
        log.error(f"  Fejl ved gem af ahrefs overview: {e}")


# ─── DataForSEO rankings ─────────────────────────────────────────────────────
def run_dataforseo_for_project(supabase: Client, project: dict, now: str):
    """Henter rankings + søgevolumener for ét projekt."""
    pid      = project["id"]
    domain   = project["domain"]
    location = project.get("location_code") or 2208

    try:
        kws_raw  = supabase.table("keywords").select("id, keyword").eq("project_id", pid).execute().data or []
        keywords = [{"keyword_id": kw["id"], "keyword": kw["keyword"]} for kw in kws_raw]
    except Exception as e:
        log.error(f"  Kan ikke hente keywords: {e}")
        return

    if not keywords:
        log.info("  Ingen keywords — springer over")
        return

    log.info(f"  {len(keywords)} keywords")

    kw_texts   = [kw["keyword"] for kw in keywords]
    volume_map = fetch_search_volumes(kw_texts, location)

    rows  = []
    found = 0
    for idx, kw in enumerate(keywords, 1):
        rank, url = fetch_single_keyword(kw["keyword"], location, domain)
        if rank:
            log.info(f"  [{idx}/{len(keywords)}] ✓ '{kw['keyword']}' → #{rank}")
            found += 1
        else:
            log.info(f"  [{idx}/{len(keywords)}] — '{kw['keyword']}' ikke i top 100")

        rows.append({
            "keyword_id":    kw["keyword_id"],
            "rank":          rank,
            "url":           url,
            "search_volume": volume_map.get(kw["keyword"].lower()),
            "recorded_at":   now,
        })
        time.sleep(KEYWORD_DELAY)

    try:
        supabase.table("rankings_history").insert(rows).execute()
        log.info(f"  ✓ Rankings: {found} placerede, {len(rows)-found} ikke i top 100")
    except Exception as e:
        log.error(f"  Fejl ved gem af rankings: {e}")


# ─── Main ────────────────────────────────────────────────────────────────────
def run():
    log.info("=== SEO Portal Scheduler v8 starter ===")

    dfs_ok    = test_dataforseo()
    ahrefs_ok = test_ahrefs()

    if not dfs_ok:
        log.error("DataForSEO ikke tilgængelig — afbryder")
        sys.exit(1)

    supabase = get_supabase()
    auto_normalize_domains(supabase)

    now = datetime.now(timezone.utc).isoformat()

    try:
        projects = supabase.table("projects").select(
            "id, domain, location_code, client_id"
        ).execute().data or []
    except Exception as e:
        log.error(f"Kan ikke hente projekter: {e}")
        sys.exit(1)

    if not projects:
        log.warning("Ingen projekter — opret kunder i Admin-panelet først")
        sys.exit(0)

    log.info(f"Fundet {len(projects)} projekter")

    for project in projects:
        pid    = project["id"]
        domain = project["domain"]
        log.info(f"\n{'='*50}")
        log.info(f"▶ {domain}")

        log.info("  → DataForSEO rankings...")
        run_dataforseo_for_project(supabase, project, now)

        if not ahrefs_ok:
            log.info("  → Ahrefs springer over (ikke forbundet)")
            continue

        log.info("  → Ahrefs domain overview...")
        overview = fetch_ahrefs_domain_overview(domain)
        if overview:
            upsert_ahrefs_overview(supabase, pid, overview)

        log.info("  → Ahrefs trafik historik...")
        traffic = fetch_ahrefs_traffic_history(domain)
        if traffic:
            upsert_traffic_history(supabase, pid, traffic)

        log.info("  → Ahrefs top sider...")
        top_pages = fetch_ahrefs_top_pages(domain)
        if top_pages:
            upsert_top_pages(supabase, pid, top_pages)

        log.info("  → Ahrefs konkurrenter...")
        competitors = fetch_ahrefs_competitors(domain)
        if competitors:
            upsert_competitors(supabase, pid, competitors)

        time.sleep(1)

    log.info(f"\n=== Scheduler v8 færdig ===")


if __name__ == "__main__":
    run()
