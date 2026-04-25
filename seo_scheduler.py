"""
SEO Portal — Data Scheduler v13
Plutonic Media ApS

Henter fra:
  - DataForSEO: keyword rankings + søgevolumener
  - Ahrefs:     trafik-historik, top sider, konkurrenter, domain overview, site audit

GitHub Secrets påkrævet:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  DATAFORSEO_LOGIN
  DATAFORSEO_PASSWORD
  AHREFS_API_TOKEN

Ændringer v12 → v13:
  - Fix: domain_rating castes til int() — Ahrefs returnerer float (77.0),
         men Supabase-kolonnen er integer → "invalid input syntax for type integer: '77.0'"
         Fix anvendt på både competitors og domain overview for konsistens.
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


# ─── Helpers ─────────────────────────────────────────────────────────────────
def to_int(val) -> int | None:
    """Konverterer float/str til int sikkert — returnerer None hvis None."""
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


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


# ─── Ahrefs helpers ──────────────────────────────────────────────────────────
def ahrefs_headers() -> dict:
    return {
        "Authorization": f"Bearer {AHREFS_TOKEN}",
        "Accept": "application/json",
    }


def today_str() -> str:
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


# ─── Ahrefs Site Explorer ────────────────────────────────────────────────────
def fetch_ahrefs_domain_overview(domain: str) -> dict | None:
    """Henter domain overview: DR, backlinks, referring domains, org traffic, org keywords."""
    try:
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/metrics",
                headers=ahrefs_headers(),
                params={
                    "target": domain,
                    "mode":   "domain",
                    "date":   today_str(),
                    "select": "domain_rating,ahrefs_rank,backlinks,referring_domains,org_keywords,org_traffic",
                },
            )
        if r.status_code != 200:
            log.warning(f"  Ahrefs overview HTTP {r.status_code}")
            log.warning(f"  Response: {r.text[:300]}")
            return None
        metrics = r.json().get("metrics", {}) or {}
        return {
            # FIX: to_int() sikrer at floats (77.0) ikke fejler mod integer-kolonner
            "domain_rating":     to_int(metrics.get("domain_rating")),
            "ahrefs_rank":       to_int(metrics.get("ahrefs_rank")),
            "backlinks":         to_int(metrics.get("backlinks")),
            "referring_domains": to_int(metrics.get("referring_domains")),
            "organic_keywords":  to_int(metrics.get("org_keywords")),
            "organic_traffic":   to_int(metrics.get("org_traffic")),
        }
    except Exception as e:
        log.error(f"  Ahrefs overview fejl for {domain}: {e}")
        return None


def fetch_ahrefs_traffic_history(domain: str) -> list[dict]:
    """Henter månedlig organisk trafik-historik (2 år tilbage)."""
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
                    "select":           "date,org_traffic,org_cost",
                },
            )

        if r.status_code != 200:
            log.warning(f"  Ahrefs traffic history HTTP {r.status_code} for {domain}")
            log.warning(f"  Response: {r.text[:300]}")
            return []

        metrics = r.json().get("metrics", []) or []
        result = []
        for m in metrics:
            raw_date = m.get("date", "")
            if not raw_date:
                continue
            result.append({
                "month":            raw_date[:7] + "-01",
                "organic_traffic":  to_int(m.get("org_traffic")),
                "organic_keywords": None,
                "traffic_value":    to_int(m.get("org_cost")),
            })

        log.info(f"  Trafik historik: {len(result)} måneder hentet")
        return result

    except Exception as e:
        log.error(f"  Ahrefs traffic history fejl for {domain}: {e}")
        return []


def fetch_ahrefs_top_pages(domain: str) -> list[dict]:
    """Henter top 20 sider sorteret efter trafik."""
    try:
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/top-pages",
                headers=ahrefs_headers(),
                params={
                    "target":   domain,
                    "mode":     "domain",
                    "date":     today_str(),
                    "select":   "url,top_keyword,top_keyword_best_position,sum_traffic,keywords,top_keyword_volume",
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
                url_path = urlparse(url).path or "/"
            except Exception:
                url_path = url

            result.append({
                "url":           url_path,
                "top_keyword":   p.get("top_keyword"),
                "position":      to_int(p.get("top_keyword_best_position")),
                "traffic":       to_int(p.get("sum_traffic")),
                "keyword_count": to_int(p.get("keywords")),
                "search_volume": to_int(p.get("top_keyword_volume")),
            })

        log.info(f"  Top sider: {len(result)} hentet")
        return result

    except Exception as e:
        log.error(f"  Ahrefs top pages fejl for {domain}: {e}")
        return []


def fetch_ahrefs_competitors(domain: str) -> list[dict]:
    """Henter organiske konkurrenter (top 10 efter trafik)."""
    try:
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-explorer/organic-competitors",
                headers=ahrefs_headers(),
                params={
                    "target":   domain,
                    "mode":     "domain",
                    "date":     today_str(),
                    "country":  "dk",
                    "select":   "competitor_domain,domain_rating,traffic,keywords_common",
                    "order_by": "traffic:desc",
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
            comp_domain = normalize_domain(comp.get("competitor_domain", ""))
            result.append({
                "domain":          comp_domain,
                # FIX: to_int() — domain_rating kommer som float (77.0) fra API
                "domain_rating":   to_int(comp.get("domain_rating")),
                "organic_traffic": to_int(comp.get("traffic")),
                "common_keywords": to_int(comp.get("keywords_common")),
                "is_self":         comp_domain == normalize_domain(domain),
            })

        log.info(f"  Konkurrenter: {len(result)} hentet")
        return result

    except Exception as e:
        log.error(f"  Ahrefs competitors fejl for {domain}: {e}")
        return []


# ─── Ahrefs Site Audit ───────────────────────────────────────────────────────
def fetch_ahrefs_site_audit(audit_project_id: int) -> dict | None:
    """Henter Site Audit overview + errors for et Ahrefs-projekt."""
    try:
        # 1. Health score + crawl-stats (gratis endpoint)
        with httpx.Client(timeout=30) as c:
            r = c.get(
                f"{AHREFS_BASE}/site-audit/projects",
                headers=ahrefs_headers(),
                params={"project_id": audit_project_id},
            )

        if r.status_code != 200:
            log.warning(f"  Site Audit projects HTTP {r.status_code}")
            log.warning(f"  Response: {r.text[:300]}")
            return None

        healthscores = r.json().get("healthscores", []) or []
        if not healthscores:
            log.warning(f"  Site Audit: ingen data for project_id {audit_project_id}")
            return None

        hs           = healthscores[0]
        health_score = to_int(hs.get("health_score"))
        crawled_urls = to_int(hs.get("total"))
        crawl_status = hs.get("status")

        log.info(f"  Site Audit: health={health_score}, crawled={crawled_urls}, status={crawl_status}")

        # 2. Issues — kun Errors, timeout 60s
        with httpx.Client(timeout=60) as c:
            r2 = c.get(
                f"{AHREFS_BASE}/site-audit/issues",
                headers=ahrefs_headers(),
                params={"project_id": audit_project_id},
            )

        if r2.status_code != 200:
            log.warning(f"  Site Audit issues HTTP {r2.status_code}")
            log.warning(f"  Response: {r2.text[:300]}")
            return {
                "health_score": health_score,
                "crawled_urls": crawled_urls,
                "issues":       [],
            }

        all_issues = r2.json().get("issues", []) or []

        errors = [
            {
                "issue_id": issue.get("issue_id"),
                "name":     issue.get("name"),
                "crawled":  to_int(issue.get("crawled")),
            }
            for issue in all_issues
            if issue.get("importance") == "Error" and (issue.get("crawled") or 0) > 0
        ]
        errors.sort(key=lambda x: x.get("crawled") or 0, reverse=True)

        log.info(f"  Site Audit: {len(errors)} errors fundet")
        return {
            "health_score": health_score,
            "crawled_urls": crawled_urls,
            "issues":       errors,
        }

    except Exception as e:
        log.error(f"  Ahrefs site audit fejl for project {audit_project_id}: {e}")
        return None


# ─── Supabase writes ──────────────────────────────────────────────────────────
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


def insert_site_audit_snapshot(supabase: Client, project_id: str, audit: dict):
    """Gemmer Site Audit snapshot — indsætter altid ny række (historik)."""
    try:
        supabase.table("site_audit_snapshots").insert({
            "project_id":   project_id,
            "health_score": audit["health_score"],
            "crawled_urls": audit["crawled_urls"],
            "issues":       audit["issues"],
        }).execute()
        log.info(f"  ✓ Site Audit snapshot gemt (health: {audit['health_score']}, errors: {len(audit['issues'])})")
    except Exception as e:
        log.error(f"  Fejl ved gem af site audit snapshot: {e}")


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
    log.info("=== SEO Portal Scheduler v13 starter ===")

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
            "id, domain, location_code, client_id, ahrefs_site_audit_id"
        ).execute().data or []
    except Exception as e:
        log.error(f"Kan ikke hente projekter: {e}")
        sys.exit(1)

    if not projects:
        log.warning("Ingen projekter — opret kunder i Admin-panelet først")
        sys.exit(0)

    log.info(f"Fundet {len(projects)} projekter")

    for project in projects:
        pid           = project["id"]
        domain        = project["domain"]
        audit_proj_id = project.get("ahrefs_site_audit_id")

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

        if audit_proj_id:
            log.info(f"  → Ahrefs Site Audit (project {audit_proj_id})...")
            audit = fetch_ahrefs_site_audit(audit_proj_id)
            if audit:
                insert_site_audit_snapshot(supabase, pid, audit)
        else:
            log.info("  → Site Audit springer over (intet ahrefs_site_audit_id)")

        time.sleep(1)

    log.info(f"\n=== Scheduler v13 færdig ===")


if __name__ == "__main__":
    run()
