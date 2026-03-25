from __future__ import annotations

import argparse
import csv
import io
import json
import re
import threading
import xml.etree.ElementTree as ET
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import urljoin, urlparse

import requests
import trafilatura
from bs4 import BeautifulSoup

from whed_excel_export import (
    ALLOWED_US_JURISDICTIONS,
    extract_primary_url,
    is_allowed_country,
    normalize_space,
    parse_txt_file,
)


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0 Safari/537.36"
)
DEFAULT_ENRICHMENT_FILE = "whed_enrichment.jsonl"
COLLEGE_SCORECARD_RECENT_ZIP = (
    "https://ed-public-download.scorecard.network/downloads/"
    "Most-Recent-Cohorts-Institution_10032025.zip"
)

ADMISSION_URL_KEYWORDS = (
    "admission",
    "admissions",
    "apply",
    "application",
    "applicant",
    "entry",
    "prospective",
    "recruit",
    "international-student",
    "international/admissions",
    "enrol",
    "enroll",
    "bewerb",
    "zulass",
    "inscri",
    "admiss",
    "matric",
    "candidat",
)

TUITION_URL_KEYWORDS = (
    "tuition",
    "fees",
    "fee",
    "cost",
    "cost-of-attendance",
    "financial-aid",
    "financial",
    "expenses",
    "prix",
    "tarif",
    "frais",
    "gebuhr",
    "gebuhren",
    "kosten",
    "matricula",
    "honor",
    "oplat",
    "czesne",
    "tax",
)

ADMISSION_TEXT_KEYWORDS = (
    "admission requirement",
    "entry requirement",
    "applicants must",
    "eligible",
    "high school",
    "secondary school",
    "bachelor",
    "master",
    "gpa",
    "sat",
    "act",
    "toefl",
    "ielts",
    "english proficiency",
    "transcript",
    "diploma",
    "matura",
    "baccalaureat",
    "entrance exam",
    "entrance examination",
)

TUITION_TEXT_KEYWORDS = (
    "tuition",
    "fees",
    "cost",
    "cost of attendance",
    "per year",
    "per annum",
    "per semester",
    "per term",
    "per credit",
    "resident",
    "nonresident",
    "international student",
    "financial aid",
    "study cost",
    "academic year",
)

CURRENCY_RE = re.compile(
    r"(?:(?:USD|EUR|CAD|GBP|CHF|SEK|NOK|DKK|PLN|HUF|CZK|RON|BGN|HRK|TRY)\s*)?"
    r"(?:[$EURGBPCHFSEKNOKDKKPLNHUFCZKRONBGNHRKTRY]|EUR|USD|CAD|GBP|CHF|SEK|NOK|DKK|PLN|HUF|CZK|RON|BGN|HRK|TRY)"
    r"\s?\d[\d,.\s]{1,20}",
    flags=re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"\s+")
STATE_ABBREVIATIONS = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

thread_local = threading.local()
scorecard_cache_lock = threading.Lock()
scorecard_cache: dict[str, list[dict[str, object]]] = {}


@dataclass
class InstitutionRecord:
    txt_file: str
    university_name: str
    iau_code: str
    country: str
    city: str
    province: str
    website: str
    admission_requirements: str
    annual_tuition_cost: str


def get_session() -> requests.Session:
    session = getattr(thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
        thread_local.session = session
    return session


def normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value or "").strip()


def clean_domain(value: str) -> str:
    value = extract_primary_url(value)
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    parsed = urlparse(value)
    return parsed.netloc.casefold().removeprefix("www.")


def domains_related(left: str, right: str) -> bool:
    left = clean_domain(left)
    right = clean_domain(right)
    if not left or not right:
        return False
    return left == right or left.endswith(f".{right}") or right.endswith(f".{left}")


def to_homepage(url: str) -> str:
    url = extract_primary_url(url)
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path
    return f"{scheme}://{netloc.strip('/')}/"


def fetch_url(url: str, timeout: int = 20) -> tuple[str, str]:
    session = get_session()
    response = session.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "")
    return response.text, content_type


def load_existing_enrichment(path: Path) -> Dict[str, dict[str, str]]:
    if not path.exists():
        return {}

    records: Dict[str, dict[str, str]] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        iau_code = normalize_space(str(payload.get("iau_code", "")))
        if iau_code:
            records[iau_code] = payload
    return records


def write_enrichment_records(path: Path, records: Dict[str, dict[str, str]]) -> None:
    ordered = sorted(records.items(), key=lambda item: item[0])
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for _, payload in ordered:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def iter_allowed_records(input_dir: Path) -> List[InstitutionRecord]:
    allowed_records: List[InstitutionRecord] = []
    for txt_file in sorted(Path(input_dir).glob("*.txt"), key=lambda item: item.name.casefold()):
        parsed = parse_txt_file(txt_file)
        if not is_allowed_country(parsed.get("Country", "")):
            continue
        allowed_records.append(
            InstitutionRecord(
                txt_file=str(txt_file.resolve()),
                university_name=normalize_space(parsed.get("University Name", "")),
                iau_code=normalize_space(parsed.get("IAU Code", "")),
                country=normalize_space(parsed.get("Country", "")),
                city=normalize_space(parsed.get("City", "")),
                province=normalize_space(parsed.get("Province", "")),
                website=extract_primary_url(parsed.get("Website", "")),
                admission_requirements=normalize_space(parsed.get("Admission Requirements", "")),
                annual_tuition_cost=normalize_space(parsed.get("Annual Tuition / Cost", "")),
            )
        )
    return allowed_records


def download_scorecard_csv(cache_dir: Path) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    zip_path = cache_dir / "Most-Recent-Cohorts-Institution_10032025.zip"
    csv_path = cache_dir / "Most-Recent-Cohorts-Institution.csv"

    if csv_path.exists():
        return csv_path

    if not zip_path.exists():
        response = requests.get(
            COLLEGE_SCORECARD_RECENT_ZIP,
            timeout=120,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        )
        response.raise_for_status()
        zip_path.write_bytes(response.content)

    with zipfile.ZipFile(zip_path) as archive:
        with archive.open("Most-Recent-Cohorts-Institution.csv") as zipped_csv:
            csv_path.write_bytes(zipped_csv.read())

    return csv_path


def load_college_scorecard_records(cache_dir: Path) -> list[dict[str, object]]:
    csv_path = download_scorecard_csv(cache_dir)
    all_records: list[dict[str, object]] = []

    with csv_path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            all_records.append(
                {
                    "id": row.get("UNITID", ""),
                    "school.name": row.get("INSTNM", ""),
                    "school.state": row.get("STABBR", ""),
                    "school.city": row.get("CITY", ""),
                    "school.school_url": row.get("INSTURL", ""),
                    "latest.cost.tuition.in_state": row.get("TUITIONFEE_IN", ""),
                    "latest.cost.tuition.out_of_state": row.get("TUITIONFEE_OUT", ""),
                    "latest.cost.attendance.academic_year": row.get("COSTT4_A", ""),
                }
            )

    return all_records


def get_scorecard_bucket(state_code: str, all_scorecard_records: list[dict[str, object]]) -> list[dict[str, object]]:
    with scorecard_cache_lock:
        cached = scorecard_cache.get(state_code)
        if cached is not None:
            return cached

        bucket = [item for item in all_scorecard_records if item.get("school.state") == state_code]
        scorecard_cache[state_code] = bucket
        return bucket


def normalized_name(value: str) -> str:
    value = value.casefold()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return normalize_text(value)


def score_scorecard_match(record: InstitutionRecord, candidate: dict[str, object]) -> float:
    score = 0.0
    record_name = normalized_name(record.university_name)
    candidate_name = normalized_name(str(candidate.get("school.name", "")))
    score += SequenceMatcher(None, record_name, candidate_name).ratio()

    candidate_domain = clean_domain(str(candidate.get("school.school_url", "")))
    record_domain = clean_domain(record.website)
    if record_domain and candidate_domain:
        if candidate_domain == record_domain:
            score += 1.5
        elif domains_related(candidate_domain, record_domain):
            score += 1.0

    record_city = normalized_name(record.city)
    candidate_city = normalized_name(str(candidate.get("school.city", "")))
    if record_city and candidate_city and record_city == candidate_city:
        score += 0.3

    return score


def find_scorecard_match(
    record: InstitutionRecord,
    all_scorecard_records: list[dict[str, object]],
) -> dict[str, object] | None:
    if not record.country.startswith("United States of America - "):
        return None

    jurisdiction = record.country.removeprefix("United States of America - ").strip()
    if jurisdiction not in ALLOWED_US_JURISDICTIONS:
        return None

    state_code = STATE_ABBREVIATIONS.get(jurisdiction, "")
    if not state_code:
        return None

    candidates = get_scorecard_bucket(state_code, all_scorecard_records)
    best_candidate = None
    best_score = 0.0

    for candidate in candidates:
        score = score_scorecard_match(record, candidate)
        if score > best_score:
            best_candidate = candidate
            best_score = score

    return best_candidate if best_candidate and best_score >= 1.2 else None


def format_currency(value: object, currency_symbol: str = "$") -> str:
    if value in (None, ""):
        return ""
    try:
        amount = int(float(value))
    except (TypeError, ValueError):
        return ""
    return f"{currency_symbol}{amount:,}"


def format_scorecard_tuition(candidate: dict[str, object]) -> str:
    in_state = format_currency(candidate.get("latest.cost.tuition.in_state"))
    out_of_state = format_currency(candidate.get("latest.cost.tuition.out_of_state"))
    attendance = format_currency(candidate.get("latest.cost.attendance.academic_year"))

    parts: list[str] = []
    if in_state and out_of_state:
        if in_state == out_of_state:
            parts.append(f"Tuition {in_state} per academic year")
        else:
            parts.append(f"In-state tuition {in_state}; out-of-state tuition {out_of_state}")
    elif in_state:
        parts.append(f"Tuition {in_state} per academic year")
    elif out_of_state:
        parts.append(f"Tuition {out_of_state} per academic year")

    if attendance:
        parts.append(f"cost of attendance {attendance}")

    if not parts:
        return ""

    return "College Scorecard latest available data: " + "; ".join(parts) + "."


def parse_sitemap_locs(xml_text: str) -> tuple[list[str], bool]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return [], False

    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}", 1)[0] + "}"

    tag = root.tag.removeprefix(namespace).casefold()
    locs = [normalize_text(elem.text or "") for elem in root.findall(f".//{namespace}loc")]
    if tag == "sitemapindex":
        return locs, True
    return locs, False


def parse_robots_for_sitemaps(homepage: str) -> list[str]:
    robots_url = urljoin(homepage, "/robots.txt")
    try:
        body, _ = fetch_url(robots_url, timeout=15)
    except Exception:
        return []

    result = []
    for line in body.splitlines():
        if line.lower().startswith("sitemap:"):
            sitemap = normalize_text(line.split(":", 1)[1])
            if sitemap:
                result.append(sitemap)
    return result


def discover_sitemap_page_urls(homepage: str) -> list[str]:
    sitemap_candidates = parse_robots_for_sitemaps(homepage)
    if not sitemap_candidates:
        sitemap_candidates = [
            urljoin(homepage, "/sitemap.xml"),
            urljoin(homepage, "/sitemap_index.xml"),
        ]

    queue = sitemap_candidates[:8]
    seen_sitemaps: set[str] = set()
    page_urls: list[str] = []

    while queue and len(seen_sitemaps) < 12 and len(page_urls) < 1500:
        sitemap_url = queue.pop(0)
        if sitemap_url in seen_sitemaps:
            continue
        seen_sitemaps.add(sitemap_url)

        try:
            xml_text, content_type = fetch_url(sitemap_url, timeout=20)
        except Exception:
            continue

        if "xml" not in content_type and not xml_text.lstrip().startswith("<"):
            continue

        locs, is_index = parse_sitemap_locs(xml_text)
        if is_index:
            queue.extend(locs[:12])
            continue

        for loc in locs:
            if loc:
                page_urls.append(loc)
                if len(page_urls) >= 1500:
                    break

    return page_urls


def html_keyword_links(homepage: str) -> list[str]:
    try:
        html, content_type = fetch_url(homepage, timeout=20)
    except Exception:
        return []

    if "html" not in content_type:
        return []

    soup = BeautifulSoup(html, "html.parser")
    homepage_domain = clean_domain(homepage)
    links: list[str] = []

    for tag in soup.find_all("a", href=True):
        href = urljoin(homepage, tag["href"])
        parsed = urlparse(href)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not domains_related(href, homepage_domain):
            continue
        href = href.split("#", 1)[0]
        if href.rstrip("/") == homepage.rstrip("/"):
            continue
        text = normalize_text(tag.get_text(" ", strip=True))
        blob = f"{text} {href}".casefold()
        if any(keyword in blob for keyword in ADMISSION_URL_KEYWORDS + TUITION_URL_KEYWORDS):
            links.append(href)

    return links


def score_candidate_url(url: str, kind: str) -> float:
    url_lower = url.casefold()
    keywords = ADMISSION_URL_KEYWORDS if kind == "admission" else TUITION_URL_KEYWORDS
    score = sum(1.0 for keyword in keywords if keyword in url_lower)
    if url_lower.endswith(".pdf"):
        score -= 2.0
    if "blog" in url_lower or "news" in url_lower:
        score -= 1.0
    score += max(0.0, 0.6 - min(len(url_lower), 120) / 300.0)
    return score


def brave_search_urls(homepage: str, university_name: str, kind: str) -> list[str]:
    domain = clean_domain(homepage)
    if not domain:
        return []

    query_terms = "admission requirements" if kind == "admission" else "tuition fees"
    try:
        html, content_type = fetch_url(
            "https://search.brave.com/search?q="
            + requests.utils.quote(f'site:{domain} {query_terms} "{university_name}"'),
            timeout=25,
        )
    except Exception:
        return []

    if "html" not in content_type:
        return []

    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    for tag in soup.select("a[href]"):
        href = normalize_text(tag.get("href", ""))
        if not href.startswith(("http://", "https://")):
            continue
        if not domains_related(href, domain):
            continue
        if href.split("#", 1)[0].rstrip("/") == homepage.rstrip("/"):
            continue
        urls.append(href.split("#", 1)[0])

    ranked = sorted(
        ((score_candidate_url(url, kind), url) for url in dict.fromkeys(urls)),
        reverse=True,
    )
    return [url for _, url in ranked[:5]]


def discover_candidate_urls(homepage: str, university_name: str, kind: str) -> list[str]:
    page_urls = discover_sitemap_page_urls(homepage)
    direct_links = html_keyword_links(homepage)
    brave_links = (
        brave_search_urls(homepage, university_name, kind)
        if len(page_urls) + len(direct_links) < 3
        else []
    )

    combined = list(dict.fromkeys(page_urls + direct_links + brave_links))
    scored = sorted(
        (
            (score_candidate_url(url, kind), url)
            for url in combined
            if score_candidate_url(url, kind) > 0
        ),
        reverse=True,
    )

    return [url for _, url in scored[:4]]


def collapse_trafilatura_text(text: str) -> str:
    text = text.replace("\r", "")
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_main_text(url: str) -> str:
    try:
        html, content_type = fetch_url(url, timeout=25)
    except Exception:
        return ""

    if "html" not in content_type and "<html" not in html[:500].casefold():
        return ""

    text = trafilatura.extract(
        html,
        include_links=False,
        include_formatting=False,
        favor_recall=True,
    )
    return collapse_trafilatura_text(text or "")


def score_text_paragraph(paragraph: str, kind: str) -> float:
    lowered = paragraph.casefold()
    keywords = ADMISSION_TEXT_KEYWORDS if kind == "admission" else TUITION_TEXT_KEYWORDS
    score = sum(1.0 for keyword in keywords if keyword in lowered)

    if kind == "tuition":
        if CURRENCY_RE.search(paragraph):
            score += 2.5
        if any(token in lowered for token in ("calculator", "estimate")) and not CURRENCY_RE.search(paragraph):
            score -= 0.5
    else:
        if any(token in lowered for token in ("essay", "prerequisite", "transcript", "toefl", "ielts")):
            score += 0.5
        if any(
            token in lowered
            for token in (
                "as a student",
                "my experience",
                "i truly",
                "i am proud",
                "learning experience",
                "testimonial",
            )
        ):
            score -= 2.0

    if len(paragraph) < 40:
        score -= 1.0
    if len(paragraph) > 900:
        score -= 1.5

    if "cookie" in lowered or "privacy" in lowered:
        score -= 2.0

    return score


def shorten_summary(text: str, max_chars: int = 650) -> str:
    text = normalize_text(text)
    if len(text) <= max_chars:
        return text
    cutoff = text.rfind(". ", 0, max_chars)
    if cutoff > max_chars * 0.6:
        return text[: cutoff + 1]
    return text[:max_chars].rsplit(" ", 1)[0] + "..."


def extract_summary_from_text(text: str, kind: str) -> str:
    if not text:
        return ""

    paragraphs = [normalize_text(part) for part in re.split(r"\n{2,}", text) if normalize_text(part)]
    scored = sorted(
        ((score_text_paragraph(paragraph, kind), paragraph) for paragraph in paragraphs),
        reverse=True,
    )

    chosen: list[str] = []
    seen: set[str] = set()
    for score, paragraph in scored:
        lowered = paragraph.casefold()
        if score < 1.0:
            continue
        if kind == "tuition" and "tuition" not in lowered and "fee" not in lowered and "cost" not in lowered:
            continue
        if kind == "admission" and not any(keyword in lowered for keyword in ADMISSION_TEXT_KEYWORDS):
            continue
        if kind == "admission" and not any(
            token in lowered
            for token in (
                "require",
                "must",
                "submit",
                "diploma",
                "certificate",
                "gpa",
                "toefl",
                "ielts",
                "sat",
                "act",
                "transcript",
                "exam",
            )
        ):
            continue
        key = lowered[:120]
        if key in seen:
            continue
        chosen.append(paragraph)
        seen.add(key)
        if len(chosen) >= 2:
            break

    return shorten_summary(" ".join(chosen))


def scrape_site_summary(homepage: str, university_name: str, kind: str) -> tuple[str, str]:
    for candidate_url in discover_candidate_urls(homepage, university_name, kind):
        text = fetch_main_text(candidate_url)
        summary = extract_summary_from_text(text, kind)
        if summary:
            return summary, candidate_url
    return "", ""


def build_payload(
    record: InstitutionRecord,
    admission_value: str,
    tuition_value: str,
    admission_source: str,
    tuition_source: str,
) -> dict[str, str]:
    return {
        "iau_code": record.iau_code,
        "university_name": record.university_name,
        "country": record.country,
        "website": record.website,
        "admission_requirements": admission_value,
        "annual_tuition_cost": tuition_value,
        "admission_source_url": admission_source,
        "tuition_source_url": tuition_source,
    }


def enrich_record(
    record: InstitutionRecord,
    existing_payload: dict[str, str] | None,
    scorecard_records: list[dict[str, object]],
) -> dict[str, str]:
    existing_payload = existing_payload or {}
    admission_value = normalize_space(existing_payload.get("admission_requirements", "")) or record.admission_requirements
    tuition_value = normalize_space(existing_payload.get("annual_tuition_cost", "")) or record.annual_tuition_cost
    admission_source = normalize_space(existing_payload.get("admission_source_url", ""))
    tuition_source = normalize_space(existing_payload.get("tuition_source_url", ""))

    if not tuition_value:
        scorecard_match = find_scorecard_match(record, scorecard_records)
        if scorecard_match:
            tuition_value = format_scorecard_tuition(scorecard_match)
            tuition_source = "https://collegescorecard.ed.gov/"

    homepage = to_homepage(record.website)
    if homepage:
        if not admission_value:
            admission_value, admission_source = scrape_site_summary(
                homepage,
                record.university_name,
                "admission",
            )
        if not tuition_value:
            tuition_value, tuition_source = scrape_site_summary(
                homepage,
                record.university_name,
                "tuition",
            )

    return build_payload(record, admission_value, tuition_value, admission_source, tuition_source)


def needs_refresh(record: InstitutionRecord, existing_payload: dict[str, str] | None, refresh: bool) -> bool:
    if refresh:
        return True
    if existing_payload is None:
        return not (record.admission_requirements and record.annual_tuition_cost)
    has_admission = normalize_space(existing_payload.get("admission_requirements", "")) or record.admission_requirements
    has_tuition = normalize_space(existing_payload.get("annual_tuition_cost", "")) or record.annual_tuition_cost
    return not (has_admission and has_tuition)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich allowed WHED institutions with admissions requirements and annual cost."
    )
    parser.add_argument("--input-dir", default="Data", help="Folder that contains WHED TXT files.")
    parser.add_argument(
        "--output-file",
        default=DEFAULT_ENRICHMENT_FILE,
        help="JSONL file that stores the enrichment results.",
    )
    parser.add_argument("--workers", type=int, default=12, help="Number of parallel workers.")
    parser.add_argument("--limit", type=int, default=0, help="Only process the first N allowed institutions.")
    parser.add_argument("--refresh", action="store_true", help="Rebuild existing enrichment records.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input_dir)
    output_file = Path(args.output_file)

    records = iter_allowed_records(input_dir)
    if args.limit > 0:
        records = records[: args.limit]

    existing = load_existing_enrichment(output_file)
    scorecard_records = load_college_scorecard_records(Path(".cache"))

    pending = [record for record in records if needs_refresh(record, existing.get(record.iau_code), args.refresh)]
    print(
        f"[info] Loaded {len(records)} allowed institutions, {len(existing)} existing enrichment rows, "
        f"{len(pending)} row(s) need processing.",
        flush=True,
    )

    completed = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {
            executor.submit(enrich_record, record, existing.get(record.iau_code), scorecard_records): record
            for record in pending
        }

        for future in as_completed(future_map):
            record = future_map[future]
            try:
                payload = future.result()
            except Exception as exc:
                payload = build_payload(record, record.admission_requirements, record.annual_tuition_cost, "", "")
                payload["error"] = normalize_text(str(exc))

            existing[record.iau_code] = payload
            completed += 1
            if completed % 25 == 0 or completed == len(pending):
                write_enrichment_records(output_file, existing)
                admission_count = sum(
                    1 for item in existing.values() if normalize_space(item.get("admission_requirements", ""))
                )
                tuition_count = sum(
                    1 for item in existing.values() if normalize_space(item.get("annual_tuition_cost", ""))
                )
                print(
                    f"[progress] {completed}/{len(pending)} processed | "
                    f"admission filled: {admission_count} | tuition filled: {tuition_count}",
                    flush=True,
                )

    write_enrichment_records(output_file, existing)
    print(f"[done] Wrote enrichment file to {output_file.resolve()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
