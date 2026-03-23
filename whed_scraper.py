import argparse
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.chrome.options import Options


BASE_URL = "https://www.whed.net/"
HOME_URL = urljoin(BASE_URL, "home.php")
RESULTS_URL = urljoin(BASE_URL, "results_institutions.php")
DETAIL_PATH_TOKEN = "detail_institution.php"

CHALLENGE_MARKERS = (
    "just a moment",
    "challenge-platform",
    "cf-chl",
    "cf-browser-verification",
    "cloudflare",
)
IAU_ID_RE = re.compile(r"IAU-\d+")
INVALID_FILENAME_RE = re.compile(r'[<>:"/\\|?*]+')
WHITESPACE_RE = re.compile(r"\s+")


def normalize_space(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value or "").strip()


def dedupe_consecutive(lines: List[str]) -> List[str]:
    cleaned: List[str] = []
    for line in lines:
        if not cleaned or cleaned[-1] != line:
            cleaned.append(line)
    return cleaned


def sanitize_filename(name: str) -> str:
    clean = normalize_space(name)
    clean = INVALID_FILENAME_RE.sub("_", clean)
    clean = clean.rstrip(". ")
    return clean[:180].rstrip() or "institution"


class WHEDScraper:
    def __init__(
        self,
        data_dir: Path,
        headless: bool = False,
        include_subregions: bool = False,
        selected_countries: Optional[List[str]] = None,
        max_countries: Optional[int] = None,
        limit_per_country: Optional[int] = None,
        request_delay: float = 0.75,
        verify_timeout: int = 600,
    ) -> None:
        self.data_dir = data_dir
        self.headless = headless
        self.include_subregions = include_subregions
        self.selected_countries = [normalize_space(item).casefold() for item in selected_countries or []]
        self.max_countries = max_countries
        self.limit_per_country = limit_per_country
        self.request_delay = request_delay
        self.verify_timeout = verify_timeout

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.existing_ids = self.load_existing_ids()

        self.driver = self.build_driver(headless=headless)
        self.wait = WebDriverWait(self.driver, 30)
        self.session = requests.Session()
        self.user_agent = ""

    def load_existing_ids(self) -> set[str]:
        ids: set[str] = set()
        for path in self.data_dir.glob("*.txt"):
            match = IAU_ID_RE.search(path.name)
            if match:
                ids.add(match.group(0))
        return ids

    def build_driver(self, headless: bool) -> webdriver.Chrome:
        options = Options()
        if headless:
            options.add_argument("--headless=new")

        options.add_argument("--window-size=1600,1200")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-infobars")
        options.add_argument("--lang=en-US")
        options.page_load_strategy = "eager"
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=options)

        try:
            driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
""".strip()
                },
            )
        except Exception:
            pass

        driver.set_page_load_timeout(90)
        return driver

    def log(self, message: str) -> None:
        print(message, flush=True)

    def page_is_challenge(self, html: str, title: str = "") -> bool:
        snippet = f"{title}\n{html[:4000]}".lower()
        return any(marker in snippet for marker in CHALLENGE_MARKERS)

    def sync_session_from_browser(self) -> None:
        self.user_agent = self.driver.execute_script("return navigator.userAgent;")
        self.session.headers.update(
            {
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": HOME_URL,
            }
        )

        for cookie in self.driver.get_cookies():
            self.session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path"),
            )

    def ensure_home_ready(self, reason: str) -> None:
        self.log(f"[manual] {reason}")
        self.log("[manual] Chrome will stay open. If WHED shows a verification page, solve it once and the scraper will continue.")

        self.driver.get(HOME_URL)
        end_time = time.time() + self.verify_timeout
        last_hint = 0.0

        while time.time() < end_time:
            try:
                if self.driver.find_elements(By.ID, "pays"):
                    self.sync_session_from_browser()
                    return
            except WebDriverException:
                pass

            if time.time() - last_hint >= 15:
                title = self.driver.title or "(no title yet)"
                self.log(f"[manual] Waiting for WHED home page... current title: {title}")
                last_hint = time.time()

            time.sleep(1)

        raise TimeoutException("Timed out while waiting for the WHED home page to become available.")

    def wait_for_results_page(self) -> None:
        def ready(driver: webdriver.Chrome) -> bool:
            try:
                if self.page_is_challenge(driver.page_source, driver.title):
                    return False

                body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                if "results found" in body_text:
                    return True
                if "no results found" in body_text:
                    return True
                if "no matching records" in body_text:
                    return True

                return bool(driver.find_elements(By.CSS_SELECTOR, f'a[href*="{DETAIL_PATH_TOKEN}"]'))
            except WebDriverException:
                return False

        WebDriverWait(self.driver, 60).until(ready)

    def wait_for_detail_page(self) -> None:
        def ready(driver: webdriver.Chrome) -> bool:
            try:
                if self.page_is_challenge(driver.page_source, driver.title):
                    return False

                body_text = driver.find_element(By.TAG_NAME, "body").text
                return "General Information" in body_text or bool(IAU_ID_RE.search(body_text))
            except WebDriverException:
                return False

        WebDriverWait(self.driver, 60).until(ready)

    def is_nested_option(self, raw_text: str) -> bool:
        if not raw_text:
            return False

        trimmed = raw_text.lstrip(" \t\r\n-\xa0")
        return trimmed != raw_text

    def get_countries(self) -> List[Dict[str, str]]:
        self.ensure_home_ready("Preparing browser session and reading country list.")

        select = Select(self.wait.until(EC.presence_of_element_located((By.ID, "pays"))))
        countries: List[Dict[str, str]] = []

        for option in select.options:
            value = normalize_space(option.get_attribute("value") or "")
            raw_text = option.get_attribute("textContent") or option.text or ""
            label = normalize_space(raw_text)

            if not value or not label or label.casefold() == "all countries":
                continue

            if not self.include_subregions and self.is_nested_option(raw_text):
                continue

            countries.append({"value": value, "label": label})

        if self.selected_countries:
            countries = [
                country
                for country in countries
                if country["value"].casefold() in self.selected_countries
                or country["label"].casefold() in self.selected_countries
            ]

        if self.max_countries is not None:
            countries = countries[: self.max_countries]

        return countries

    def response_needs_verification(self, status_code: int, text: str) -> bool:
        if status_code >= 400:
            return True
        return self.page_is_challenge(text)

    def get_html_with_session(self, url: str) -> str:
        for attempt in range(1, 4):
            try:
                response = self.session.get(url, timeout=90)
                if not self.response_needs_verification(response.status_code, response.text):
                    time.sleep(self.request_delay)
                    return response.text

                self.log(f"[retry] Browser verification was requested while reading: {url}")
            except RequestException as exc:
                self.log(f"[retry] Session request failed ({attempt}/3) for {url}: {exc}")

            self.ensure_home_ready("Session needs fresh verification/cookies.")

        self.log(f"[fallback] Session fetch failed, opening detail page in browser: {url}")
        for attempt in range(1, 4):
            self.driver.get(url)
            try:
                self.wait_for_detail_page()
                self.sync_session_from_browser()
                return self.driver.page_source
            except TimeoutException:
                if attempt == 3:
                    raise
                self.ensure_home_ready("Browser verification is required before opening a detail page.")

        raise RuntimeError(f"Could not load detail page in browser: {url}")

    def set_max_results_per_page(self) -> None:
        selects = self.driver.find_elements(By.TAG_NAME, "select")
        for element in selects:
            try:
                control = Select(element)
                options = [normalize_space(option.text) for option in control.options]
                numeric_options = sorted({int(item) for item in options if item.isdigit()})
                if not numeric_options:
                    continue

                target = str(numeric_options[-1])
                if normalize_space(control.first_selected_option.text) == target:
                    return

                before = self.current_page_signature()
                control.select_by_visible_text(target)
                WebDriverWait(self.driver, 20).until(lambda _: self.current_page_signature() != before)
                time.sleep(1)
                return
            except (NoSuchElementException, StaleElementReferenceException, TimeoutException, WebDriverException):
                continue

    def current_page_signature(self) -> str:
        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, f'a[href*="{DETAIL_PATH_TOKEN}"]')
            hrefs = []
            sample = links[:3] + links[-3:] if len(links) > 6 else links
            for element in sample:
                href = element.get_attribute("href") or ""
                if href:
                    hrefs.append(href)

            if hrefs:
                return f"{len(links)}|{'|'.join(hrefs)}"

            body = self.driver.find_element(By.TAG_NAME, "body").text
            return normalize_space(body[:500])
        except WebDriverException:
            return str(time.time())

    def next_page(self) -> bool:
        candidates = self.driver.find_elements(
            By.XPATH,
            (
                "//*[self::a or self::button or self::input]"
                "[contains(normalize-space(.), 'Next page')"
                " or normalize-space(@value)='Next page']"
            ),
        )

        for element in candidates:
            try:
                classes = (element.get_attribute("class") or "").lower()
                disabled = element.get_attribute("disabled")
                href = element.get_attribute("href")

                if disabled or "disabled" in classes:
                    continue

                if element.tag_name.lower() == "a" and not href:
                    continue

                before = self.current_page_signature()
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                self.driver.execute_script("arguments[0].click();", element)
                WebDriverWait(self.driver, 30).until(lambda _: self.current_page_signature() != before)
                self.wait_for_results_page()
                return True
            except (StaleElementReferenceException, TimeoutException, WebDriverException):
                continue

        return False

    def parse_results_page(self, html: str, country_label: str) -> List[Dict[str, Optional[str]]]:
        soup = BeautifulSoup(html, "html.parser")
        institutions: List[Dict[str, Optional[str]]] = []
        seen_urls: set[str] = set()

        for anchor in soup.select(f'a[href*="{DETAIL_PATH_TOKEN}"]'):
            name = normalize_space(anchor.get_text(" ", strip=True))
            href = anchor.get("href") or ""

            if not name or name.lower() == "image" or not href:
                continue

            url = urljoin(BASE_URL, href)
            if url in seen_urls:
                continue

            iau_id = None
            previous_text = anchor.find_previous(string=IAU_ID_RE)
            if previous_text:
                match = IAU_ID_RE.search(str(previous_text))
                if match:
                    iau_id = match.group(0)

            institutions.append(
                {
                    "name": name,
                    "url": url,
                    "iau_id": iau_id,
                    "country": country_label,
                }
            )
            seen_urls.add(url)

        return institutions

    def collect_country_institutions(self, country: Dict[str, str]) -> List[Dict[str, Optional[str]]]:
        encoded_country = quote_plus(country["value"])
        url = f"{RESULTS_URL}?Chp1={encoded_country}"

        self.log(f"[country] Opening results for {country['label']}")
        for attempt in range(1, 4):
            self.driver.get(url)
            try:
                self.wait_for_results_page()
                break
            except TimeoutException:
                if attempt == 3:
                    raise
                self.ensure_home_ready("Browser verification is required before loading a country results page.")

        self.set_max_results_per_page()

        institutions: Dict[str, Dict[str, Optional[str]]] = {}
        visited_signatures: set[str] = set()

        while True:
            signature = self.current_page_signature()
            if signature in visited_signatures:
                break

            visited_signatures.add(signature)
            page_items = self.parse_results_page(self.driver.page_source, country["label"])
            for item in page_items:
                institutions[item["url"]] = item

            if self.limit_per_country is not None and len(institutions) >= self.limit_per_country:
                break

            if not self.next_page():
                break

        items = list(institutions.values())
        if self.limit_per_country is not None:
            items = items[: self.limit_per_country]

        self.log(f"[country] {country['label']}: found {len(items)} institution link(s)")
        return items

    def clean_detail_text(self, html: str, source_url: str) -> Dict[str, Optional[str]]:
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        title = normalize_space(soup.title.get_text(" ", strip=True) if soup.title else "")
        if " - WHED" in title:
            title = title.split(" - WHED", 1)[0].strip()

        body = soup.body or soup
        lines = []
        for raw_line in body.get_text("\n").splitlines():
            line = normalize_space(raw_line)
            if not line:
                continue
            if line in {"IAU Website", "UNESCO Website", "top"}:
                continue
            if line.lower() == "image":
                continue
            if line.startswith("© ") or line.startswith("Â© "):
                continue
            lines.append(line)

        lines = dedupe_consecutive(lines)
        text = "\n".join(lines).strip()

        iau_match = IAU_ID_RE.search(text)
        iau_id = iau_match.group(0) if iau_match else None

        permalink = ""
        permalink_anchor = soup.select_one('a[href*="/institutions/IAU-"]')
        if permalink_anchor and permalink_anchor.get("href"):
            permalink = permalink_anchor["href"]

        header_lines = [f"Source URL: {source_url}"]
        if permalink:
            header_lines.append(f"Permanent URL: {permalink}")

        content = "\n".join(header_lines)
        if text:
            content = f"{content}\n\n{text}"

        return {"title": title, "iau_id": iau_id, "content": content}

    def output_path_for(self, institution: Dict[str, Optional[str]], detail: Dict[str, Optional[str]]) -> Path:
        name = detail["title"] or institution["name"] or "institution"
        iau_id = detail["iau_id"] or institution.get("iau_id")

        if iau_id:
            base_name = f"{name} [{iau_id}]"
        else:
            country = institution.get("country") or "Unknown Country"
            base_name = f"{country} - {name}"

        path = self.data_dir / f"{sanitize_filename(base_name)}.txt"
        if not path.exists():
            return path

        counter = 2
        while True:
            candidate = self.data_dir / f"{sanitize_filename(base_name)} ({counter}).txt"
            if not candidate.exists():
                return candidate
            counter += 1

    def save_institution(self, institution: Dict[str, Optional[str]]) -> Optional[Path]:
        known_id = institution.get("iau_id")
        if known_id and known_id in self.existing_ids:
            self.log(f"[skip] {known_id} already exists in Data")
            return None

        html = self.get_html_with_session(institution["url"])
        detail = self.clean_detail_text(html, institution["url"])

        final_id = detail["iau_id"] or known_id
        if final_id and final_id in self.existing_ids:
            self.log(f"[skip] {final_id} already exists in Data")
            return None

        path = self.output_path_for(institution, detail)
        path.write_text(detail["content"], encoding="utf-8")

        if final_id:
            self.existing_ids.add(final_id)

        self.log(f"[save] {path.name}")
        return path

    def run(self) -> None:
        countries = self.get_countries()
        if not countries:
            raise RuntimeError("No country options were collected from the WHED home page.")

        self.log(f"[info] Country/option count to scrape: {len(countries)}")
        self.log(f"[info] Output folder: {self.data_dir.resolve()}")

        saved = 0
        skipped = 0

        for index, country in enumerate(countries, start=1):
            self.log(f"\n=== [{index}/{len(countries)}] {country['label']} ===")
            institutions = self.collect_country_institutions(country)

            for item_index, institution in enumerate(institutions, start=1):
                self.log(
                    f"[item] {country['label']} {item_index}/{len(institutions)}"
                    f" -> {institution['name']}"
                )
                result = self.save_institution(institution)
                if result is None:
                    skipped += 1
                else:
                    saved += 1

        self.log(f"\n[done] Saved {saved} new file(s), skipped {skipped} existing file(s).")

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass
        self.session.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape WHED HEI entries into individual TXT files inside the Data directory. "
            "Chrome is opened in normal mode by default because WHED commonly blocks headless access."
        )
    )
    parser.add_argument("--data-dir", default="Data", help="Folder where TXT files will be written.")
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Try headless mode. WHED may block this with a verification page.",
    )
    parser.add_argument(
        "--include-subregions",
        action="store_true",
        help="Include indented subdivision options from the country dropdown.",
    )
    parser.add_argument(
        "--country",
        action="append",
        help="Scrape only the given country/option value. Can be repeated.",
    )
    parser.add_argument(
        "--max-countries",
        type=int,
        help="Limit how many country options are processed. Useful for test runs.",
    )
    parser.add_argument(
        "--limit-per-country",
        type=int,
        help="Limit how many institutions are saved for each country. Useful for test runs.",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.75,
        help="Seconds to wait between detail-page HTTP requests.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    scraper = WHEDScraper(
        data_dir=Path(args.data_dir),
        headless=args.headless,
        include_subregions=args.include_subregions,
        selected_countries=args.country,
        max_countries=args.max_countries,
        limit_per_country=args.limit_per_country,
        request_delay=args.request_delay,
    )

    try:
        scraper.run()
        return 0
    except KeyboardInterrupt:
        print("\n[stop] Interrupted by user.", flush=True)
        return 130
    except Exception as exc:
        print(f"\n[error] {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        scraper.close()


if __name__ == "__main__":
    raise SystemExit(main())
