from __future__ import annotations

import asyncio
import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Iterable

try:
    from googletrans import Translator as GoogleTranslator
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency 'googletrans'. Install it with: python -m pip install googletrans==4.0.2"
    ) from exc


PRESERVE_PATHS = {
    ("university_details", "code"),
    ("data", "general_information", "university_name"),
    ("data", "general_information", "native_name"),
    ("data", "general_information", "country"),
    ("data", "divisions", "division_name"),
    ("data", "divisions", "more_details"),
    ("data", "degree_fields", "degree_field_title"),
    ("data", "location_information", "street"),
    ("data", "location_information", "city"),
    ("data", "location_information", "province"),
    ("data", "location_information", "post_code"),
    ("data", "location_information", "full_address"),
    ("data", "contact_information", "website"),
    ("data", "contact_information", "contact_page"),
    ("data", "contact_information", "email"),
    ("data", "contact_information", "phone"),
    ("data", "contact_information", "phone_standardized"),
    ("data", "officers", "name"),
}

COMMA_LIST_PATHS = {
    ("data", "divisions", "fields_of_study"),
    ("data", "degree_fields", "degree_field_subjects"),
    ("data", "classification_information", "bachelors_degree"),
    ("data", "classification_information", "masters_degree"),
    ("data", "classification_information", "doctors_degree"),
    ("data", "classification_information", "diploma_certificate"),
}

SEMICOLON_LIST_PATHS = {
    ("data", "general_information", "languages"),
}

DO_NOT_TRANSLATE_KEYS = {
    "code",
    "file",
    "source_file",
    "state",
}

NO_TRANSLATE_REPORT_ITEMS = [
    (
        "university_details.code",
        "Identifier code; machine-readable value.",
    ),
    (
        "data.general_information.university_name",
        "Official institution name; proper noun.",
    ),
    (
        "data.general_information.native_name",
        "Official native-language institution name; proper noun.",
    ),
    (
        "data.general_information.country",
        "Original location string from source data; kept as-is to avoid translating place names.",
    ),
    (
        "data.divisions.division_name",
        "Official division / faculty / school name; treated as an institutional proper name.",
    ),
    (
        "data.divisions.more_details",
        "Often contains campus names, person names, abbreviations, or location notes; kept as-is.",
    ),
    (
        "data.degree_fields.degree_field_title",
        "Formal degree title from source data; preserved to avoid mistranslating official award names.",
    ),
    (
        "data.location_information.street",
        "Postal address component; should stay in original form.",
    ),
    (
        "data.location_information.city",
        "City name; proper location name.",
    ),
    (
        "data.location_information.province",
        "State/province/region name; proper location name.",
    ),
    (
        "data.location_information.post_code",
        "Postal code; identifier value.",
    ),
    (
        "data.location_information.full_address",
        "Full postal address; should stay in original form.",
    ),
    (
        "data.contact_information.website",
        "URL; locator value.",
    ),
    (
        "data.contact_information.contact_page",
        "URL; locator value.",
    ),
    (
        "data.contact_information.email",
        "Email address; locator value.",
    ),
    (
        "data.contact_information.phone",
        "Phone number; locator value.",
    ),
    (
        "data.contact_information.phone_standardized",
        "Standardized phone number; locator value.",
    ),
    (
        "data.officers.name",
        "Person name; proper noun.",
    ),
    (
        "manifest[].state",
        "State label in generated manifest files; proper location name.",
    ),
]

PARTIAL_TRANSLATION_REPORT_ITEMS = [
    (
        "data.contact_information.key_contacts",
        "Person names stay original; titles inside parentheses are translated.",
    ),
    (
        "data.officers.role",
        "Role labels are translated; person names are preserved.",
    ),
    (
        "data.officers.job_title",
        "Job titles are translated; person names are preserved separately.",
    ),
]

PATTERN_BASED_REPORT_ITEMS = [
    (
        "URLs",
        "Any full URL value is preserved.",
    ),
    (
        "Emails",
        "Any full email value is preserved.",
    ),
    (
        "Codes",
        "IAU/WHED-style identifiers are preserved.",
    ),
    (
        "Numeric-only values",
        "Pure numeric / score / percentage-like strings are preserved.",
    ),
    (
        "File names and filesystem paths",
        "File names, manifest file references, and local paths are preserved.",
    ),
]

REVIEW_CANDIDATE_REPORT_ITEMS = [
    (
        "data.general_information.history",
        "Usually safe to translate, but may contain former official institution names inside the sentence.",
    ),
]

KNOWN_TEXT_FIXES = {
    "Baden-WÃ¼rttemberg": "Baden-Württemberg",
}

MOJIBAKE_REPLACEMENTS = {
    "\u00c3\u00bc": "\u00fc",
    "\u00c3\u009c": "\u00dc",
    "\u00c3\u00b6": "\u00f6",
    "\u00c3\u2013": "\u00d6",
    "\u00c3\u00a7": "\u00e7",
    "\u00c3\u2021": "\u00c7",
    "\u00c4\u00b1": "\u0131",
    "\u00c4\u00b0": "\u0130",
    "\u00c5\u015f": "\u015f",
    "\u00c5\u017d": "\u015e",
    "\u00c4\u0178": "\u011f",
    "\u00c4\u017d": "\u011e",
    "\u00c3\u00a2": "\u00e2",
    "\u00c3\u00ae": "\u00ee",
    "\u00c3\u00bb": "\u00fb",
    "Ba\u015ekan": "Ba\u015fkan",
    "Birle\u015eik": "Birle\u015fik",
    "A\u015ea\u011f\u0131": "A\u015fa\u011f\u0131",
}

EXACT_TRANSLATIONS = {
    "Private": "Özel",
    "Private-for-profit": "Kâr amaçlı özel",
    "Public": "Devlet",
    "co-ed": "Karma",
    "female": "Kadın",
    "male": "Erkek",
    "Yes": "Evet",
    "No": "Hayır",
    "city": "şehir",
    "country": "ülke",
    "region": "bölge",
    "Head": "Başkan",
    "Senior Administrative Officer": "Kıdemli İdari Yetkili",
    "International Relations Officer": "Uluslararası İlişkiler Sorumlusu",
    "President": "Başkan",
    "Vice President": "Başkan Yardımcısı",
    "Director": "Direktör",
    "Administrative Assistant": "İdari Asistan",
    "Mixed": "Karışık",
    "Challenging": "Zorlayıcı",
    "Not student-friendly": "Öğrenci dostu değil",
    "Student-friendly": "Öğrenci dostu",
    "Very student-friendly": "Çok öğrenci dostu",
    "Academy": "Akademi",
    "Area": "Alan",
    "Board of Study": "Çalışma Kurulu",
    "Bureau": "Büro",
    "Campus": "Kampüs",
    "Campus Abroad": "Yurt Dışı Kampüs",
    "Centre": "Merkez",
    "Chair": "Kürsü",
    "College": "Kolej",
    "Conservatory": "Konservatuvar",
    "Course/Programme": "Kurs/Program",
    "Deanery": "Dekanlık",
    "Department/Division": "Bölüm/Birim",
    "Faculty": "Fakülte",
    "Foundation": "Vakıf",
    "Graduate School": "Lisansüstü Okul",
    "Group": "Grup",
    "Higher Vocational School": "Yüksek Meslek Okulu",
    "Institute": "Enstitü",
    "Intermediate Institute": "Ara Enstitü",
    "Laboratory": "Laboratuvar",
    "Research Division": "Araştırma Birimi",
    "School": "Okul",
    "They include": "Bunlara şunlar dahildir",
    "UNESCO Chair and Network": "UNESCO Kürsüsü ve Ağı",
    "Unit": "Birim",
    "http": "http",
    "https": "https",
    "Bachelor's Degree": "Lisans",
    "Bachelor’s Degree": "Lisans",
    "Master's Degree": "Yüksek Lisans",
    "Doctor's Degree": "Doktora",
    "Diploma/Certificate": "Diploma/Sertifika",
    "Cold desert climate": "Soğuk çöl iklimi",
    "Cold semi-arid climate": "Soğuk yarı kurak iklim",
    "Hot desert climate": "Sıcak çöl iklimi",
    "Hot semi-arid climate": "Sıcak yarı kurak iklim",
    "Hot-summer Mediterranean climate": "Sıcak yazlı Akdeniz iklimi",
    "Hot-summer humid continental climate": "Sıcak yazlı nemli karasal iklim",
    "Humid subtropical climates": "Nemli subtropikal iklim",
    "Mediterranean-influenced subarctic climate": "Akdeniz etkili subarktik iklim",
    "Monsoon-influenced hot-summer humid continental climate": "Muson etkili sıcak yazlı nemli karasal iklim",
    "Monsoon-influenced warm-summer humid continental climate": "Muson etkili ılık yazlı nemli karasal iklim",
    "Subarctic climate": "Subarktik iklim",
    "Temperate oceanic climate": "Ilıman okyanusal iklim",
    "Tropical monsoon climate": "Tropikal muson iklimi",
    "Tropical rainforest climate": "Tropikal yağmur ormanı iklimi",
    "Tropical savanna climate with dry-summer characteristics": "Yazı kurak tropikal savan iklimi",
    "Tropical savanna climate with dry-winter characteristics": "Kışı kurak tropikal savan iklimi",
    "Tundra climate": "Tundra iklimi",
    "Warm-summer Mediterranean climate": "Ilık yazlı Akdeniz iklimi",
    "Warm-summer humid continental climate": "Ilık yazlı nemli karasal iklim",
}

COUNTRY_TRANSLATIONS = {
    "Austria": "Avusturya",
    "Belgium": "Belçika",
    "Bulgaria": "Bulgaristan",
    "Canada": "Kanada",
    "Croatia": "Hırvatistan",
    "Cyprus": "Kıbrıs",
    "Czechia": "Çekya",
    "Denmark": "Danimarka",
    "Estonia": "Estonya",
    "Finland": "Finlandiya",
    "France": "Fransa",
    "Germany": "Almanya",
    "Greece": "Yunanistan",
    "Hungary": "Macaristan",
    "Ireland": "İrlanda",
    "Italy": "İtalya",
    "Latvia": "Letonya",
    "Lithuania": "Litvanya",
    "Luxembourg": "Lüksemburg",
    "Malta": "Malta",
    "Netherlands": "Hollanda",
    "Poland": "Polonya",
    "Portugal": "Portekiz",
    "Romania": "Romanya",
    "Slovak Republic": "Slovakya",
    "Slovenia": "Slovenya",
    "Spain": "İspanya",
    "Sweden": "İsveç",
    "United States of America": "Amerika Birleşik Devletleri",
}

LANGUAGE_TRANSLATIONS = {
    "Arabic": "Arapça",
    "Basque": "Baskça",
    "Belarusian": "Belarusça",
    "Bosnian": "Boşnakça",
    "Bulgarian": "Bulgarca",
    "Cambodian": "Kmerce",
    "Catalan": "Katalanca",
    "Chinese": "Çince",
    "Croatian": "Hırvatça",
    "Czech": "Çekçe",
    "Danish": "Danca",
    "Dutch": "Felemenkçe",
    "Dzongkha": "Dzongkha",
    "English": "İngilizce",
    "Estonian": "Estonca",
    "Finnish": "Fince",
    "French": "Fransızca",
    "Galician": "Galiçyaca",
    "German": "Almanca",
    "Greek": "Yunanca",
    "Hebrew": "İbranice",
    "Hungarian": "Macarca",
    "Italian": "İtalyanca",
    "Korean": "Korece",
    "Latin": "Latince",
    "Latvian": "Letonca",
    "Lithuanian": "Litvanca",
    "Maltese": "Maltaca",
    "Polish": "Lehçe",
    "Portuguese": "Portekizce",
    "Romanian": "Romence",
    "Russian": "Rusça",
    "Scottish Gaelic": "İskoç Galcesi",
    "Serbian": "Sırpça",
    "Slovak": "Slovakça",
    "Slovenian": "Slovence",
    "Spanish": "İspanyolca",
    "Swedish": "İsveççe",
    "Thai": "Tayca",
    "Turkish": "Türkçe",
    "Ukrainian": "Ukraynaca",
}

SUMMARY_TERM_TRANSLATIONS = {
    "academic ecosystem": "akademik ekosistem",
    "affordability": "karşılanabilirlik",
    "air quality/environment": "hava kalitesi/çevre",
    "day-to-day living": "gündelik yaşam",
    "mobility": "hareketlilik",
}

URL_RE = re.compile(r"https?://\S+|www\.\S+", flags=re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[^@\s]+@[^@\s]+\b")
CODE_RE = re.compile(r"^(IAU|WHED)-[A-Za-z0-9-]+$")
NUMERIC_RE = re.compile(r"^[\d\s,.\-+%()/]+$")
FILE_NAME_RE = re.compile(r".+\.(json|xlsx|csv|txt|pdf|zip|log)$", flags=re.IGNORECASE)
WINDOWS_PATH_RE = re.compile(r"^[A-Za-z]:\\")
WHITESPACE_RE = re.compile(r"\s+")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Translate DataJSONv2 payloads into Turkish while preserving the original schema."
    )
    parser.add_argument("--input-dir", default="DataJSONv2", help="English DataJSONv2 directory.")
    parser.add_argument("--output-dir", default="DataJSONv2-TR", help="Turkish output directory.")
    parser.add_argument(
        "--cache-file",
        default=".cache/datajsonv2_tr_translation_cache.json",
        help="Translation cache file path.",
    )
    parser.add_argument("--batch-size", type=int, default=40, help="Batch size for remote translations.")
    parser.add_argument(
        "--parallel-batches",
        type=int,
        default=8,
        help="How many translation batches to send concurrently.",
    )
    return parser


def normalize_text_fix(value: str) -> str:
    fixed = KNOWN_TEXT_FIXES.get(value, value)
    previous = None
    while previous != fixed:
        previous = fixed
        for source, target in MOJIBAKE_REPLACEMENTS.items():
            fixed = fixed.replace(source, target)
    return fixed


def clean_spacing(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def preserve_string(*, key: str | None, path: tuple[str, ...], value: str) -> bool:
    if not value.strip():
        return True
    if key in DO_NOT_TRANSLATE_KEYS:
        return True
    if path in PRESERVE_PATHS:
        return True
    if CODE_RE.match(value.strip()):
        return True
    if NUMERIC_RE.match(value.strip()):
        return True
    if URL_RE.fullmatch(value.strip()):
        return True
    if EMAIL_RE.fullmatch(value.strip()):
        return True
    if FILE_NAME_RE.fullmatch(value.strip()):
        return True
    if WINDOWS_PATH_RE.match(value.strip()):
        return True
    return False


def protect_inline_tokens(text: str) -> tuple[str, dict[str, str]]:
    protected: dict[str, str] = {}

    def replacer(match: re.Match[str]) -> str:
        token = f"__KEEP_{len(protected)}__"
        protected[token] = match.group(0)
        return token

    updated = URL_RE.sub(replacer, text)
    updated = EMAIL_RE.sub(replacer, updated)
    return updated, protected


def restore_inline_tokens(text: str, protected: dict[str, str]) -> str:
    restored = text
    for token, value in protected.items():
        restored = restored.replace(token, value)
    return restored


def split_keep_simple(text: str, delimiter: str) -> list[str]:
    return [part.strip() for part in text.split(delimiter) if part.strip()]


def chunked(values: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def write_no_translate_report(output_dir: Path) -> None:
    report_path = output_dir / "NON_TRANSLATED_FIELDS.md"
    lines: list[str] = [
        "# Non-Translated Fields",
        "",
        "These values are intentionally preserved in their original form during the TR export.",
        "",
        "## Applied Now",
        "",
    ]

    for path, reason in NO_TRANSLATE_REPORT_ITEMS:
        lines.append(f"- `{path}`: {reason}")

    lines.extend(
        [
            "",
            "## Partial Translation Rules",
            "",
        ]
    )
    for path, reason in PARTIAL_TRANSLATION_REPORT_ITEMS:
        lines.append(f"- `{path}`: {reason}")

    lines.extend(
        [
            "",
            "## Pattern-Based Rules",
            "",
        ]
    )
    for label, reason in PATTERN_BASED_REPORT_ITEMS:
        lines.append(f"- `{label}`: {reason}")

    lines.extend(
        [
            "",
            "## Review Candidates",
            "",
        ]
    )
    for path, reason in REVIEW_CANDIDATE_REPORT_ITEMS:
        lines.append(f"- `{path}`: {reason}")

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def collect_no_translate_samples(output_dir: Path) -> dict[str, list[str]]:
    sample_limit = 8
    samples: dict[str, list[str]] = {
        "university_details.code": [],
        "data.general_information.university_name": [],
        "data.general_information.native_name": [],
        "data.general_information.country": [],
        "data.divisions.division_name": [],
        "data.divisions.more_details": [],
        "data.degree_fields.degree_field_title": [],
        "data.location_information.street": [],
        "data.location_information.city": [],
        "data.location_information.province": [],
        "data.location_information.post_code": [],
        "data.location_information.full_address": [],
        "data.contact_information.website": [],
        "data.contact_information.contact_page": [],
        "data.contact_information.email": [],
        "data.contact_information.phone": [],
        "data.contact_information.phone_standardized": [],
        "data.officers.name": [],
        "manifest[].state": [],
    }
    seen: dict[str, set[str]] = {key: set() for key in samples}

    def add(label: str, value: object) -> None:
        if label not in samples:
            return
        if value is None:
            return
        text = str(value).strip()
        if not text or text in seen[label] or len(samples[label]) >= sample_limit:
            return
        seen[label].add(text)
        samples[label].append(text)

    for path in sorted(output_dir.rglob("*.json")):
        if path.name in {"summary.json", "all_countries.json", "NON_TRANSLATED_FIELD_SAMPLES.json"}:
            continue

        payload = json.loads(path.read_text(encoding="utf-8"))
        if path.name == "manifest.json" and isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    add("manifest[].state", item.get("state"))
            continue

        if not isinstance(payload, dict):
            continue

        for university in payload.get("university_details", []):
            add("university_details.code", university.get("code"))
            data = university.get("data", {})
            general = data.get("general_information", {})
            location = data.get("location_information", {})
            contact = data.get("contact_information", {})

            add("data.general_information.university_name", general.get("university_name"))
            add("data.general_information.native_name", general.get("native_name"))
            add("data.general_information.country", general.get("country"))

            for division in data.get("divisions", []):
                add("data.divisions.division_name", division.get("division_name"))
                add("data.divisions.more_details", division.get("more_details"))

            for degree_field in data.get("degree_fields", []):
                add("data.degree_fields.degree_field_title", degree_field.get("degree_field_title"))

            add("data.location_information.street", location.get("street"))
            add("data.location_information.city", location.get("city"))
            add("data.location_information.province", location.get("province"))
            add("data.location_information.post_code", location.get("post_code"))
            add("data.location_information.full_address", location.get("full_address"))

            add("data.contact_information.website", contact.get("website"))
            add("data.contact_information.contact_page", contact.get("contact_page"))
            add("data.contact_information.email", contact.get("email"))
            add("data.contact_information.phone", contact.get("phone"))
            add("data.contact_information.phone_standardized", contact.get("phone_standardized"))

            for officer in data.get("officers", []):
                add("data.officers.name", officer.get("name"))

    return samples


def write_no_translate_samples(output_dir: Path) -> None:
    report_path = output_dir / "NON_TRANSLATED_FIELD_SAMPLES.json"
    payload = {
        "description": "Sample values for fields intentionally kept untranslated in DataJSONv2-TR.",
        "samples": collect_no_translate_samples(output_dir),
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class TurkishTranslator:
    def __init__(self, *, cache_file: Path, batch_size: int, parallel_batches: int) -> None:
        self.cache_file = cache_file
        self.batch_size = max(batch_size, 1)
        self.parallel_batches = max(parallel_batches, 1)
        self.cache = self._load_cache()
        self.new_cache_entries = 0

    def _load_cache(self) -> dict[str, str]:
        if not self.cache_file.exists():
            return {}
        return json.loads(self.cache_file.read_text(encoding="utf-8"))

    def save_cache(self) -> None:
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache_file.write_text(
            json.dumps(dict(sorted(self.cache.items())), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def translate_many(self, texts: list[str]) -> None:
        pending = [text for text in texts if text not in self.cache and text not in EXACT_TRANSLATIONS]
        if not pending:
            return

        batches = list(chunked(pending, self.batch_size))
        total_groups = (len(batches) + self.parallel_batches - 1) // self.parallel_batches
        for group_index, batch_group in enumerate(chunked(batches, self.parallel_batches), start=1):
            asyncio.run(self._translate_group(batch_group))
            if group_index % 5 == 0 or group_index == total_groups:
                self.save_cache()
                print(
                    f"[translate] groups {group_index}/{total_groups}, cache size={len(self.cache)}",
                    flush=True,
                )

    async def _translate_group(self, batch_group: list[list[str]]) -> None:
        await asyncio.gather(*(self._translate_batch_recursive(batch) for batch in batch_group))

    async def _translate_batch_recursive(self, batch: list[str]) -> None:
        if not batch:
            return
        if len(batch) == 1:
            source = batch[0]
            self.cache[source] = await self._translate_single_async(source)
            self.new_cache_entries += 1
            return

        for attempt in range(5):
            try:
                async with GoogleTranslator() as translator:
                    protected_batch: list[str] = []
                    protected_maps: list[dict[str, str]] = []
                    for item in batch:
                        protected_text, protected = protect_inline_tokens(item)
                        protected_batch.append(protected_text)
                        protected_maps.append(protected)

                    translated_items = await translator.translate(
                        protected_batch,
                        src="auto",
                        dest="tr",
                    )
                    if not isinstance(translated_items, list):
                        translated_items = [translated_items]
                    if len(translated_items) != len(batch):
                        raise RuntimeError("Unexpected translation batch length")

                    for source, target, protected in zip(batch, translated_items, protected_maps):
                        restored = restore_inline_tokens(str(target.text or "").strip(), protected)
                        normalized = clean_spacing(normalize_text_fix(restored))
                        if not normalized:
                            raise RuntimeError("Empty translation received")
                        self.cache[source] = normalized
                        self.new_cache_entries += 1
                    return
            except Exception:
                await asyncio.sleep(1.2 * (attempt + 1))

        midpoint = len(batch) // 2
        await self._translate_batch_recursive(batch[:midpoint])
        await self._translate_batch_recursive(batch[midpoint:])

    async def _translate_single_async(self, text: str) -> str:
        for attempt in range(5):
            try:
                protected_text, protected = protect_inline_tokens(text)
                async with GoogleTranslator() as translator:
                    translated = await translator.translate(protected_text, src="auto", dest="tr")
                restored = restore_inline_tokens(str(translated.text or "").strip(), protected)
                normalized = clean_spacing(normalize_text_fix(restored))
                if normalized:
                    return normalized
            except Exception:
                await asyncio.sleep(1.2 * (attempt + 1))
        raise RuntimeError(f"Translation failed after retries: {text[:120]}")

    def translate_fragment(self, text: str) -> str:
        normalized_source = clean_spacing(normalize_text_fix(text))
        if not normalized_source:
            return text
        if normalized_source in EXACT_TRANSLATIONS:
            return clean_spacing(normalize_text_fix(EXACT_TRANSLATIONS[normalized_source]))
        cached = self.cache.get(normalized_source)
        if cached is not None:
            return clean_spacing(normalize_text_fix(cached))
        translated = asyncio.run(self._translate_single_async(normalized_source))
        self.cache[normalized_source] = translated
        self.new_cache_entries += 1
        return translated


def collect_translatable_fragments(
    obj: Any,
    *,
    collector: set[str],
    path: tuple[str, ...] = (),
    key: str | None = None,
) -> None:
    if isinstance(obj, dict):
        for child_key, child_value in obj.items():
            collect_translatable_fragments(
                child_value,
                collector=collector,
                path=path + (child_key,),
                key=child_key,
            )
        return

    if isinstance(obj, list):
        for item in obj:
            collect_translatable_fragments(item, collector=collector, path=path, key=key)
        return

    if not isinstance(obj, str):
        return

    tail_path = path[-3:]
    if key == "locale":
        return

    value = normalize_text_fix(obj).strip()
    if preserve_string(key=key, path=tail_path, value=value):
        return

    if tail_path == ("data", "contact_information", "key_contacts"):
        for entry in split_keep_simple(value, ";"):
            match = re.match(r"^(.*?)\s*\((.*?)\)\s*$", entry)
            if match:
                collector.add(clean_spacing(match.group(2)))
            else:
                collector.add(clean_spacing(entry))
        return

    if tail_path in COMMA_LIST_PATHS:
        for part in split_keep_simple(value, ","):
            collector.add(clean_spacing(normalize_text_fix(part)))
        return

    if tail_path in SEMICOLON_LIST_PATHS:
        for part in split_keep_simple(value, ";"):
            collector.add(clean_spacing(normalize_text_fix(part)))
        return

    if tail_path == ("data", "general_information", "country"):
        parts = value.split(" - ", 1)
        if parts[0] not in COUNTRY_TRANSLATIONS:
            collector.add(clean_spacing(parts[0]))
        if len(parts) == 2:
            collector.add(clean_spacing(parts[1]))
        return

    collector.add(clean_spacing(value))


def translate_language_token(text: str, translator: TurkishTranslator) -> str:
    normalized = clean_spacing(normalize_text_fix(text))
    for source, target in LANGUAGE_TRANSLATIONS.items():
        if normalized == source:
            return target
        if normalized == f"{source} Religious Affiliation Christian":
            return f"{target} Dini Bağlılık: Hristiyan"
        if normalized == f"{source} Religious Affiliation None":
            return f"{target} Dini Bağlılık: Yok"
    return translator.translate_fragment(normalized)


def translate_country_value(text: str, translator: TurkishTranslator) -> str:
    normalized = clean_spacing(normalize_text_fix(text))
    parts = normalized.split(" - ", 1)
    country_part = COUNTRY_TRANSLATIONS.get(parts[0], translator.translate_fragment(parts[0]))
    if len(parts) == 1:
        return country_part
    region_part = translator.translate_fragment(parts[1])
    return f"{country_part} - {region_part}"


def translate_key_contacts(text: str, translator: TurkishTranslator) -> str:
    normalized = clean_spacing(normalize_text_fix(text))
    translated_entries: list[str] = []
    for entry in split_keep_simple(normalized, ";"):
        match = re.match(r"^(.*?)\s*\((.*?)\)\s*$", entry)
        if match:
            person_name = match.group(1).strip()
            title = translator.translate_fragment(match.group(2).strip())
            translated_entries.append(f"{person_name} ({title})")
        else:
            translated_entries.append(translator.translate_fragment(entry))
    return "; ".join(translated_entries)


def translate_student_friendliness_summary(text: str, translator: TurkishTranslator) -> str:
    normalized = clean_spacing(normalize_text_fix(text))
    if not normalized:
        return normalized

    parts = split_keep_simple(normalized, ";")
    translated_parts: list[str] = []
    for part in parts:
        if part.startswith("Strengths:"):
            raw_values = part.split(":", 1)[1].strip()
            translated_values = ", ".join(
                SUMMARY_TERM_TRANSLATIONS.get(item.strip(), translator.translate_fragment(item.strip()))
                for item in split_keep_simple(raw_values, ",")
            )
            translated_parts.append(f"Güçlü yönler: {translated_values}")
            continue

        if part.startswith("Weakness:"):
            raw_values = part.split(":", 1)[1].strip()
            translated_values = ", ".join(
                SUMMARY_TERM_TRANSLATIONS.get(item.strip(), translator.translate_fragment(item.strip()))
                for item in split_keep_simple(raw_values, ",")
            )
            translated_parts.append(f"Zayıf yön: {translated_values}")
            continue

        if part == "Uses region-level fallback data":
            translated_parts.append("Bölge düzeyindeki yedek veriler kullanılmıştır")
            continue

        if part == "Uses country-level fallback data":
            translated_parts.append("Ülke düzeyindeki yedek veriler kullanılmıştır")
            continue

        translated_parts.append(translator.translate_fragment(part))

    return "; ".join(translated_parts)


def translate_by_separator(
    text: str,
    *,
    separator: str,
    translator: TurkishTranslator,
    value_translator,
) -> str:
    normalized = clean_spacing(normalize_text_fix(text))
    return f"{separator} ".join(value_translator(part.strip(), translator) for part in normalized.split(separator) if part.strip())


def translate_string_value(
    value: str,
    *,
    path: tuple[str, ...],
    key: str | None,
    translator: TurkishTranslator,
    output_dir: Path,
) -> str:
    if key == "locale":
        return "tr"

    if key == "output_dir":
        return str(output_dir.resolve())

    tail_path = path[-3:]
    normalized = normalize_text_fix(value).strip()
    if preserve_string(key=key, path=tail_path, value=normalized):
        return normalized

    if tail_path == ("data", "contact_information", "key_contacts"):
        return translate_key_contacts(normalized, translator)

    if tail_path == ("data", "student_friendliness", "summary"):
        return translate_student_friendliness_summary(normalized, translator)

    if tail_path in COMMA_LIST_PATHS:
        return translate_by_separator(
            normalized,
            separator=",",
            translator=translator,
            value_translator=lambda part, tr: tr.translate_fragment(part),
        )

    if tail_path in SEMICOLON_LIST_PATHS:
        return translate_by_separator(
            normalized,
            separator=";",
            translator=translator,
            value_translator=translate_language_token,
        )

    if tail_path == ("data", "general_information", "country"):
        return translate_country_value(normalized, translator)

    return translator.translate_fragment(normalized)


def translate_structure(
    obj: Any,
    *,
    translator: TurkishTranslator,
    output_dir: Path,
    path: tuple[str, ...] = (),
    key: str | None = None,
) -> Any:
    if isinstance(obj, dict):
        return {
            child_key: translate_structure(
                child_value,
                translator=translator,
                output_dir=output_dir,
                path=path + (child_key,),
                key=child_key,
            )
            for child_key, child_value in obj.items()
        }

    if isinstance(obj, list):
        return [
            translate_structure(
                item,
                translator=translator,
                output_dir=output_dir,
                path=path,
                key=key,
            )
            for item in obj
        ]

    if isinstance(obj, str):
        return translate_string_value(
            obj,
            path=path,
            key=key,
            translator=translator,
            output_dir=output_dir,
        )

    return obj


def iter_json_files(input_dir: Path) -> list[Path]:
    return sorted(path for path in input_dir.rglob("*.json") if path.is_file())


def main() -> None:
    args = build_parser().parse_args()
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    cache_file = Path(args.cache_file)

    json_files = iter_json_files(input_dir)
    if not json_files:
        raise SystemExit(f"No JSON files found under {input_dir}")

    translator = TurkishTranslator(
        cache_file=cache_file,
        batch_size=args.batch_size,
        parallel_batches=args.parallel_batches,
    )

    collector: set[str] = set()
    for json_file in json_files:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        collect_translatable_fragments(payload, collector=collector)

    pending = sorted(text for text in collector if text not in EXACT_TRANSLATIONS and text not in translator.cache)
    print(f"[collect] JSON files: {len(json_files)}", flush=True)
    print(f"[collect] Unique text fragments: {len(collector)}", flush=True)
    print(f"[collect] Pending remote translations: {len(pending)}", flush=True)

    translator.translate_many(pending)
    translator.save_cache()

    for index, json_file in enumerate(json_files, start=1):
        payload = json.loads(json_file.read_text(encoding="utf-8"))
        translated_payload = translate_structure(
            payload,
            translator=translator,
            output_dir=output_dir,
        )
        output_path = output_dir / json_file.relative_to(input_dir)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(translated_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if index % 10 == 0 or index == len(json_files):
            print(f"[write] files {index}/{len(json_files)}", flush=True)

    translator.save_cache()
    write_no_translate_report(output_dir)
    write_no_translate_samples(output_dir)
    print(f"[done] Output directory: {output_dir.resolve()}", flush=True)
    print(f"[done] Cache entries: {len(translator.cache)}", flush=True)


if __name__ == "__main__":
    main()
