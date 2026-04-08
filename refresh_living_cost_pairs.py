from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any


SECTION_NUMERIC_FIELDS = {
    "living_cost_information": (
        "estimated_annual_student_cost_shared_housing_usd",
        "estimated_annual_student_cost_solo_apartment_usd",
        "estimated_annual_base_living_cost_without_rent_usd",
        "estimated_annual_housing_cost_shared_usd",
        "estimated_annual_housing_cost_solo_usd",
        "estimated_annual_food_cost_usd",
        "estimated_annual_transport_cost_usd",
    ),
    "student_friendliness": (
        "data_completeness_percent",
        "affordability_score",
        "daily_life_score",
        "mobility_score",
        "environment_score",
        "academic_ecosystem_score",
        "score",
    ),
    "ranking_and_outcome_information": (
        "acceptance_rate_percent",
        "graduation_rate_percent",
        "accepted_students_count",
        "graduates_count",
        "citation_per_faculty",
        "average_graduate_salary_usd",
        "international_student_ratio_percent",
        "international_staff_ratio_percent",
        "number_of_partner_universities",
        "visa_difficulty_score",
        "part_time_job_availability_score",
    ),
    "environment_and_lifestyle": (
        "cost_of_living_trend_annual_inflation_percent",
        "inflation_adjusted_cost_index",
        "dorm_capacity",
        "cultural_activity_score",
        "nightlife_score",
        "family_friendliness_score",
        "digital_infrastructure_score",
    ),
}
COUNTRY_JSON_RE = re.compile(r"^\d+-.*\.json$")
NUMBER_RE = re.compile(r"-?\d+(?:[.,]\d+)?")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Rewrite DataJSONv2 numeric city/state metrics into "
            "'city_value / country_max_value' format."
        )
    )
    parser.add_argument(
        "output_dirs",
        nargs="*",
        default=["DataJSONv2", "DataJSONv2-TR"],
        help="DataJSON output directories to update.",
    )
    return parser


def parse_numeric_value(value: object) -> float | None:
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    city_segment = text.split("/", 1)[0].strip()
    match = NUMBER_RE.search(city_segment)
    if match is None:
        return None

    return float(match.group(0).replace(",", "."))


def format_cost_pair(city_value: float, country_max_value: float) -> str:
    return f"{city_value:.2f} / {country_max_value:.2f}"


def iter_country_payloads(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("university_details"), list):
        return [payload]

    if isinstance(payload, list):
        return [
            item
            for item in payload
            if isinstance(item, dict) and isinstance(item.get("university_details"), list)
        ]

    return []


def root_country_json_files(output_dir: Path) -> list[Path]:
    return sorted(
        path
        for path in output_dir.glob("*.json")
        if path.is_file() and COUNTRY_JSON_RE.match(path.name)
    )


def all_json_files(output_dir: Path) -> list[Path]:
    return sorted(path for path in output_dir.rglob("*.json") if path.is_file())


def build_country_maxima(output_dir: Path) -> dict[int, dict[str, dict[str, float]]]:
    maxima: dict[int, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))

    for path in root_country_json_files(output_dir):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for country_payload in iter_country_payloads(payload):
            country_id = country_payload.get("country_id")
            if not isinstance(country_id, int):
                continue

            for university in country_payload["university_details"]:
                if not isinstance(university, dict):
                    continue

                data = university.get("data")
                if not isinstance(data, dict):
                    continue

                for section_name, section_fields in SECTION_NUMERIC_FIELDS.items():
                    section = data.get(section_name)
                    if not isinstance(section, dict):
                        continue

                    for field in section_fields:
                        value = parse_numeric_value(section.get(field))
                        if value is None:
                            continue

                        current_max = maxima[country_id][section_name].get(field)
                        if current_max is None or value > current_max:
                            maxima[country_id][section_name][field] = value

    return {
        country_id: {
            section_name: dict(field_maxima)
            for section_name, field_maxima in section_maxima.items()
        }
        for country_id, section_maxima in maxima.items()
    }


def update_payload_with_country_maxima(
    payload: Any,
    country_maxima: dict[int, dict[str, dict[str, float]]],
) -> tuple[bool, int]:
    changed = False
    updated_values = 0

    for country_payload in iter_country_payloads(payload):
        country_id = country_payload.get("country_id")
        if not isinstance(country_id, int):
            continue

        section_maxima = country_maxima.get(country_id, {})
        if not section_maxima:
            continue

        for university in country_payload["university_details"]:
            if not isinstance(university, dict):
                continue

            data = university.get("data")
            if not isinstance(data, dict):
                continue

            for section_name, section_fields in SECTION_NUMERIC_FIELDS.items():
                section = data.get(section_name)
                if not isinstance(section, dict):
                    continue

                field_maxima = section_maxima.get(section_name, {})
                if not field_maxima:
                    continue

                for field in section_fields:
                    city_value = parse_numeric_value(section.get(field))
                    country_max_value = field_maxima.get(field)
                    if city_value is None or country_max_value is None:
                        continue

                    formatted_value = format_cost_pair(city_value, country_max_value)
                    if section.get(field) == formatted_value:
                        continue

                    section[field] = formatted_value
                    changed = True
                    updated_values += 1

    return changed, updated_values


def refresh_output_dir(output_dir: Path) -> dict[str, int]:
    country_maxima = build_country_maxima(output_dir)
    if not country_maxima:
        raise SystemExit(f"No country maxima could be built from {output_dir}")

    files_written = 0
    updated_values = 0

    for path in all_json_files(output_dir):
        payload = json.loads(path.read_text(encoding="utf-8"))
        changed, payload_updates = update_payload_with_country_maxima(payload, country_maxima)
        if not changed:
            continue

        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        files_written += 1
        updated_values += payload_updates

    return {
        "country_count": len(country_maxima),
        "files_written": files_written,
        "updated_values": updated_values,
    }


def main() -> None:
    args = build_parser().parse_args()

    for raw_output_dir in args.output_dirs:
        output_dir = Path(raw_output_dir)
        summary = refresh_output_dir(output_dir)
        print(f"[done] Directory: {output_dir.resolve()}", flush=True)
        print(f"[done] Countries scanned: {summary['country_count']}", flush=True)
        print(f"[done] JSON files updated: {summary['files_written']}", flush=True)
        print(f"[done] Numeric metric values updated: {summary['updated_values']}", flush=True)


if __name__ == "__main__":
    main()
