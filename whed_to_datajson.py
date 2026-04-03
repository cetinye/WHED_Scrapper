from __future__ import annotations

import argparse
import json
import re
import uuid
from collections import Counter, defaultdict
from pathlib import Path

from openpyxl import load_workbook

from txt_to_excel import (
    build_city_indexes,
    build_country_lookup,
    build_district_indexes,
    build_holland_match_index,
    build_whed_admission_requirement_code,
    build_whed_admission_requirement_usage_index,
    choose_ambiguous_holland_match,
    choose_preferred_isced_code,
    extract_whed_bachelor_program_items,
    infer_program_attributes,
    is_noise_db_program_name,
    load_whed_admission_requirement_records,
    match_city_id,
    normalize_text,
    normalize_university_type,
    parse_country_name,
    parse_region_value,
    slugify_identifier,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export WHED workbook data into the requested country-scoped JSON format."
    )
    parser.add_argument(
        "--whed-file",
        default="whed_data.xlsx",
        help="WHED workbook that contains the Institutions and Admission Requirement IDs sheets.",
    )
    parser.add_argument(
        "--output-dir",
        default="DataJSON",
        help="Folder that will receive the generated JSON files.",
    )
    parser.add_argument(
        "--countries-file",
        default=r"References/TercihAnalizi Database Tables/countries.xlsx",
        help="Countries reference workbook.",
    )
    parser.add_argument(
        "--cities-file",
        default=r"References/TercihAnalizi Database Tables/cities.xlsx",
        help="Cities reference workbook.",
    )
    parser.add_argument(
        "--districts-file",
        default=r"References/TercihAnalizi Database Tables/districts.xlsx",
        help="Districts reference workbook.",
    )
    parser.add_argument(
        "--holland-matches-file",
        default=r"References/TercihAnalizi Database Tables/holland_matches.xlsx",
        help="Holland match reference workbook.",
    )
    return parser


def load_full_institution_rows(whed_file: Path) -> list[dict[str, object]]:
    workbook = load_workbook(whed_file, read_only=True, data_only=True)
    sheet = workbook["Institutions"]
    rows = sheet.iter_rows(values_only=True)
    header = list(next(rows))
    index_by_name = {str(column): position for position, column in enumerate(header)}

    selected_columns = [
        "University Name",
        "IAU Code",
        "Country",
        "City",
        "Province",
        "Institution Funding",
        "Bachelor's Degree",
        "ISCED-F",
        "Admission Requirement IDs",
    ]

    records: list[dict[str, object]] = []
    for row in rows:
        records.append({column: row[index_by_name[column]] for column in selected_columns})
    return records


def split_condition_ids(value: object) -> list[str]:
    condition_ids: list[str] = []
    for part in re.split(r"[;,]", str(value or "")):
        normalized = str(part).strip()
        if normalized:
            condition_ids.append(normalized)
    return condition_ids


def generate_program_provider_id(country_id: object, program_name: str) -> str:
    name_seed = f"namespace={country_id}&name={program_name.strip()}&version=1.0"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name_seed))


def choose_canonical_name(
    name_counts: Counter[str],
    first_seen_order: dict[str, int],
) -> str:
    if not name_counts:
        return ""

    return min(
        name_counts,
        key=lambda name: (-name_counts[name], first_seen_order[name], normalize_text(name), name),
    )


def ordered_aliases(canonical_name: str, first_seen_order: dict[str, int]) -> list[str]:
    aliases = sorted(first_seen_order, key=lambda name: (name != canonical_name, first_seen_order[name], name))
    return aliases or ([canonical_name] if canonical_name else [])


def build_country_variants_by_base_country(
    institution_rows: list[dict[str, object]],
    usage_index: dict[tuple[str, str], int],
) -> dict[str, set[str]]:
    variants: dict[str, set[str]] = defaultdict(set)

    for raw_country, _condition_id in usage_index:
        if raw_country:
            variants[parse_country_name(raw_country)].add(raw_country)

    for row in institution_rows:
        raw_country = str(row.get("Country") or "").strip()
        raw_ids = split_condition_ids(row.get("Admission Requirement IDs"))
        if raw_country and raw_ids:
            variants[parse_country_name(raw_country)].add(raw_country)

    return variants


def build_placement_conditions(
    *,
    whed_file: Path,
    country_lookup: dict[str, tuple[object, str]],
    country_variants_by_base_country: dict[str, set[str]],
) -> tuple[dict[object, list[dict[str, str]]], dict[object, dict[str, int]], dict[str, int]]:
    usage_index = build_whed_admission_requirement_usage_index(whed_file)
    source_rows = load_whed_admission_requirement_records(whed_file)

    placement_conditions_by_country: dict[object, list[dict[str, str]]] = defaultdict(list)
    placement_condition_positions: dict[object, dict[str, int]] = defaultdict(dict)
    stats = {
        "condition_rows_scanned": len(source_rows),
        "condition_rows_used": 0,
        "condition_rows_skipped_unused": 0,
        "condition_country_misses": 0,
    }

    for row in source_rows:
        raw_country = str(row.get("Country") or "").strip()
        condition_id = str(row.get("Condition ID") or "").strip()
        condition = str(row.get("Condition") or "").strip()

        if not raw_country or not condition_id or not condition:
            stats["condition_rows_skipped_unused"] += 1
            continue

        if (raw_country, condition_id) not in usage_index:
            stats["condition_rows_skipped_unused"] += 1
            continue

        country_name = parse_country_name(raw_country)
        country_match = country_lookup.get(normalize_text(country_name))
        if country_match is None:
            stats["condition_country_misses"] += 1
            continue

        country_id = country_match[0]
        scoped_code = build_whed_admission_requirement_code(
            raw_country=raw_country,
            condition_id=condition_id,
            country_variants_by_base_country=country_variants_by_base_country,
        )
        if scoped_code in placement_condition_positions[country_id]:
            continue

        placement_conditions_by_country[country_id].append(
            {
                "code": scoped_code,
                "content": condition,
            }
        )
        placement_condition_positions[country_id][scoped_code] = len(placement_conditions_by_country[country_id])
        stats["condition_rows_used"] += 1

    return placement_conditions_by_country, placement_condition_positions, stats


def build_programs(
    *,
    institution_rows: list[dict[str, object]],
    country_lookup: dict[str, tuple[object, str]],
    holland_matches_file: Path,
) -> tuple[dict[object, list[dict[str, object]]], dict[tuple[object, str], str], dict[str, int]]:
    holland_unique_match_index, holland_ambiguous_match_index = build_holland_match_index(holland_matches_file)

    aggregates: dict[tuple[object, str], dict[str, object]] = {}
    stats = {
        "program_country_misses": 0,
        "program_offerings_scanned": 0,
        "program_unique_rows": 0,
        "program_holland_exact": 0,
        "program_holland_heuristic": 0,
        "program_holland_inferred": 0,
        "program_holland_missing": 0,
    }
    name_sequence = 0

    for row in institution_rows:
        raw_country = row.get("Country")
        country_name = parse_country_name(raw_country)
        country_match = country_lookup.get(normalize_text(country_name))
        if country_match is None:
            stats["program_country_misses"] += 1
            continue

        country_id = country_match[0]
        for item in extract_whed_bachelor_program_items(row):
            if is_noise_db_program_name(item["name"], country_lookup):
                continue

            stats["program_offerings_scanned"] += 1
            key = (country_id, normalize_text(item["name"]))
            aggregate = aggregates.setdefault(
                key,
                {
                    "country_id": country_id,
                    "name_counts": Counter(),
                    "first_seen_order": {},
                    "offer_count": 0,
                    "isced_counts": Counter(),
                },
            )

            aggregate["name_counts"][item["name"]] += 1
            if item["name"] not in aggregate["first_seen_order"]:
                aggregate["first_seen_order"][item["name"]] = name_sequence
                name_sequence += 1
            aggregate["offer_count"] += 1
            if item["isced_f"]:
                aggregate["isced_counts"][item["isced_f"]] += 1

    programs_by_country: dict[object, list[dict[str, object]]] = defaultdict(list)
    provider_id_by_program_key: dict[tuple[object, str], str] = {}

    for key in sorted(aggregates, key=lambda item: (item[0], item[1])):
        aggregate = aggregates[key]
        canonical_name = choose_canonical_name(aggregate["name_counts"], aggregate["first_seen_order"])
        alias_list = ordered_aliases(canonical_name, aggregate["first_seen_order"])
        selected_isced = choose_preferred_isced_code(aggregate["isced_counts"])

        holland_match = holland_unique_match_index.get(selected_isced)
        holland_match_id = None
        riasec_code = ""
        value_code = ""
        map_point = None
        holland_status = "missing"
        self_inference_basis = ""

        if holland_match is not None:
            holland_match_id = int(holland_match["id"])
            riasec_code = str(holland_match.get("riasec_code") or "")
            value_code = str(holland_match.get("value_code") or "")
            map_point = holland_match.get("map_point")
            holland_status = "matched"
            stats["program_holland_exact"] += 1
        elif selected_isced in holland_ambiguous_match_index:
            ambiguous_match = choose_ambiguous_holland_match(
                program_name=canonical_name,
                isced_f=selected_isced,
                ambiguous_matches=holland_ambiguous_match_index[selected_isced],
            )
            if ambiguous_match is not None:
                holland_match_id = int(ambiguous_match["id"])
                riasec_code = str(ambiguous_match.get("riasec_code") or "")
                value_code = str(ambiguous_match.get("value_code") or "")
                map_point = ambiguous_match.get("map_point")
                holland_status = "heuristic"
                stats["program_holland_heuristic"] += 1

        if holland_match_id is None:
            inferred_attributes = infer_program_attributes(
                program_name=canonical_name,
                isced_f=selected_isced,
            )
            if inferred_attributes is not None:
                riasec_code, value_code, map_point, self_inference_basis = inferred_attributes
                holland_status = "self_inferred"
                stats["program_holland_inferred"] += 1
            else:
                stats["program_holland_missing"] += 1

        provider_id = generate_program_provider_id(aggregate["country_id"], canonical_name)
        provider_id_by_program_key[key] = provider_id

        details: dict[str, object] = {
            "source": "WHED",
            "degree_type": "Bachelor's Degree",
            "whed_offer_count": int(aggregate["offer_count"]),
            "holland_status": holland_status,
        }
        if self_inference_basis:
            details["self_inference_basis"] = self_inference_basis

        programs_by_country[aggregate["country_id"]].append(
            {
                "isced_code": selected_isced,
                "holland_match_id": holland_match_id,
                "name": canonical_name,
                "map_point": map_point,
                "riasec_code": riasec_code,
                "value_code": value_code,
                "dignity": 0,
                "year": 4,
                "details": details,
                "alias": alias_list,
                "provider_name": "whed",
                "provider_id": provider_id,
            }
        )

    for country_id in programs_by_country:
        programs_by_country[country_id].sort(key=lambda item: (normalize_text(item["name"]), item["name"]))

    stats["program_unique_rows"] = len(aggregates)
    return programs_by_country, provider_id_by_program_key, stats


def build_universities(
    *,
    institution_rows: list[dict[str, object]],
    country_lookup: dict[str, tuple[object, str]],
    city_exact_index: dict[tuple[object, str], list[object]],
    city_simplified_index: dict[tuple[object, str], list[object]],
    city_iso2_index: dict[tuple[object, str], object],
    district_exact_index: dict[tuple[object, str], list[tuple[object, str]]],
    district_simplified_index: dict[tuple[object, str], list[tuple[object, str]]],
    placement_condition_positions: dict[object, dict[str, int]],
    country_variants_by_base_country: dict[str, set[str]],
    provider_id_by_program_key: dict[tuple[object, str], str],
) -> tuple[dict[object, list[dict[str, object]]], dict[str, int]]:
    universities_by_country: dict[object, list[dict[str, object]]] = defaultdict(list)
    stats = {
        "university_country_misses": 0,
        "university_city_misses": 0,
        "university_rows_used": 0,
        "university_program_rows": 0,
        "condition_refs_missing_from_array": 0,
    }
    fallback_code_counters: Counter[object] = Counter()

    for row in institution_rows:
        raw_country = row.get("Country")
        country_name = parse_country_name(raw_country)
        country_match = country_lookup.get(normalize_text(country_name))
        if country_match is None:
            stats["university_country_misses"] += 1
            continue

        country_id, matched_country_name = country_match
        region_value = parse_region_value(raw_country, row.get("Province"))
        city_id = match_city_id(
            country_id=country_id,
            country_name=matched_country_name,
            city_value=row.get("City"),
            region_value=region_value,
            city_exact_index=city_exact_index,
            city_simplified_index=city_simplified_index,
            city_iso2_index=city_iso2_index,
            district_exact_index=district_exact_index,
            district_simplified_index=district_simplified_index,
        )
        if city_id is None:
            stats["university_city_misses"] += 1

        raw_iau_code = str(row.get("IAU Code") or "").strip()
        if raw_iau_code:
            university_code = raw_iau_code
        else:
            fallback_code_counters[country_id] += 1
            university_code = f"WHED-{country_id}-{fallback_code_counters[country_id]}"

        condition_numbers: list[str] = []
        seen_condition_numbers: set[str] = set()
        for condition_id in split_condition_ids(row.get("Admission Requirement IDs")):
            scoped_code = build_whed_admission_requirement_code(
                raw_country=str(raw_country or "").strip(),
                condition_id=condition_id,
                country_variants_by_base_country=country_variants_by_base_country,
            )
            position = placement_condition_positions.get(country_id, {}).get(scoped_code)
            if position is None:
                stats["condition_refs_missing_from_array"] += 1
                continue

            position_text = str(position)
            if position_text in seen_condition_numbers:
                continue

            seen_condition_numbers.add(position_text)
            condition_numbers.append(position_text)

        university_programs: list[dict[str, object]] = []
        program_sequence = 1
        for item in extract_whed_bachelor_program_items(row):
            if is_noise_db_program_name(item["name"], country_lookup):
                continue

            program_key = (country_id, normalize_text(item["name"]))
            program_provider_id = provider_id_by_program_key.get(
                program_key,
                generate_program_provider_id(country_id, item["name"]),
            )

            details: dict[str, object] = {
                "source": "WHED",
            }
            if item["isced_f"]:
                details["isced_f"] = item["isced_f"]

            university_programs.append(
                {
                    "program_provider_id": program_provider_id,
                    "name": item["name"],
                    "university_program_code": f"{university_code}-{program_sequence}",
                    "year": 4,
                    "conditions": ", ".join(condition_numbers),
                    "details": details,
                }
            )
            program_sequence += 1
            stats["university_program_rows"] += 1

        universities_by_country[country_id].append(
            {
                "city_id": city_id,
                "name": str(row.get("University Name") or "").strip(),
                "code": university_code,
                "type": normalize_university_type(row.get("Institution Funding")),
                "university_programs": university_programs,
            }
        )
        stats["university_rows_used"] += 1

    return universities_by_country, stats


def write_country_json_files(
    *,
    output_dir: Path,
    country_names_by_id: dict[object, str],
    programs_by_country: dict[object, list[dict[str, object]]],
    universities_by_country: dict[object, list[dict[str, object]]],
    placement_conditions_by_country: dict[object, list[dict[str, str]]],
) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)

    country_ids = sorted(
        {
            *programs_by_country.keys(),
            *universities_by_country.keys(),
            *placement_conditions_by_country.keys(),
        }
    )

    all_countries_payload: list[dict[str, object]] = []
    file_count = 0
    for country_id in country_ids:
        payload = {
            "country_id": country_id,
            "programs": programs_by_country.get(country_id, []),
            "universities": universities_by_country.get(country_id, []),
            "placement_conditions": placement_conditions_by_country.get(country_id, []),
        }
        all_countries_payload.append(payload)

        country_name = country_names_by_id.get(country_id, str(country_id))
        file_name = f"{country_id}-{slugify_identifier(country_name) or country_id}.json"
        output_path = output_dir / file_name
        output_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        file_count += 1

    (output_dir / "all_countries.json").write_text(
        json.dumps(all_countries_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return {
        "country_json_files": file_count,
        "all_countries_entries": len(all_countries_payload),
    }


def main() -> int:
    args = build_parser().parse_args()

    whed_file = Path(args.whed_file)
    output_dir = Path(args.output_dir)
    countries_file = Path(args.countries_file)
    cities_file = Path(args.cities_file)
    districts_file = Path(args.districts_file)
    holland_matches_file = Path(args.holland_matches_file)

    institution_rows = load_full_institution_rows(whed_file)
    country_lookup = build_country_lookup(countries_file)
    city_exact_index, city_simplified_index, city_iso2_index = build_city_indexes(cities_file)
    district_exact_index, district_simplified_index = build_district_indexes(districts_file)

    usage_index = build_whed_admission_requirement_usage_index(whed_file)
    country_variants_by_base_country = build_country_variants_by_base_country(institution_rows, usage_index)

    placement_conditions_by_country, placement_condition_positions, condition_stats = build_placement_conditions(
        whed_file=whed_file,
        country_lookup=country_lookup,
        country_variants_by_base_country=country_variants_by_base_country,
    )

    programs_by_country, provider_id_by_program_key, program_stats = build_programs(
        institution_rows=institution_rows,
        country_lookup=country_lookup,
        holland_matches_file=holland_matches_file,
    )

    universities_by_country, university_stats = build_universities(
        institution_rows=institution_rows,
        country_lookup=country_lookup,
        city_exact_index=city_exact_index,
        city_simplified_index=city_simplified_index,
        city_iso2_index=city_iso2_index,
        district_exact_index=district_exact_index,
        district_simplified_index=district_simplified_index,
        placement_condition_positions=placement_condition_positions,
        country_variants_by_base_country=country_variants_by_base_country,
        provider_id_by_program_key=provider_id_by_program_key,
    )

    country_names_by_id: dict[object, str] = {}
    for normalized_country_name, (country_id, country_name) in country_lookup.items():
        if country_id not in country_names_by_id:
            country_names_by_id[country_id] = country_name
        if normalized_country_name == normalize_text(country_name):
            country_names_by_id[country_id] = country_name

    write_stats = write_country_json_files(
        output_dir=output_dir,
        country_names_by_id=country_names_by_id,
        programs_by_country=programs_by_country,
        universities_by_country=universities_by_country,
        placement_conditions_by_country=placement_conditions_by_country,
    )

    summary = {
        "source_file": str(whed_file.resolve()),
        "output_dir": str(output_dir.resolve()),
        "institution_rows_scanned": len(institution_rows),
        **condition_stats,
        **program_stats,
        **university_stats,
        **write_stats,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[done] Institution rows scanned: {summary['institution_rows_scanned']}", flush=True)
    print(f"[done] Country JSON files written: {summary['country_json_files']}", flush=True)
    print(f"[done] Programs exported: {summary['program_unique_rows']}", flush=True)
    print(f"[done] Universities exported: {summary['university_rows_used']}", flush=True)
    print(f"[done] University programs exported: {summary['university_program_rows']}", flush=True)
    print(f"[done] Placement conditions exported: {summary['condition_rows_used']}", flush=True)
    print(f"[warn] Country misses (programs): {summary['program_country_misses']}", flush=True)
    print(f"[warn] Country misses (universities): {summary['university_country_misses']}", flush=True)
    print(f"[warn] City misses: {summary['university_city_misses']}", flush=True)
    print(
        f"[warn] Missing condition references in university_programs: {summary['condition_refs_missing_from_array']}",
        flush=True,
    )
    print(f"[done] Summary file: {(output_dir / 'summary.json').resolve()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
