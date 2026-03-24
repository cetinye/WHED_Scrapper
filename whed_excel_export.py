from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, List

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from isced_f import classify_bachelors_cell, split_bachelor_programs, write_bachelor_program_map


SECTION_TITLES = [
    "General Information",
    "Officers",
    "Divisions",
    "Degrees",
    "Academic Periodicals",
    "Student & Staff Numbers",
]

SECTION_SET = set(SECTION_TITLES)
IAU_ID_RE = re.compile(r"IAU-\d+")
MULTISPACE_RE = re.compile(r"\s+")

OUTPUT_COLUMNS = [
    "Whed Link",
    "University Name",
    "IAU Code",
    "Native Name",
    "Country",
    "Street",
    "City",
    "Province",
    "Post Code",
    "Website",
    "Statistics Year",
    "Total Staff",
    "Total Student",
    "Institution Funding",
    "History",
    "Academic Year",
    "Admission Requirements",
    "Language(s)",
    "Accrediting Agency",
    "Student Body",
    "Permanent URL",
    "Student Statistics Year",
    "Staff Statistics Year",
    "Staff Full Time Total",
    "Staff Part Time Total",
    "Bachelor's Degree",
    "ISCED-F",
    "Master's Degree",
    "Doctor's Degree",
    "Diploma/Certificate",
    "General Information",
    "Officers",
    "Divisions",
    "Degrees",
    "Academic Periodicals",
    "Student & Staff Numbers",
    "Updated On",
    # "TXT File",
    "Raw Text",
]

ADDRESS_FIELD_MAP = {
    "street": "Street",
    "city": "City",
    "province": "Province",
    "post code": "Post Code",
    "post-code": "Post Code",
    "www": "Website",
}

GENERAL_FIELD_MAP = {
    "institution funding": "Institution Funding",
    "history": "History",
    "academic year": "Academic Year",
    "admission requirements": "Admission Requirements",
    "language(s)": "Language(s)",
    "languages": "Language(s)",
    "accrediting agency": "Accrediting Agency",
    "student body": "Student Body",
}

STUDENT_FIELD_MAP = {
    "statistics year": "Student Statistics Year",
    "total": "Total Student",
}

STAFF_FIELD_MAP = {
    "statistics year": "Staff Statistics Year",
    "total": "Total Staff",
    "full time total": "Staff Full Time Total",
    "full-time total": "Staff Full Time Total",
    "part time total": "Staff Part Time Total",
    "part-time total": "Staff Part Time Total",
}

DEGREE_PATTERNS = {
    "Bachelor's Degree": (
        "bachelor",
        "licenc",
        "licence",
        "licenciat",
        "bacharel",
        "bakalavr",
        "undergraduate",
    ),
    "Master's Degree": (
        "master",
        "magister",
        "maestr",
        "msc",
        "mba",
        "graduate",
    ),
    "Doctor's Degree": (
        "doctor",
        "doktor",
        "doctorat",
        "doctorado",
        "doctorate",
        "phd",
        "ph.d",
    ),
    "Diploma/Certificate": (
        "diploma",
        "certificate",
        "certificat",
        "sertifika",
        "post-bachelor",
        "post bachelor",
    ),
}

DEGREE_NON_SUBJECT_LABELS = {
    "note",
    "notes",
    "duration",
    "remarks",
    "remark",
}


def normalize_space(value: str) -> str:
    return MULTISPACE_RE.sub(" ", value or "").strip()


def append_value(record: Dict[str, str], key: str, value: str) -> None:
    value = normalize_space(value)
    if not value:
        return

    current = normalize_space(record.get(key, ""))
    if not current:
        record[key] = value
        return

    existing_parts = {part.strip() for part in current.split(" | ") if part.strip()}
    if value in existing_parts:
        return

    record[key] = f"{current} | {value}"


def split_subject_items(value: str) -> List[str]:
    items: List[str] = []
    for part in re.split(r"\s*[;,]\s*", normalize_space(value)):
        clean = normalize_space(part)
        if clean:
            items.append(clean)
    return items


def append_subject_values(record: Dict[str, str], key: str, value: str) -> None:
    new_items = split_subject_items(value)
    if not new_items:
        return

    current_items = split_subject_items(record.get(key, ""))
    seen = {item.casefold() for item in current_items}

    for item in new_items:
        lowered = item.casefold()
        if lowered not in seen:
            current_items.append(item)
            seen.add(lowered)

    record[key] = ", ".join(current_items)


def is_navigation_line(line: str) -> bool:
    return sum(1 for title in SECTION_TITLES if title in line) >= 2


def split_lines(text: str) -> List[str]:
    result: List[str] = []
    for raw_line in text.splitlines():
        line = normalize_space(raw_line)
        if line:
            result.append(line)
    return result


def split_key_value(line: str) -> tuple[str, str] | tuple[None, None]:
    if ":" not in line:
        return None, None
    key, value = line.split(":", 1)
    key = normalize_space(key)
    value = normalize_space(value)
    if not key:
        return None, None
    return key, value


def first_non_empty(values: Iterable[str]) -> str:
    for value in values:
        value = normalize_space(value)
        if value:
            return value
    return ""


def parse_general_information(lines: List[str], record: Dict[str, str]) -> None:
    active_field = ""
    buffer: List[str] = []

    def flush_active() -> None:
        nonlocal active_field, buffer
        if active_field and buffer:
            value = " ".join(buffer)
            append_value(record, active_field, value)
            if active_field == "Staff Full Time Total" and not normalize_space(record.get("Total Staff", "")):
                record["Total Staff"] = value
        active_field = ""
        buffer = []

    for line in lines:
        if line == "* * *":
            flush_active()
            continue

        if active_field and line.casefold().startswith(("http://", "https://")):
            buffer.append(line)
            continue

        if line.casefold() in GENERAL_FIELD_MAP:
            flush_active()
            active_field = GENERAL_FIELD_MAP[line.casefold()]
            continue

        key, value = split_key_value(line)
        if key is not None:
            flush_active()
            mapped_address = ADDRESS_FIELD_MAP.get(key.casefold())
            mapped_general = GENERAL_FIELD_MAP.get(key.casefold())
            target_field = mapped_address or mapped_general or key
            if mapped_address and value:
                append_value(record, mapped_address, value)
            elif mapped_general and value:
                append_value(record, mapped_general, value)
            elif value:
                append_value(record, key, value)
            else:
                active_field = target_field
            continue

        if active_field:
            buffer.append(line)

    flush_active()


def parse_student_staff(lines: List[str], record: Dict[str, str]) -> None:
    current_group = ""
    active_field = ""
    buffer: List[str] = []

    def flush_active() -> None:
        nonlocal active_field, buffer
        if active_field and buffer:
            append_value(record, active_field, " ".join(buffer))
        active_field = ""
        buffer = []

    for line in lines:
        if line == "* * *":
            flush_active()
            continue

        if line in {"Students", "Staff"}:
            flush_active()
            current_group = line
            continue

        key, value = split_key_value(line)
        if key is None:
            if active_field:
                buffer.append(line)
            continue

        flush_active()

        if current_group == "Students":
            mapped = STUDENT_FIELD_MAP.get(key.casefold())
            target_field = mapped or f"Students {key}"
            if mapped and value:
                append_value(record, mapped, value)
            elif value:
                append_value(record, f"Students {key}", value)
            else:
                active_field = target_field
        elif current_group == "Staff":
            mapped = STAFF_FIELD_MAP.get(key.casefold())
            target_field = mapped or f"Staff {key}"
            if mapped and value:
                append_value(record, mapped, value)
                if mapped == "Staff Full Time Total" and not normalize_space(record.get("Total Staff", "")):
                    record["Total Staff"] = value
            elif value:
                append_value(record, f"Staff {key}", value)
            else:
                active_field = target_field

    flush_active()


def categorize_degree(title: str) -> str:
    lowered = title.casefold()
    for column, patterns in DEGREE_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return column
    return ""


def is_degree_heading(line: str) -> bool:
    normalized = normalize_space(line)
    if not normalized or normalized.endswith(":"):
        return False

    return bool(
        re.match(
            r"^(bachelor|master|doctor|doktor|ph\.?d|diplom|diploma|certificate|certificat|"
            r"post-bachelor|also diploma|licenc|licence|licenciat|bacharel|bakalavr|"
            r"undergraduate|graduate|magister|maestr)",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def extract_inline_degree_subjects(title: str) -> str:
    cleaned = normalize_space(title)
    patterns = [
        r"^(?:Also\s+)?Diploma(?:/Certificate)?(?:\s+in)?\s+(.+)$",
        r"^(?:Post-bachelor'?s\s+)?Diploma/Certificate(?:\s+in)?\s+(.+)$",
        r"^Doctor of\s+(.+)$",
        r"^Master of\s+(.+)$",
        r"^Master in\s+(.+)$",
        r"^Bachelor of\s+(.+)$",
    ]
    for pattern in patterns:
        match = re.match(pattern, cleaned, flags=re.IGNORECASE)
        if match:
            return normalize_space(match.group(1))
    return ""


def parse_degrees(lines: List[str], record: Dict[str, str]) -> None:
    current_column = ""
    pending_label = ""
    pending_buffer: List[str] = []

    def flush_pending() -> None:
        nonlocal pending_label, pending_buffer
        if current_column and pending_label.casefold() == "fields of study" and pending_buffer:
            append_subject_values(record, current_column, " ".join(pending_buffer))
        pending_label = ""
        pending_buffer = []

    for line in lines:
        if line == "* * *":
            flush_pending()
            continue

        if current_column and pending_label.casefold() == "fields of study" and line.casefold() in DEGREE_NON_SUBJECT_LABELS:
            flush_pending()
            continue

        if current_column and pending_label.casefold() == "fields of study" and not is_degree_heading(line):
            pending_buffer.append(line)
            continue

        degree_column = categorize_degree(line) if is_degree_heading(line) else ""
        if degree_column:
            flush_pending()
            current_column = degree_column
            inline_subjects = extract_inline_degree_subjects(line)
            if inline_subjects:
                append_subject_values(record, current_column, inline_subjects)
            continue

        key, value = split_key_value(line)
        if key is not None:
            flush_pending()
            if current_column and key.casefold() == "fields of study":
                if value:
                    append_subject_values(record, current_column, value)
                else:
                    pending_label = key
            continue

        if current_column and pending_label.casefold() == "fields of study":
            pending_buffer.append(line)

    flush_pending()


def collect_sections(lines: List[str]) -> tuple[Dict[str, List[str]], str]:
    sections = {title: [] for title in SECTION_TITLES}
    updated_on = ""
    current_section = ""

    for line in lines:
        if is_navigation_line(line):
            continue

        if line in SECTION_SET:
            current_section = line
            continue

        if line in {"Students", "Staff"} and current_section in {"Degrees", "Academic Periodicals"}:
            current_section = "Student & Staff Numbers"
            sections[current_section].append(line)
            continue

        if line.startswith("Updated on "):
            updated_on = line.removeprefix("Updated on ").strip()
            current_section = ""
            continue

        if current_section:
            sections[current_section].append(line)

    return sections, updated_on


def parse_metadata(lines: List[str], record: Dict[str, str]) -> None:
    front_lines: List[str] = []

    for line in lines:
        if line.startswith("Source URL:"):
            record["Whed Link"] = line.split(":", 1)[1].strip()
            continue

        if line.startswith("Permanent URL:"):
            record["Permanent URL"] = line.split(":", 1)[1].strip()
            continue

        if line in SECTION_SET:
            break

        if is_navigation_line(line):
            continue

        front_lines.append(line)

    iau_id = ""
    for line in front_lines:
        match = IAU_ID_RE.search(line)
        if match:
            iau_id = match.group(0)
            break

    record["IAU Code"] = iau_id

    core_lines = [
        line
        for line in front_lines
        if not line.startswith("Source URL:")
        and not line.startswith("Permanent URL:")
        and not line.startswith("http://")
        and not line.startswith("https://")
    ]

    if core_lines:
        record["University Name"] = core_lines[0]

    if iau_id and iau_id in core_lines:
        id_index = core_lines.index(iau_id)
        between = [line for line in core_lines[1:id_index] if line != iau_id]
        if between:
            record["Native Name"] = " | ".join(between)

        if id_index + 1 < len(core_lines):
            record["Country"] = core_lines[id_index + 1]
    else:
        tail = core_lines[1:]
        if tail:
            record["Country"] = tail[-1]
            if len(tail) > 1:
                record["Native Name"] = " | ".join(tail[:-1])


def parse_txt_file(path: Path) -> Dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = split_lines(text)

    record: Dict[str, str] = {column: "" for column in OUTPUT_COLUMNS}
    record["TXT File"] = str(path.resolve())
    record["Raw Text"] = text.strip()

    parse_metadata(lines, record)
    sections, updated_on = collect_sections(lines)
    record["Updated On"] = updated_on

    parse_general_information(sections["General Information"], record)
    parse_student_staff(sections["Student & Staff Numbers"], record)
    parse_degrees(sections["Degrees"], record)
    record["ISCED-F"] = classify_bachelors_cell(record.get("Bachelor's Degree", ""))

    for title in SECTION_TITLES:
        record[title] = "\n".join(sections[title]).strip()

    if not record["Website"]:
        for candidate in lines:
            if candidate.startswith("WWW:"):
                record["Website"] = candidate.split(":", 1)[1].strip()
                break

    if not record["IAU Code"]:
        match = IAU_ID_RE.search(path.name)
        if match:
            record["IAU Code"] = match.group(0)

    if not record["University Name"]:
        record["University Name"] = path.stem

    student_year = normalize_space(record.get("Student Statistics Year", ""))
    staff_year = normalize_space(record.get("Staff Statistics Year", ""))
    if student_year and staff_year and student_year != staff_year:
        record["Statistics Year"] = f"{student_year} | {staff_year}"
    else:
        record["Statistics Year"] = student_year or staff_year

    return record


def autofit_worksheet(worksheet) -> None:
    width_overrides = {
        "Whed Link": 42,
        "University Name": 34,
        "Native Name": 34,
        "Street": 28,
        "Website": 28,
        "General Information": 40,
        "Divisions": 40,
        "Degrees": 40,
        "ISCED-F": 18,
        "Raw Text": 60,
    }

    for index, column_name in enumerate(OUTPUT_COLUMNS, start=1):
        letter = get_column_letter(index)
        if column_name in width_overrides:
            worksheet.column_dimensions[letter].width = width_overrides[column_name]
            continue

        max_length = len(column_name)
        for cell in worksheet[letter][: min(worksheet.max_row, 200)]:
            if cell.value is None:
                continue
            max_length = max(max_length, len(str(cell.value)))

        worksheet.column_dimensions[letter].width = min(max(max_length + 2, 12), 60)


def export_txt_directory_to_excel(input_dir: Path, output_file: Path) -> int:
    input_dir = Path(input_dir)
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Institutions"
    sheet.append(OUTPUT_COLUMNS)

    header_font = Font(bold=True)
    wrap_alignment = Alignment(vertical="top", wrap_text=True)

    for cell in sheet[1]:
        cell.font = header_font
        cell.alignment = wrap_alignment

    txt_files = sorted(input_dir.glob("*.txt"), key=lambda item: item.name.casefold())
    bachelor_programs: set[str] = set()

    for txt_file in txt_files:
        record = parse_txt_file(txt_file)
        bachelor_programs.update(split_bachelor_programs(record.get("Bachelor's Degree", "")))
        sheet.append([record.get(column, "") for column in OUTPUT_COLUMNS])

    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = wrap_alignment

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    # autofit_worksheet(sheet)
    workbook.save(output_file)
    write_bachelor_program_map(bachelor_programs)

    return len(txt_files)
