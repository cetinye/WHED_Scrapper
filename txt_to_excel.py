import argparse
import sys
from pathlib import Path

from whed_excel_export import export_txt_directory_outputs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert WHED TXT files into Excel exports, including relational database-style tables."
    )
    parser.add_argument(
        "--input-dir",
        default="Data",
        help="Folder that contains institution TXT files.",
    )
    parser.add_argument(
        "--output-file",
        default="whed_data.xlsx",
        help="Legacy all-in-one Excel file to create.",
    )
    parser.add_argument(
        "--skip-full-workbook",
        action="store_true",
        help="Skip generating the legacy all-in-one workbook.",
    )
    parser.add_argument(
        "--universities-file",
        default="universities.xlsx",
        help="Excel file to create for university records.",
    )
    parser.add_argument(
        "--programs-file",
        default="programs.xlsx",
        help="Excel file to create for unique programs.",
    )
    parser.add_argument(
        "--university-programs-file",
        default="university_programs.xlsx",
        help="Excel file to create for university-program pivot rows.",
    )
    parser.add_argument(
        "--enrichment-file",
        default=None,
        help="Optional enrichment JSONL file. Defaults to whed_enrichment.jsonl next to the outputs if present.",
    )
    parser.add_argument(
        "--program-source",
        choices=("all-degree-fields", "bachelors"),
        default="bachelors",
        help="Which parsed degree fields should feed the programs and university_programs outputs.",
    )
    parser.add_argument(
        "--all-countries",
        action="store_true",
        help="Include every parsed country instead of the current allowed-country filter.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    results = export_txt_directory_outputs(
        input_dir=Path(args.input_dir),
        output_file=None if args.skip_full_workbook else Path(args.output_file),
        universities_output_file=Path(args.universities_file),
        programs_output_file=Path(args.programs_file),
        university_programs_output_file=Path(args.university_programs_file),
        enrichment_file=Path(args.enrichment_file) if args.enrichment_file else None,
        include_all_countries=args.all_countries,
        program_source=args.program_source,
    )

    if "full_workbook" in results:
        print(f"[done] Full workbook rows: {results['full_workbook']} -> {Path(args.output_file).resolve()}", flush=True)
    print(f"[done] Universities rows: {results['universities']} -> {Path(args.universities_file).resolve()}", flush=True)
    print(f"[done] Programs rows: {results['programs']} -> {Path(args.programs_file).resolve()}", flush=True)
    print(
        f"[done] University-program rows: {results['university_programs']} -> {Path(args.university_programs_file).resolve()}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
