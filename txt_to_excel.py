import argparse
import sys
from pathlib import Path

from whed_excel_export import export_txt_directory_to_excel


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert all WHED TXT files inside a folder into a single Excel workbook."
    )
    parser.add_argument(
        "--input-dir",
        default="Data",
        help="Folder that contains institution TXT files.",
    )
    parser.add_argument(
        "--output-file",
        default="whed_data.xlsx",
        help="Excel file to create.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    count = export_txt_directory_to_excel(Path(args.input_dir), Path(args.output_file))
    print(f"[done] Exported {count} TXT file(s) to {Path(args.output_file).resolve()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
