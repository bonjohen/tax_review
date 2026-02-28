"""Extract statutory tax parameters from IRS Revenue Procedure PDFs.

Revenue Procedures contain bracket thresholds, standard deductions, and AMT
parameters in a predictable text structure. This module extracts them into
structured JSON files.

Usage:
    python -m src.parameters.extract_tax_params [--years 2020 2021 2022]
"""

import argparse
import json
import logging
from pathlib import Path

import pdfplumber

from src.etl.url_registry import YEARS

logger = logging.getLogger(__name__)

PARAMS_DIR = Path("data") / "parameters"

FILING_STATUSES = [
    "single",
    "married_filing_jointly",
    "married_filing_separately",
    "head_of_household",
]

# Revenue Procedure filenames by year
REV_PROC_FILES = {
    2020: "rp-19-44.pdf",
    2021: "rp-20-45.pdf",
    2022: "rp-21-45.pdf",
}


def extract_parameters(pdf_path: Path, year: int) -> dict:
    """Extract structured tax parameters from a Revenue Procedure PDF.

    Returns dict with keys:
    - year
    - source
    - ordinary_income_brackets: {filing_status: [{rate, threshold, upper}, ...]}
    - capital_gains_brackets: {filing_status: [{rate, threshold, upper}, ...]}
    - standard_deduction: {filing_status: amount}
    - amt_exemption: {filing_status: amount}
    - amt_phaseout: {filing_status: amount}
    """
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

    # TODO: Implement regex-based extraction from Revenue Procedure text.
    # The text has a predictable structure:
    # - Section .01: Tax Rate Tables (ordinary income brackets by filing status)
    # - Section on Standard Deduction amounts
    # - Section on AMT exemption and phaseout
    #
    # Fallback: if extraction fails, load from manually curated JSON files.

    params = {
        "year": year,
        "source": pdf_path.name,
        "ordinary_income_brackets": {},
        "capital_gains_brackets": {},
        "standard_deduction": {},
        "amt_exemption": {},
        "amt_phaseout": {},
    }

    logger.warning(
        f"Automated PDF extraction not yet implemented for {pdf_path.name}. "
        f"Please provide manually curated {year}_tax_parameters.json"
    )

    return params


def save_parameters(params: dict, output_dir: Path | None = None) -> Path:
    """Write parameters as {year}_tax_parameters.json."""
    output_dir = output_dir or PARAMS_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{params['year']}_tax_parameters.json"
    with open(output_path, "w") as f:
        json.dump(params, f, indent=2)
    logger.info(f"Wrote {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Extract tax parameters from Revenue Procedure PDFs")
    parser.add_argument("--years", nargs="+", type=int, default=YEARS)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    for year in args.years:
        pdf_path = PARAMS_DIR / REV_PROC_FILES[year]
        if not pdf_path.exists():
            logger.error(f"PDF not found: {pdf_path}. Run download first.")
            continue
        params = extract_parameters(pdf_path, year)
        save_parameters(params)


if __name__ == "__main__":
    main()
