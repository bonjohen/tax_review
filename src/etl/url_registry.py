"""Central registry of all IRS SOI download URLs.

All files sourced from IRS Statistics of Income (SOI), Publication 1304.
https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-returns-complete-report-publication-1304
"""

IRS_SOI_BASE = "https://www.irs.gov/pub/irs-soi"
IRS_DROP_BASE = "https://www.irs.gov/pub/irs-drop"

# Table suffix patterns: {YY}{suffix}.xls
_TABLE_SUFFIXES = {
    "1.1":  "in11si",
    "1.2":  "in12ms",
    "1.3":  "in13ms",
    "1.4":  "in14ar",
    "1.4A": "in14acg",
    "1.6":  "in16ag",
    "3.2":  "in32tt",
    "3.3":  "in33ar",
    "3.4":  "in34tr",
    "3.5":  "in35tr",
    "3.6":  "in36tr",
}

# Revenue Procedure PDFs for statutory tax parameters
_REV_PROC_FILES = {
    2018: "rp-18-22.pdf",
    2019: "rp-18-57.pdf",
    2020: "rp-19-44.pdf",
    2021: "rp-20-45.pdf",
    2022: "rp-21-45.pdf",
}

YEARS = [2018, 2019, 2020, 2021, 2022]


def get_excel_files(year: int) -> dict[str, dict]:
    """Return dict of table_id -> {filename, url} for a given tax year."""
    yy = str(year)[2:]
    result = {}
    for table_id, suffix in _TABLE_SUFFIXES.items():
        filename = f"{yy}{suffix}.xls"
        result[table_id] = {
            "filename": filename,
            "url": f"{IRS_SOI_BASE}/{filename}",
            "table_id": table_id,
            "year": year,
        }
    return result


def get_pdf_file(year: int) -> dict:
    """Return {filename, url} for the Revenue Procedure PDF for a given year."""
    filename = _REV_PROC_FILES[year]
    return {
        "filename": filename,
        "url": f"{IRS_DROP_BASE}/{filename}",
        "year": year,
        "table_id": "rev_proc",
    }


def get_all_downloads(years: list[int] | None = None) -> list[dict]:
    """Return flat list of all download entries across all years."""
    years = years or YEARS
    downloads = []
    for year in years:
        for entry in get_excel_files(year).values():
            downloads.append(entry)
        downloads.append(get_pdf_file(year))
    return downloads
