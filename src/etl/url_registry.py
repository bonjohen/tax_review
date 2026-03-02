"""Central registry of all IRS SOI download URLs.

All files sourced from IRS Statistics of Income (SOI), Publication 1304.
https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-returns-complete-report-publication-1304

SOCA (Sales of Capital Assets) data sourced from:
https://www.irs.gov/statistics/soi-tax-stats-sales-of-capital-assets-reported-on-individual-tax-returns
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
    2023: "rp-22-38.pdf",
}

YEARS = [2018, 2019, 2020, 2021, 2022]

# When 2023 SOI data is published, add 2023 to YEARS above.
# Excel files will follow the same {yy}{suffix}.xls pattern (e.g. 23in11si.xls).
# The Rev Proc PDF for TY2023 parameters (rp-22-38.pdf) is already registered.

# --- SOCA (Sales of Capital Assets) -------------------------------------------
# SOCA tables use .xlsx format and follow pattern: {yy}{suffix}.xlsx
# Available years: 2007–2015; we use the most recent three.

SOCA_YEARS = [2013, 2014, 2015]

_SOCA_TABLE_FILES = {
    "soca_t1": {"suffix": "in01soca", "desc": "ST/LT gains by asset type"},
    "soca_t2": {"suffix": "in02soca", "desc": "Gains by AGI & asset type"},
    "soca_t4": {"suffix": "in04soca", "desc": "Gains by holding duration"},
}

# SOI Bulletin article PDFs for SOCA cross-reference
_SOCA_BULLETIN_FILES = {
    2013: "13socabulletin.pdf",
    2014: "14socabulletin.pdf",
    2015: "15socabulletin.pdf",
}


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


# --- SOCA accessors -----------------------------------------------------------

def get_soca_files(year: int) -> dict[str, dict]:
    """Return dict of table_id -> {filename, url} for SOCA tables in a given year."""
    yy = str(year)[2:]
    result = {}
    for table_id, info in _SOCA_TABLE_FILES.items():
        filename = f"{yy}{info['suffix']}.xlsx"
        result[table_id] = {
            "filename": filename,
            "url": f"{IRS_SOI_BASE}/{filename}",
            "table_id": table_id,
            "year": year,
        }
    return result


def get_soca_bulletin(year: int) -> dict | None:
    """Return {filename, url} for the SOCA bulletin PDF, or None if unavailable."""
    filename = _SOCA_BULLETIN_FILES.get(year)
    if filename is None:
        return None
    return {
        "filename": filename,
        "url": f"{IRS_SOI_BASE}/{filename}",
        "year": year,
        "table_id": "soca_bulletin",
    }


def get_all_soca_downloads(years: list[int] | None = None) -> list[dict]:
    """Return flat list of all SOCA download entries."""
    years = years or SOCA_YEARS
    downloads = []
    for year in years:
        for entry in get_soca_files(year).values():
            downloads.append(entry)
        bulletin = get_soca_bulletin(year)
        if bulletin:
            downloads.append(bulletin)
    return downloads
