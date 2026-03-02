"""Tests for tax parameter extraction and persistence."""

import json

import pytest

from src.parameters.extract_tax_params import (
    extract_parameters,
    save_parameters,
    PARAMS_DIR,
    REV_PROC_FILES,
    FILING_STATUSES,
)


class TestSaveParameters:
    """Test JSON parameter persistence."""

    def test_save_creates_json(self, tmp_path):
        params = {
            "year": 2022,
            "source": "test.pdf",
            "ordinary_income_brackets": {},
            "standard_deduction": {"single": 12950},
        }
        path = save_parameters(params, tmp_path)
        assert path.exists()
        assert path.name == "2022_tax_parameters.json"

        loaded = json.loads(path.read_text())
        assert loaded["year"] == 2022
        assert loaded["standard_deduction"]["single"] == 12950

    def test_save_creates_directory(self, tmp_path):
        nested = tmp_path / "sub" / "dir"
        params = {"year": 2021, "source": "test.pdf"}
        path = save_parameters(params, nested)
        assert path.exists()

    def test_save_overwrites_existing(self, tmp_path):
        params = {"year": 2020, "source": "v1.pdf"}
        save_parameters(params, tmp_path)

        params["source"] = "v2.pdf"
        path = save_parameters(params, tmp_path)

        loaded = json.loads(path.read_text())
        assert loaded["source"] == "v2.pdf"


class TestExtractParameters:
    """Test PDF parameter extraction (currently a stub)."""

    @pytest.mark.parametrize("year", [2020, 2021, 2022])
    def test_extract_returns_structure(self, year):
        pdf_path = PARAMS_DIR / REV_PROC_FILES[year]
        if not pdf_path.exists():
            pytest.skip(f"PDF not found: {pdf_path}")

        params = extract_parameters(pdf_path, year)

        assert params["year"] == year
        assert params["source"] == REV_PROC_FILES[year]
        # Stub returns empty dicts for bracket fields
        assert isinstance(params["ordinary_income_brackets"], dict)
        assert isinstance(params["standard_deduction"], dict)


class TestConstants:
    def test_filing_statuses_complete(self):
        expected = {"single", "married_filing_jointly",
                    "married_filing_separately", "head_of_household"}
        assert set(FILING_STATUSES) == expected

    def test_rev_proc_files_have_expected_years(self):
        assert 2020 in REV_PROC_FILES
        assert 2021 in REV_PROC_FILES
        assert 2022 in REV_PROC_FILES
