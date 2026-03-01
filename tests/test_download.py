"""Tests for download infrastructure."""

from src.etl.url_registry import get_excel_files, get_pdf_file, get_all_downloads, YEARS


class TestUrlRegistry:
    def test_excel_files_per_year(self):
        for year in YEARS:
            files = get_excel_files(year)
            assert len(files) == 11, f"Expected 11 Excel files for {year}"

    def test_table_ids_present(self):
        expected_tables = {"1.1", "1.2", "1.3", "1.4", "1.4A", "1.6",
                           "3.2", "3.3", "3.4", "3.5", "3.6"}
        for year in YEARS:
            files = get_excel_files(year)
            assert set(files.keys()) == expected_tables

    def test_filenames_have_year_prefix(self):
        for year in YEARS:
            yy = str(year)[2:]
            files = get_excel_files(year)
            for table_id, info in files.items():
                assert info["filename"].startswith(yy), (
                    f"{info['filename']} should start with {yy}"
                )

    def test_pdf_files(self):
        for year in YEARS:
            pdf = get_pdf_file(year)
            assert pdf["filename"].endswith(".pdf")
            assert "irs-drop" in pdf["url"]

    def test_all_downloads_count(self):
        # 11 Excel files * 5 years + 5 PDFs = 60
        downloads = get_all_downloads()
        assert len(downloads) == 60
