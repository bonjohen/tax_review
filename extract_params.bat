@echo off
call .venv\Scripts\activate
python -m src.parameters.extract_tax_params %*
