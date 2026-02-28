@echo off
pushd "%~dp0..\.."
call .venv\Scripts\activate
python -m src.parameters.extract_tax_params %*
popd
