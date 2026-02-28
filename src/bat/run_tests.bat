@echo off
pushd "%~dp0..\.."
call .venv\Scripts\activate
pytest tests/ -v %*
popd
