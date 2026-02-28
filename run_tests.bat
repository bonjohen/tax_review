@echo off
call .venv\Scripts\activate
pytest tests/ -v %*
