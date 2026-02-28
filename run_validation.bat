@echo off
call .venv\Scripts\activate
python -m src.validation.reconcile %*
