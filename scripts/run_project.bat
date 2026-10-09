@echo off
REM Runs the whole Spark pipeline. Examples:
REM   scripts\run_project.bat            (full dataset, all stages)
REM   scripts\run_project.bat --sample   (synthetic smoke test)
REM   scripts\run_project.bat --stage preprocess
cd /d "%~dp0\.."
if exist .venv\Scripts\activate.bat call .venv\Scripts\activate.bat
python -m src.run_pipeline %*
