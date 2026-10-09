@echo off
REM Starts the Streamlit dashboard (add --sample to view synthetic smoke-test results)
cd /d "%~dp0\.."
if exist .venv\Scripts\activate.bat call .venv\Scripts\activate.bat
if "%1"=="--sample" set CA3_SAMPLE=1
streamlit run dashboard\app.py
