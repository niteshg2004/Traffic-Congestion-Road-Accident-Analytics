@echo off
REM Creates a virtual environment (.venv) and installs requirements (Windows).
cd /d "%~dp0\.."
where java >nul 2>nul || (echo Java not found. Install Java 11 or 17 first. & exit /b 1)
python --version
python -m venv .venv || exit /b 1
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt || exit /b 1
echo Setup complete. Next: scripts\run_project.bat --sample   (smoke test)  or place the dataset in data\raw\
