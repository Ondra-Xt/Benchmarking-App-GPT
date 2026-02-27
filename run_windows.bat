@echo off

IF NOT EXIST .venv (
    echo Environment not found. Please run setup_windows.bat first.
    pause
    exit
)

call .venv\Scripts\activate
streamlit run app.py