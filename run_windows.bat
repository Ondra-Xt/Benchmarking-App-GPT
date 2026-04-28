@echo off

IF NOT EXIST .venv (
    echo Environment not found. Please run setup_windows.bat first.
    pause
    exit
)

call .venv\Scripts\activate
streamlit run app.py --server.address 0.0.0.0 --server.port 8502
pause