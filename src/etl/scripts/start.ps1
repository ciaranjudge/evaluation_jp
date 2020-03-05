.\venv\Scripts\activate.bat
$env:PATH += ";.\venv"
python pipeline\wwld.py WWLDEtl --local-scheduler --config config.json