#!/bin/bash
set -e
pip install -r requirements.txt --quiet
exec uvicorn dbx_app.api:app --host 0.0.0.0 --port 8000
