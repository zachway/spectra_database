#!/bin/bash

cd /nfs/morgan/users/way/spectra_database
source venv/bin/activate
DATABASE_URL=postgresql:///spectra_db?host=/tmp python3 -m scripts.export_to_parquet --out-dir ~/public_html/spectra_data
