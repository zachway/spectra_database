---
title: Spectra Database
emoji: 🔭
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
---

Search webapp for a multi-archive spectroscopy cross-match database. Reads a
Parquet snapshot published by `scripts/export_to_parquet.py` from the
Postgres database `sync/main.py` and `ingest/add_star.py` maintain — see
`webapp/app.py`'s module docstring for how to point it at a snapshot.
