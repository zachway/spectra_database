# Spectra Database

Search webapp for a multi-archive spectroscopy cross-match database. Reads a
Parquet snapshot published by `scripts/export_to_parquet.py` from the
Postgres database `sync/main.py` and `ingest/add_star.py` maintain — see
`webapp/app.py`'s module docstring for how to point it at a snapshot.

Deployed on Google Cloud Run (project `spectra-database`, service
`spectra-database`, region `us-central1`) from the root `Dockerfile`:

```
gcloud run deploy spectra-database --source . --region us-central1 --allow-unauthenticated
```

The current version of this webapp is hosted at:

<a href="https://spectra-database-997472993697.us-central1.run.app">https://spectra-database-997472993697.us-central1.run.app</a>
