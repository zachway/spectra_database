# The Spectra Pointer

A cross-match database and search tool that unifies stellar spectroscopy
holdings across more than 30 independent astronomical archives (ESO, SDSS,
Gaia, LAMOST, Keck/KOA, Gemini, and more) behind a single [Gaia DR3](https://www.cosmos.esa.int/web/gaia/dr3) `source_id`
lookup.

## Statement of need

Ground- and space-based spectroscopic surveys and archives each publish
their own holdings under their own conventions — different identifier
schemes, different metadata fields, different levels of positional
precision, some with public TAP/VO services and some without. As more 
spectroscopic data is observed, it becomes intractable for an
individual researcher to find out if a particular star has been observed.
This information is necessary for creating novel research, combing
through archival data, and writing proposals for time on large 
telescopes with limited budgets.

The Spectra Pointer solves this by running an independent sync process per
archive (41 are currently implemented) that discovers spectroscopic
observations and cross-matches each one to a canonical Gaia DR3 `source_id` 
— first by a named identifier when the archive publishes one (via
the [SIMBAD database](https://simbad.cds.unistra.fr/simbad/), falling back
to positional matching (via a [PostgreSQL q3c extension](https://github.com/segasai/q3c)) 
when it doesn't. The result is a single Postgres table (`spectroscopy_holdings`) that a
researcher can query by Gaia `source_id` or common name to get a
consolidated, deduplicated list of every archive holding a spectrum of
that star, along with pointers back to the original data in each home
archive. A public search webapp (built on this database) is deployed at
<https://spectra-pointer-997472993697.us-central1.run.app>. This is 
software is developed to be reusable infrastructure for anybody,
using no proprietary access and minimal personal keys.

## Architecture

```
sync/main.py            per-archive fetch → cross-match (sync/matcher.py) → Postgres
ingest/add_star.py      registers a star (Gaia source_id) to be tracked
db/schema.sql           Postgres schema (stars, spectroscopy_holdings, q3c indexing)
scripts/export_to_parquet.py   snapshots Postgres to a static Parquet dataset
webapp/app.py           read-only search UI, queries the Parquet snapshot via DuckDB
```

Purposefully, the sync layer never talks to the public web tier directly: `sync/main.py`
and `ingest/add_star.py` write to Postgres, `scripts/export_to_parquet.py`
periodically snapshots that database to Parquet, and `webapp/app.py` serves
search results from the snapshot (locally or over HTTP), so the public
webapp never holds live database credentials. This architecture synchronizes
well with Georgia State University's network, which has much storage
but cannot handle many requests. The Parquet snapshots also allow users
to effectively download the database directly, should they want to run
more complicated queries.

## Installation

Requires Python 3.9+ and PostgreSQL with the
[q3c](https://github.com/segasai/q3c) extension (used for indexed radial
positional cross-matching; not packaged for conda-forge or Homebrew as of
this writing, must be built from source).

```bash
git clone https://github.com/zachway/spectra_pointer.git
cd spectra_pointer
pip install -r requirements.txt

# create the database and schema
createdb spectra
psql spectra -f db/schema.sql

export DATABASE_URL=postgresql:///spectra
```

## Usage

Register a star to track (by Gaia DR3 `source_id`):

```bash
python -m ingest.add_star --source-id 2200433413577635456
```

Run every implemented archive sync to convergence (or a subset):

```bash
python -m sync.main                        # all implemented archives
python -m sync.main --only rave galah       # just these
```

Export the current database to a Parquet snapshot and run the search
webapp against it locally:

```bash
python3 -m scripts.export_to_parquet --out-dir ./data
SPECTRA_DATA_DIR=./data python3 -m webapp.app
```

See the module docstrings in `sync/main.py`, `ingest/add_star.py`, and
`webapp/app.py` for the full set of options and environment variables
(archive-specific auth, caching, and deployment configuration).

## Testing

```bash
pip install pytest
pytest tests/
```

## Deployment

The webapp is deployed on Google Cloud Run from the root `Dockerfile`:

```bash
gcloud run deploy spectra-pointer --source . --region us-central1 --allow-unauthenticated
```

## Contributing

Bug reports and pull requests are welcome via
[GitHub Issues](https://github.com/zachway/spectra_pointer/issues) and
[Pull Requests](https://github.com/zachway/spectra_pointer/pulls). If
you're adding support for a new archive, take a look at an existing
implementation under `sync/archives/` as a template — each one implements
a `fetch()` generator that the shared matcher and runner in `sync/` drive
to convergence.

## License

This project is licensed under the MIT License — see [LICENSE](LICENSE)
for details.
