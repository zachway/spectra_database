import os

import psycopg
import pytest

# Synthetic Gaia source_ids used across matcher tests, kept in a dedicated
# range so cleanup can't accidentally touch real data.
TEST_ID_LOW = 900000000000000000
TEST_ID_HIGH = 900000000000999999


@pytest.fixture
def conn():
    database_url = os.environ.get("DATABASE_URL", "postgresql:///spectra_test")
    connection = psycopg.connect(database_url)
    with connection.cursor() as cur:
        cur.execute(
            "INSERT INTO archives (archive_code, display_name) "
            "VALUES ('unit_test', 'Unit Test Archive') ON CONFLICT DO NOTHING"
        )
        cur.execute("DELETE FROM spectroscopy_holdings WHERE archive_code = 'unit_test'")
        cur.execute(
            "DELETE FROM stars WHERE gaia_source_id BETWEEN %s AND %s",
            (TEST_ID_LOW, TEST_ID_HIGH),
        )
    connection.commit()
    yield connection
    connection.close()
