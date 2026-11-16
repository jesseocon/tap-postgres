# tap-postgres (Aria)

Hotglue's fork (gitlab.com/hotglue/tap-postgres) of
[pipelinewise-tap-postgres](https://github.com/transferwise/pipelinewise-tap-postgres), with the
additions below. Licensed as upstream: GNU AGPLv3 (see `LICENSE`). It is the tap behind Aria's
`postgres` connector (`aria-singer-taps`, singer-connector), pinned by tag (`v1.8.3-aria.N`).
Everything upstream and Hotglue's fork do still works as before; the additions are opt-in config.

## What this fork adds

| Config | Default | What it does |
|---|---|---|
| `read_only` | `true` | Every session (but logical replication's) is READ ONLY: `SET SESSION default_transaction_read_only = on`, and every transaction `BEGIN ... READ ONLY`. A login that could write never does. |
| `default_replication_method: XMIN` (or a stream's `replication-method`) | | Incremental sync without a replication key or a slot: rows whose `xmin` is at or after the last read's snapshot (below). |
| `chunk_rows` | 10000 | Initial-load chunk: rows per short READ ONLY transaction, by primary key. |
| `xmin_chunk_keys` | 100000 | Incremental: keys walked per transaction while filtering on xmin. |
| `key_scan_interval_hours` | 24 | How often each table's primary keys are sent for delete detection (0: never). |
| `key_scan` | `false` | Send the keys this run, whatever the interval. |
| `key_scan_chunk_keys` | 100000 | Keys per transaction in a key scan. |
| `time_budget_seconds` | none | Stop starting new chunks after this long; the state says where to resume, and the next run starts with that table. |
| `standby_retries` | 4 | A chunk cancelled by a hot standby (SQLSTATE 40001 / 57014) or a dropped connection is retried this often, at half its size, with backoff. |
| `stream_name_separator` | none | `"__"` names streams `<schema>__<table>` (default, as Hotglue's fork: the table's name alone). |
| `include_tables` | none (every table) | Only these tables: names or fnmatch patterns on `table` or `schema.table` (list, or comma separated). Tables created later sync automatically when nothing narrows them. |
| `exclude_tables`, `exclude_schemas` | none | Never these, even if included: excluded tables are not even discovered. |
| `exclude_partitions` | `false` | Leave partitions out; their rows come through the partitioned parent. |
| `sslmode` | none | libpq `sslmode` (`require`, `verify-full`, ...); `ssl: "true"` still means `require`. |

Columns of types psycopg2 returns as text (`ltree`, `lquery`, `ltxtquery`, `xml`, `tsvector`, `tsquery`, `name`) sync as strings. Other
types the tap can't map (`point`, `interval`, `bytea`, ranges, ...) are left out, and discovery logs each one:
`<schema>.<table>.<column>: type <type> is not supported; the column is not synced`.

### XMIN replication

`tap_postgres/sync_strategies/xmin.py`. Per table, in the stream's bookmarks:

- **Initial load** (no `xmin_snapshot` yet): every row in chunks of `chunk_rows`, ordered by the
  primary key, each chunk its own REPEATABLE READ, READ ONLY transaction. `initial_load` keeps
  the first chunk's snapshot xmin and the last key read, so a run cut short resumes there.
- **Incremental**: rows whose 32-bit `xmin` lies in `[xmin_snapshot, snapshot xmax)` on the
  32-bit circle (one range, or two across 2^32; ids 0-2 never match). Bookmarks are 64-bit
  (`pg_snapshot_xmin(pg_current_snapshot())`, `txid_current_snapshot()` before Postgres 13); no
  call ever assigns a transaction id, so it runs on a hot standby. The new bookmark is the
  earliest snapshot xmin of the run, so a row can be read twice, never missed.
- **Re-sync**: a bookmark half the 32-bit circle behind the snapshot (wraparound) or far ahead of
  it (another cluster, a restore) starts a new initial load. A replica a little behind reads
  nothing this time.
- **Deletes**: a deleted row leaves nothing to read. Every `key_scan_interval_hours` the table's
  primary keys are sent as records holding only the key columns and `_sdc_key_only: true` (in the
  stream's SCHEMA as a nullable boolean). A loader can mark rows whose keys were not sent as
  deleted; aria-singer-taps sets `_aria_deleted_at`. Loaders that don't know the column get extra
  key-only rows, so leave `key_scan_interval_hours: 0` for them.
- **No primary key** (or a view): read whole each run in one READ ONLY transaction, retried only
  if it failed before the first row was sent.

## Read-only role

The connector reads with one login, `aria_sync`, that can only `SELECT`. Run either version on the
**primary**, as the role that owns the tables (on Neon, the default owner role, usually
`neondb_owner`), then give the connector the **read replica's** host, with `aria_sync` and its password
(direct, not pooled, `sslmode=require`).

### Simple version

Enough when all tables are in `public` and the role running it is the one that creates tables
(e.g. runs the migrations). Replace the password and the database name.

```sql
CREATE ROLE aria_sync LOGIN PASSWORD '<long random password>';
GRANT CONNECT ON DATABASE neondb TO aria_sync;
GRANT USAGE ON SCHEMA public TO aria_sync;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO aria_sync;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO aria_sync;
```

`ALTER DEFAULT PRIVILEGES` covers tables created later **by the role that ran it**. If another role
creates tables, run `ALTER DEFAULT PRIVILEGES FOR ROLE <that_role> IN SCHEMA public GRANT SELECT ON
TABLES TO aria_sync;` too.

### Complete version

One block for any database. Safe to re-run.

- Covers every non-system schema, not only `public`.
- Covers tables created later by every role that owns tables in each schema, plus the role running it.
- Creates the role once; each run sets a fresh generated password and re-applies the grants.
- Locks the role down: no superuser, no creating databases or roles, no replication.
- Ends with a small result: the password (shown once), `tables readable by aria_sync: N of N`, and a
  `WARNING` row naming the role to re-run as if a future-tables grant could not be set.

```sql
-- Read-only login for Aria's connector. Safe to re-run.
CREATE TEMP TABLE IF NOT EXISTS aria_sync_setup (item text, value text);
TRUNCATE aria_sync_setup;

DO $$
DECLARE
  pw     text := replace(gen_random_uuid()::text || gen_random_uuid()::text, '-', '');
  s      record;
  o      record;
BEGIN
  -- 1. The role: login only, nothing else. Created once; password set fresh each run.
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aria_sync') THEN
    CREATE ROLE aria_sync LOGIN;
  END IF;
  EXECUTE format('ALTER ROLE aria_sync LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %L', pw);
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO aria_sync', current_database());

  -- 2. Every non-system schema: read its tables and views now…
  FOR s IN SELECT oid, nspname FROM pg_namespace
           WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema' LOOP
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO aria_sync', s.nspname);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO aria_sync', s.nspname);

    -- …and tables created later, by whoever creates them: this role and every role that owns tables there.
    FOR o IN SELECT DISTINCT r FROM (
               SELECT current_user::text AS r
               UNION SELECT pg_get_userbyid(c.relowner) FROM pg_class c
               WHERE c.relnamespace = s.oid AND c.relkind IN ('r','p','v','m','f')) x LOOP
      BEGIN
        EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT ON TABLES TO aria_sync', o.r, s.nspname);
      EXCEPTION WHEN insufficient_privilege THEN
        INSERT INTO aria_sync_setup VALUES ('WARNING', format('could not cover future tables created by %s in %s: run this as %s', o.r, s.nspname, o.r));
      END;
    END LOOP;
  END LOOP;

  INSERT INTO aria_sync_setup VALUES ('password (save it now; shown once)', pw);
END $$;

-- 3. Proof: what aria_sync can read, against everything there is.
INSERT INTO aria_sync_setup
SELECT 'tables readable by aria_sync',
       count(*) FILTER (WHERE has_table_privilege('aria_sync', format('%I.%I', schemaname, tablename), 'SELECT'))
       || ' of ' || count(*)
FROM pg_tables WHERE schemaname NOT LIKE 'pg\_%' AND schemaname <> 'information_schema';

SELECT * FROM aria_sync_setup;
```

### Check the future-tables grants

```sql
SELECT pg_get_userbyid(defaclrole) AS for_tables_created_by,
       defaclnamespace::regnamespace AS in_schema,
       defaclacl
FROM pg_default_acl;
```

Each table owner should have a row whose ACL includes `aria_sync=r`.

---

# pipelinewise-tap-postgres

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-postgres.svg)](https://badge.fury.io/py/pipelinewise-tap-postgres)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-postgres.svg)](https://pypi.org/project/pipelinewise-tap-postgres/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [PostgreSQL](https://www.postgresql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Tap Postgres](https://transferwise.github.io/pipelinewise/connectors/taps/postgres.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).


It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-postgres
```

or

```bash
  make venv
```

### Create a config.json

```
{
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "password": "secret",
  "dbname": "db"
}
```

These are the same basic configuration properties used by the PostgreSQL command-line client (`psql`).

Full list of options in `config.json`:

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| host                                | String  | Yes        | PostgreSQL host                                               |
| port                                | Integer | Yes        | PostgreSQL port                                               |
| user                                | String  | Yes        | PostgreSQL user                                               |
| password                            | String  | Yes        | PostgreSQL password                                           |
| dbname                              | String  | Yes        | PostgreSQL database name                                      |
| filter_schemas                      | String  | No         | Comma separated schema names to scan only the required schemas to improve the performance of data extraction. (Default: None) |
| ssl                                 | String  | No         | If set to `"true"` then use SSL via postgres sslmode `require` option. If the server does not accept SSL connections or the client certificate is not recognized the connection will fail. (Default: None) |
| logical_poll_total_seconds          | Integer | No         | Stop running the tap when no data received from wal after certain number of seconds. (Default: 10800) |
| break_at_end_lsn                    | Boolean | No         | Stop running the tap if the newly received lsn is after the max lsn that was detected when the tap started. (Default: true) |
| max_run_seconds                     | Integer | No         | Stop running the tap after certain number of seconds. (Default: 43200) |
| debug_lsn                           | String  | No         | If set to `"true"` then add `_sdc_lsn` property to the singer messages to debug postgres LSN position in the WAL stream. (Default: None) |
| tap_id                              | String  | No         | ID of the pipeline/tap (Default: None) |
| itersize                            | Integer | No         | Size of PG cursor iterator when doing INCREMENTAL or FULL_TABLE  (Default: 20000) |
| default_replication_method          | String  | No         | Default replication method to use when no one is provided in the catalog (Values: `LOG_BASED`, `INCREMENTAL` or `FULL_TABLE`)  (Default: None) |
| use_secondary                       | Boolean | No         | Use a database replica for `INCREMENTAL` and `FULL_TABLE` replication (Default : False) |
| secondary_host                      | String  | No         | PostgreSQL Replica host (required if `use_secondary` is `True`) |
| secondary_port                      | Integer | No         | PostgreSQL Replica port (required if `use_secondary` is `True`) |


### Run the tap in Discovery Mode

```
tap-postgres --config config.json --discover                # Should dump a Catalog to stdout
tap-postgres --config config.json --discover > catalog.json # Capture the Catalog
```

### Add Metadata to the Catalog

Each entry under the Catalog's "stream" key will need the following metadata:

```
{
  "streams": [
    {
      "stream_name": "my_topic"
      "metadata": [{
        "breadcrumb": [],
        "metadata": {
          "selected": true,
          "replication-method": "LOG_BASED",
        }
      }]
    }
  ]
}
```

The replication method can be one of `FULL_TABLE`, `INCREMENTAL` or `LOG_BASED`.

**Note**: Log based replication requires a few adjustments in the source postgres database, please read further
for more information.

### Run the tap in Sync Mode

```
tap-postgres --config config.json --properties catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter
to the tap for the next sync.

### Log Based replication requirements

* PostgreSQL databases running **PostgreSQL versions 9.4.x or greater**. To avoid a critical PostgreSQL bug,
  use at least one of the following minor versions:
   - PostgreSQL 12.0
   - PostgreSQL 11.2
   - PostgreSQL 10.7
   - PostgreSQL 9.6.12
   - PostgreSQL 9.5.16
   - PostgreSQL 9.4.21

* **A connection to the master instance**. Log-based replication will only work by connecting to the master instance.

* **wal2json plugin**: To use Log Based for your PostgreSQL integration, you must install the wal2json plugin.
  The wal2json plugin outputs JSON objects for logical decoding, which the tap then uses to perform Log-based Replication.
  Steps for installing the plugin vary depending on your operating system. Instructions for each operating system type
  are in the wal2json’s GitHub repository:

  * [Unix-based operating systems](https://github.com/eulerto/wal2json#unix-based-operating-systems)
  * [Windows](https://github.com/eulerto/wal2json#windows)


* **postgres config file**: Locate the database configuration file (usually `postgresql.conf`) and define
  the parameters as follows:

    ```
    wal_level=logical
    max_replication_slots=5
    max_wal_senders=5
    ```

    Restart your PostgreSQL service to ensure the changes take effect.

    **Note**: For `max_replication_slots` and `max_wal_senders`, we’re defaulting to a value of 5.
    This should be sufficient unless you have a large number of read replicas connected to the master instance.


* **Existing replication slot**: Log based replication requires a dedicated logical replication slot.
  In PostgreSQL, a logical replication slot represents a stream of database changes that can then be replayed to a
  client in the order they were made on the original server. Each slot streams a sequence of changes from a single
  database.

  Login to the master instance as a superuser and using the `wal2json` plugin, create a logical replication slot:
  ```
    SELECT *
    FROM pg_create_logical_replication_slot('pipelinewise_<database_name>', 'wal2json');
  ```

  **Note**: Replication slots are specific to a given database in a cluster. If you want to connect multiple
  databases - whether in one integration or several - you’ll need to create a replication slot for each database.

### To run tests:

1. Install python test dependencies in a virtual env:
```
 make venv
```

2. You need to have a postgres database to run the tests and export its credentials:
```
  export TAP_POSTGRES_HOST=<postgres-host>
  export TAP_POSTGRES_PORT=<postgres-port>
  export TAP_POSTGRES_SECONDARY_HOST=<postgres-replica-host>
  export TAP_POSTGRES_SECONDARY_PORT=<postgres-replica-port>
  export TAP_POSTGRES_USER=<postgres-user>
  export TAP_POSTGRES_PASSWORD=<postgres-password>
```

You can make use of the local docker-compose to spin up a test database by running `make start_db`

Test objects will be created in the `postgres` database.

3. To run the tests:
```
  make test
```

### To run pylint:

1. Install python dependencies and run python linter
```
  make venv
  make pylint
```
