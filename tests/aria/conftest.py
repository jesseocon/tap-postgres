"""A throwaway Postgres for the XMIN / read-only tests: initdb on a free port (or
TAP_POSTGRES_TEST_ADMIN_DSN), one database per test, and the tap run as a subprocess reading as
aria_sync, a role set up with the README's read-only grants.

Skipped when no Postgres binaries are found (set PG_BIN to a directory with initdb and pg_ctl).
"""
import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg2
import pytest

READER = 'aria_sync'
TAP = Path(sys.executable).parent / 'tap-postgres'


def _pg_bin():
    candidates = [os.environ.get('PG_BIN', '')]
    found = shutil.which('pg_ctl')
    if found:
        candidates.append(str(Path(found).parent))
    candidates += sorted(glob.glob('/opt/homebrew/opt/postgresql@*/bin'), reverse=True)
    candidates += sorted(glob.glob('/usr/lib/postgresql/*/bin'), reverse=True)
    for c in candidates:
        if c and (Path(c) / 'initdb').exists() and (Path(c) / 'pg_ctl').exists():
            return Path(c)
    return None


def _free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope='session')
def cluster(tmp_path_factory):
    bin_dir = _pg_bin()
    if bin_dir is None:
        pytest.skip('no Postgres binaries (set PG_BIN)')
    root = tmp_path_factory.mktemp('pg')
    data = root / 'data'
    # Unix socket paths are short (~100 bytes): a directory of its own under /tmp.
    sock = tempfile.mkdtemp(prefix='pg', dir='/tmp')
    port = _free_port()
    subprocess.run([bin_dir / 'initdb', '-D', data, '-U', 'postgres', '-A', 'trust', '--no-sync', '-E', 'UTF8'],
                   check=True, capture_output=True)
    subprocess.run([bin_dir / 'pg_ctl', '-D', data, '-l', root / 'log', '-w', 'start',
                    '-o', f"-p {port} -k {sock} -c listen_addresses=127.0.0.1 -c fsync=off -c max_connections=50"],
                   check=True, capture_output=True)
    admin = {'host': '127.0.0.1', 'port': port, 'user': 'postgres', 'dbname': 'postgres'}
    conn = psycopg2.connect(**admin)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE ROLE {READER} LOGIN PASSWORD 'x' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION")
    conn.close()
    try:
        yield admin
    finally:
        subprocess.run([bin_dir / 'pg_ctl', '-D', data, '-m', 'immediate', 'stop'], capture_output=True)
        shutil.rmtree(sock, ignore_errors=True)


class Db:
    def __init__(self, admin, name, work):
        self.admin = admin
        self.name = name
        self.work = work
        self.conn = psycopg2.connect(**dict(admin, dbname=name))
        self.conn.autocommit = True
        self.state = {}

    def sql(self, statement, params=None):
        with self.conn.cursor() as cur:
            cur.execute(statement, params)
            return cur.fetchall() if cur.description else []

    def config(self, **extra):
        cfg = {'host': self.admin['host'], 'port': self.admin['port'], 'user': READER, 'password': 'x',
               'dbname': self.name, 'filter_schemas': 'public', 'default_replication_method': 'XMIN',
               'stream_name_separator': '__'}
        cfg.update(extra)
        return cfg

    def _tap(self, args, config):
        cfg = self.work / 'config.json'
        cfg.write_text(json.dumps(config))
        out = subprocess.run([str(TAP), '--config', str(cfg), *args], capture_output=True, text=True)
        if out.returncode != 0:
            raise RuntimeError(out.stderr[-4000:])
        return out

    def discover(self, **extra):
        return json.loads(self._tap(['--discover'], self.config(**extra)).stdout)

    def sync(self, state=None, **extra):
        """Discover, select every stream, sync from `state` (default: the last sync's)."""
        catalog = self.discover(**extra)
        for stream in catalog['streams']:
            for entry in stream['metadata']:
                if entry['breadcrumb'] == []:
                    entry['metadata']['selected'] = True
        (self.work / 'catalog.json').write_text(json.dumps(catalog))
        (self.work / 'state.json').write_text(json.dumps(self.state if state is None else state))
        out = self._tap(['--catalog', str(self.work / 'catalog.json'), '--state', str(self.work / 'state.json')],
                        self.config(**extra))
        result = Result(out.stdout, out.stderr)
        if result.state is not None:
            self.state = result.state
        return result


class Result:
    def __init__(self, stdout, stderr):
        self.messages = [json.loads(line) for line in stdout.splitlines() if line.startswith('{')]
        self.stderr = stderr
        states = [m['value'] for m in self.messages if m['type'] == 'STATE']
        self.state = states[-1] if states else None

    def records(self, stream, key_only=None):
        rows = [m['record'] for m in self.messages if m['type'] == 'RECORD' and m['stream'] == stream]
        if key_only is not None:
            rows = [r for r in rows if bool(r.get('_sdc_key_only')) == key_only]
        return rows

    def schema(self, stream):
        return next(m for m in self.messages if m['type'] == 'SCHEMA' and m['stream'] == stream)

    def streams(self):
        return {m['stream'] for m in self.messages if m['type'] == 'SCHEMA'}


@pytest.fixture
def db(cluster, tmp_path):
    name = f't_{uuid.uuid4().hex[:10]}'
    conn = psycopg2.connect(**cluster)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE DATABASE {name}')
    conn.close()
    d = Db(cluster, name, tmp_path)
    # The README's read-only grants (simple version).
    d.sql(f'GRANT CONNECT ON DATABASE {name} TO {READER}')
    d.sql(f'GRANT USAGE ON SCHEMA public TO {READER}')
    d.sql(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO {READER}')
    d.sql(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {READER}')
    try:
        yield d
    finally:
        d.conn.close()
        conn = psycopg2.connect(**cluster)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS {name} WITH (FORCE)')
        conn.close()
