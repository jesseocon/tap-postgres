"""A decoder for Postgres's built-in pgoutput logical replication protocol (proto_version 1).

pgoutput is the plugin every managed Postgres ships (Cloud SQL, RDS, Aurora, Azure, Supabase, Neon);
it streams the changes of a publication's tables as binary messages. The decoder turns each Insert,
Update and Delete into the same change payload the wal2json path produces, so the rest of the
logical replication sync (bookmarks, feedback, records) is shared:

    {"kind": "insert" | "update", "schema", "table", "columnnames": [...], "columnvalues": [...]}
    {"kind": "delete", "schema", "table", "oldkeys": {"keynames": [...], "keyvalues": [...]}}

Relation messages (sent before a table's first change in a session, and after DDL) describe the
columns; Begin, Commit, Origin, Type, Truncate and Message carry no rows and yield None. Values come
as text and are turned into ints, floats and booleans by type oid so they match wal2json's JSON
values; everything else stays text (numeric, dates, json, arrays) and is converted downstream by
the column's SQL type, as for wal2json.

An unchanged TOASTed value ('u': a large value an UPDATE didn't touch, when the table's replica
identity isn't FULL) is left out of the row, so the target keeps what it has.
"""
import struct

BOOL = {16}
INTS = {20, 21, 23, 26}  # int8, int2, int4, oid
FLOATS = {700, 701}  # float4, float8


class PgOutputError(Exception):
    pass


class _Reader:
    def __init__(self, data):
        self.data = bytes(data)
        self.pos = 0

    def byte(self):
        value = self.data[self.pos:self.pos + 1]
        self.pos += 1
        return value.decode()

    def int8(self):
        value = self.data[self.pos]
        self.pos += 1
        return value

    def int16(self):
        (value,) = struct.unpack_from('!h', self.data, self.pos)
        self.pos += 2
        return value

    def int32(self):
        (value,) = struct.unpack_from('!i', self.data, self.pos)
        self.pos += 4
        return value

    def string(self):
        end = self.data.index(b'\x00', self.pos)
        value = self.data[self.pos:end].decode()
        self.pos = end + 1
        return value

    def raw(self, length):
        value = self.data[self.pos:self.pos + length]
        self.pos += length
        return value


def _value(text, type_oid):
    if text is None:
        return None
    if type_oid in BOOL:
        return text == 't'
    if type_oid in INTS:
        return int(text)
    if type_oid in FLOATS:
        if text in ('NaN', 'Infinity', '-Infinity'):
            return text
        return float(text)
    return text


class Relation:
    def __init__(self, schema, table, columns):
        self.schema = schema
        self.table = table
        # [(name, type_oid, is_key)]
        self.columns = columns


class Decoder:
    """Feed it each replication message's payload (bytes); it returns a change payload or None."""

    def __init__(self):
        self.relations = {}

    def _tuple(self, reader, relation):
        """(names, values) of a TupleData, leaving out unchanged TOAST values."""
        count = reader.int16()
        names, values = [], []
        for i in range(count):
            kind = reader.byte()
            name, type_oid, _key = relation.columns[i]
            if kind == 'n':
                names.append(name)
                values.append(None)
            elif kind == 'u':
                continue
            elif kind == 't':
                length = reader.int32()
                names.append(name)
                values.append(_value(reader.raw(length).decode(), type_oid))
            elif kind == 'b':
                raise PgOutputError(f'binary column values are not supported ({relation.schema}.{relation.table}.{name})')
            else:
                raise PgOutputError(f'unknown tuple column kind {kind!r}')
        return names, values

    def _relation(self, relid):
        relation = self.relations.get(relid)
        if relation is None:
            raise PgOutputError(f'change for relation {relid} before its Relation message')
        return relation

    def feed(self, payload):
        reader = _Reader(payload)
        kind = reader.byte()
        if kind == 'R':
            relid = reader.int32()
            schema = reader.string()
            table = reader.string()
            reader.int8()  # replica identity
            columns = []
            for _ in range(reader.int16()):
                flags = reader.int8()
                name = reader.string()
                type_oid = reader.int32()
                reader.int32()  # type modifier
                columns.append((name, type_oid, bool(flags & 1)))
            self.relations[relid] = Relation(schema, table or '', columns)
            return None
        if kind == 'I':
            relation = self._relation(reader.int32())
            if reader.byte() != 'N':
                raise PgOutputError('insert without a new tuple')
            names, values = self._tuple(reader, relation)
            return {'kind': 'insert', 'schema': relation.schema, 'table': relation.table,
                    'columnnames': names, 'columnvalues': values}
        if kind == 'U':
            relation = self._relation(reader.int32())
            marker = reader.byte()
            if marker in ('K', 'O'):
                self._tuple(reader, relation)  # the old key or row: the new tuple has the key
                marker = reader.byte()
            if marker != 'N':
                raise PgOutputError('update without a new tuple')
            names, values = self._tuple(reader, relation)
            return {'kind': 'update', 'schema': relation.schema, 'table': relation.table,
                    'columnnames': names, 'columnvalues': values}
        if kind == 'D':
            relation = self._relation(reader.int32())
            if reader.byte() not in ('K', 'O'):
                raise PgOutputError('delete without an old key')
            names, values = self._tuple(reader, relation)
            keys = {name for name, _oid, is_key in relation.columns if is_key}
            pairs = [(n, v) for n, v in zip(names, values) if n in keys] or list(zip(names, values))
            return {'kind': 'delete', 'schema': relation.schema, 'table': relation.table,
                    'oldkeys': {'keynames': [n for n, _ in pairs], 'keyvalues': [v for _, v in pairs]}}
        # B(egin), C(ommit), O(rigin), Y (type), T(runcate), M(essage): no rows.
        return None
