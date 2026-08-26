"""Copy a DeepSift SQLite database into an isolated PostgreSQL schema.

The default target schema is deliberately separate from ``public``. This tool
does not change the application's database connection or production traffic.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import struct
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


POSTGRES_URL_ENV = "POSTGRES_MIGRATION_URL"
DEFAULT_TARGET_SCHEMA = "deepsift_shadow"
OBJECT_NAME_LIMIT = 63
FINGERPRINT_MODULUS = 1 << 256
SAFE_SCHEMA_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
NUMERIC_DEFAULT_PATTERN = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")


class MigrationError(RuntimeError):
    """Raised when migration safety or fidelity checks fail."""


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    declared_type: str
    not_null: bool
    default_sql: str | None
    primary_key_position: int

    @property
    def postgres_type(self) -> str:
        return map_postgres_type(self.declared_type)


@dataclass(frozen=True)
class ForeignKeySchema:
    source_columns: tuple[str, ...]
    target_table: str
    target_columns: tuple[str, ...]
    on_update: str
    on_delete: str


@dataclass(frozen=True)
class IndexColumn:
    name: str | None
    descending: bool
    collation: str | None
    is_expression: bool


@dataclass(frozen=True)
class IndexSchema:
    name: str
    unique: bool
    origin: str
    partial: bool
    columns: tuple[IndexColumn, ...]
    original_sql: str | None

    @property
    def has_expression(self) -> bool:
        return any(column.is_expression for column in self.columns)


@dataclass(frozen=True)
class TableSchema:
    name: str
    columns: tuple[ColumnSchema, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKeySchema, ...]
    indexes: tuple[IndexSchema, ...]

    @property
    def identity_column(self) -> str | None:
        if len(self.primary_key) != 1:
            return None
        primary_name = self.primary_key[0]
        column = next(column for column in self.columns if column.name == primary_name)
        if "INT" not in (column.declared_type or "").upper():
            return None
        return primary_name


@dataclass
class DataFingerprint:
    row_count: int = 0
    xor_value: int = 0
    sum_value: int = 0

    def add(self, row: Sequence[Any]) -> None:
        digest = hashlib.sha256(encode_row(row)).digest()
        digest_int = int.from_bytes(digest, "big")
        self.row_count += 1
        self.xor_value ^= digest_int
        self.sum_value = (self.sum_value + digest_int) % FINGERPRINT_MODULUS

    def as_dict(self) -> dict[str, Any]:
        return {
            "row_count": self.row_count,
            "xor_sha256": f"{self.xor_value:064x}",
            "sum_sha256": f"{self.sum_value:064x}",
        }

    def matches(self, other: "DataFingerprint") -> bool:
        return self.as_dict() == other.as_dict()


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{utc_now_text()}] {message}", file=sys.stderr, flush=True)


def sqlite_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def sqlite_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def postgres_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def postgres_qualified_name(schema: str, name: str) -> str:
    return f"{postgres_identifier(schema)}.{postgres_identifier(name)}"


def safe_object_name(prefix: str, *parts: str) -> str:
    raw = "_".join([prefix, *[str(part) for part in parts if part]])
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", raw).strip("_") or prefix
    if len(cleaned) <= OBJECT_NAME_LIMIT:
        return cleaned
    suffix = hashlib.sha1(cleaned.encode("utf-8")).hexdigest()[:10]
    return f"{cleaned[: OBJECT_NAME_LIMIT - len(suffix) - 1]}_{suffix}"


def map_postgres_type(declared_type: str) -> str:
    declared = (declared_type or "").strip().upper()
    if "INT" in declared:
        return "BIGINT"
    if any(token in declared for token in ("CHAR", "CLOB", "TEXT")) or not declared:
        return "TEXT"
    if "BLOB" in declared:
        return "BYTEA"
    if any(token in declared for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE PRECISION"
    if any(token in declared for token in ("NUMERIC", "DECIMAL")):
        return "NUMERIC"
    if any(token in declared for token in ("DATE", "TIME")):
        return "TEXT"
    raise MigrationError(f"Unsupported SQLite declared type: {declared_type!r}")


def postgres_default(default_sql: str | None, target_type: str = "") -> str | None:
    if default_sql is None:
        return None
    value = str(default_sql).strip()
    while value.startswith("(") and value.endswith(")"):
        value = value[1:-1].strip()
    upper = value.upper()
    if upper == "CURRENT_TIMESTAMP" and target_type == "TEXT":
        return "(to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))"
    if upper == "CURRENT_DATE" and target_type == "TEXT":
        return "(to_char(CURRENT_DATE, 'YYYY-MM-DD'))"
    if upper == "CURRENT_TIME" and target_type == "TEXT":
        return "(to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'HH24:MI:SS'))"
    if upper in {"NULL", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}:
        return upper
    if NUMERIC_DEFAULT_PATTERN.fullmatch(value):
        return value
    if len(value) >= 2 and value[0] == value[-1] == "'":
        return value
    if len(value) >= 2 and value[0] == value[-1] == '"':
        inner = value[1:-1].replace('""', '"').replace("'", "''")
        return f"'{inner}'"
    raise MigrationError(f"Unsupported SQLite default expression: {default_sql!r}")


def _pragma_rows(connection: sqlite3.Connection, pragma: str, object_name: str) -> list[sqlite3.Row]:
    safe_name = sqlite_string_literal(object_name)
    return list(connection.execute(f"PRAGMA {pragma}({safe_name})").fetchall())


def inspect_sqlite_schema(connection: sqlite3.Connection) -> tuple[TableSchema, ...]:
    connection.row_factory = sqlite3.Row
    table_rows = connection.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    if not table_rows:
        raise MigrationError("The source SQLite database has no application tables.")

    columns_by_table: dict[str, tuple[ColumnSchema, ...]] = {}
    primary_keys: dict[str, tuple[str, ...]] = {}
    for table_row in table_rows:
        table_name = table_row["name"]
        column_rows = _pragma_rows(connection, "table_info", table_name)
        columns = tuple(
            ColumnSchema(
                name=row["name"],
                declared_type=row["type"] or "",
                not_null=bool(row["notnull"]),
                default_sql=row["dflt_value"],
                primary_key_position=int(row["pk"] or 0),
            )
            for row in column_rows
        )
        columns_by_table[table_name] = columns
        primary_keys[table_name] = tuple(
            column.name
            for column in sorted(columns, key=lambda item: item.primary_key_position or 10_000)
            if column.primary_key_position
        )

    schemas: list[TableSchema] = []
    for table_row in table_rows:
        table_name = table_row["name"]
        foreign_key_groups: dict[int, list[sqlite3.Row]] = defaultdict(list)
        for row in _pragma_rows(connection, "foreign_key_list", table_name):
            foreign_key_groups[int(row["id"])].append(row)
        foreign_keys: list[ForeignKeySchema] = []
        for group_id in sorted(foreign_key_groups):
            rows = sorted(foreign_key_groups[group_id], key=lambda item: int(item["seq"]))
            target_table = rows[0]["table"]
            target_columns = tuple(row["to"] for row in rows)
            if any(column is None for column in target_columns):
                target_columns = primary_keys.get(target_table, ())
            if len(target_columns) != len(rows):
                raise MigrationError(
                    f"Cannot resolve referenced columns for {table_name} foreign key {group_id}."
                )
            foreign_keys.append(
                ForeignKeySchema(
                    source_columns=tuple(row["from"] for row in rows),
                    target_table=target_table,
                    target_columns=tuple(str(column) for column in target_columns),
                    on_update=(rows[0]["on_update"] or "NO ACTION").upper(),
                    on_delete=(rows[0]["on_delete"] or "NO ACTION").upper(),
                )
            )

        indexes: list[IndexSchema] = []
        for index_row in _pragma_rows(connection, "index_list", table_name):
            origin = (index_row["origin"] or "c").lower()
            if origin == "pk":
                continue
            index_name = index_row["name"]
            index_columns = []
            for column_row in _pragma_rows(connection, "index_xinfo", index_name):
                if not int(column_row["key"]):
                    continue
                column_id = int(column_row["cid"])
                index_columns.append(
                    IndexColumn(
                        name=column_row["name"],
                        descending=bool(column_row["desc"]),
                        collation=column_row["coll"],
                        is_expression=column_id == -2,
                    )
                )
            original_row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
                (index_name,),
            ).fetchone()
            indexes.append(
                IndexSchema(
                    name=index_name,
                    unique=bool(index_row["unique"]),
                    origin=origin,
                    partial=bool(index_row["partial"]),
                    columns=tuple(index_columns),
                    original_sql=original_row["sql"] if original_row else None,
                )
            )

        schemas.append(
            TableSchema(
                name=table_name,
                columns=columns_by_table[table_name],
                primary_key=primary_keys[table_name],
                foreign_keys=tuple(foreign_keys),
                indexes=tuple(indexes),
            )
        )
    return tuple(schemas)


def create_table_sql(schema: str, table: TableSchema) -> str:
    identity_column = table.identity_column
    definitions: list[str] = []
    for column in table.columns:
        parts = [postgres_identifier(column.name), column.postgres_type]
        if column.name == identity_column:
            parts.extend(["GENERATED BY DEFAULT AS IDENTITY", "PRIMARY KEY"])
        if column.not_null and column.name != identity_column:
            parts.append("NOT NULL")
        default_value = postgres_default(column.default_sql, column.postgres_type)
        if default_value is not None:
            parts.extend(["DEFAULT", default_value])
        definitions.append(" ".join(parts))
    if table.primary_key and identity_column is None:
        primary_columns = ", ".join(postgres_identifier(name) for name in table.primary_key)
        definitions.append(f"PRIMARY KEY ({primary_columns})")
    body = ",\n    ".join(definitions)
    return f"CREATE TABLE {postgres_qualified_name(schema, table.name)} (\n    {body}\n)"


def _extract_index_body(original_sql: str) -> str:
    match = re.search(
        r"\bON\s+(?:\"(?:\"\"|[^\"])+\"|`[^`]+`|\[[^\]]+\]|[^\s(]+)\s*(\(.*)$",
        original_sql.strip(),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise MigrationError(f"Cannot parse SQLite index SQL: {original_sql}")
    body = match.group(1).strip()
    if re.search(r"\bCOLLATE\s+NOCASE\b", body, flags=re.IGNORECASE):
        raise MigrationError(f"SQLite NOCASE index needs an explicit PostgreSQL mapping: {original_sql}")
    return body


def create_index_sql(schema: str, table: TableSchema, index: IndexSchema) -> str:
    index_name = index.name
    if index.origin == "u" and index_name.startswith("sqlite_autoindex_"):
        column_names = [column.name or "expression" for column in index.columns]
        index_name = safe_object_name("uq", table.name, *column_names)
    unique_sql = "UNIQUE " if index.unique else ""
    if index.has_expression or index.partial:
        if not index.original_sql:
            raise MigrationError(f"Index {index.name} has no source SQL to translate.")
        body = _extract_index_body(index.original_sql)
    else:
        rendered_columns = []
        for column in index.columns:
            if not column.name:
                raise MigrationError(f"Index {index.name} contains an unresolved expression.")
            if column.collation and column.collation.upper() not in {"BINARY"}:
                raise MigrationError(
                    f"SQLite collation {column.collation!r} on index {index.name} "
                    "needs an explicit PostgreSQL mapping."
                )
            rendered = postgres_identifier(column.name)
            if column.descending:
                rendered += " DESC"
            rendered_columns.append(rendered)
        body = "(" + ", ".join(rendered_columns) + ")"
    return (
        f"CREATE {unique_sql}INDEX {postgres_identifier(safe_object_name(index_name))} "
        f"ON {postgres_qualified_name(schema, table.name)} {body}"
    )


def create_foreign_key_sql(
    schema: str,
    table: TableSchema,
    foreign_key: ForeignKeySchema,
    position: int,
) -> tuple[str, str]:
    constraint_name = safe_object_name(
        "fk",
        table.name,
        str(position),
        foreign_key.target_table,
    )
    source_columns = ", ".join(postgres_identifier(name) for name in foreign_key.source_columns)
    target_columns = ", ".join(postgres_identifier(name) for name in foreign_key.target_columns)
    statement = (
        f"ALTER TABLE {postgres_qualified_name(schema, table.name)} "
        f"ADD CONSTRAINT {postgres_identifier(constraint_name)} "
        f"FOREIGN KEY ({source_columns}) "
        f"REFERENCES {postgres_qualified_name(schema, foreign_key.target_table)} ({target_columns}) "
        f"ON UPDATE {foreign_key.on_update} ON DELETE {foreign_key.on_delete} NOT VALID"
    )
    return constraint_name, statement


def encode_value(value: Any) -> bytes:
    if value is None:
        return b"N"
    if isinstance(value, bool):
        return b"I" + str(int(value)).encode("ascii")
    if isinstance(value, int):
        return b"I" + str(value).encode("ascii")
    if isinstance(value, float):
        return b"F" + struct.pack(">d", value)
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytes):
        return b"B" + value
    if isinstance(value, str):
        return b"T" + value.encode("utf-8")
    return b"O" + str(value).encode("utf-8")


def encode_row(row: Sequence[Any]) -> bytes:
    output = bytearray()
    for value in row:
        encoded = encode_value(value)
        output.extend(len(encoded).to_bytes(8, "big"))
        output.extend(encoded)
    return bytes(output)


def coerce_value(column: ColumnSchema, value: Any) -> Any:
    if value is None:
        return None
    target_type = column.postgres_type
    if target_type == "TEXT":
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        else:
            value = str(value)
        if "\x00" in value:
            raise MigrationError(f"PostgreSQL TEXT cannot store NUL bytes in column {column.name}.")
        return value
    if target_type == "BIGINT":
        if isinstance(value, float) and not value.is_integer():
            raise MigrationError(f"Non-integral value {value!r} found in INTEGER column {column.name}.")
        return int(value)
    if target_type == "DOUBLE PRECISION":
        return float(value)
    if target_type == "BYTEA":
        if isinstance(value, memoryview):
            return value.tobytes()
        return bytes(value)
    return value


def coerce_row(table: TableSchema, row: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(coerce_value(column, value) for column, value in zip(table.columns, row))


def open_sqlite_readonly(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise MigrationError(f"SQLite source does not exist: {resolved}")
    connection = sqlite3.connect(resolved.as_uri() + "?mode=ro", uri=True, timeout=60)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def create_sqlite_snapshot(source_path: Path) -> Path:
    source = open_sqlite_readonly(source_path)
    temp_dir = Path(tempfile.mkdtemp(prefix="deepsift-pg-migration-"))
    snapshot_path = temp_dir / "crm-snapshot.db"
    destination = sqlite3.connect(snapshot_path)
    last_logged_pages = 0

    def progress(_status: int, remaining: int, total: int) -> None:
        nonlocal last_logged_pages
        copied = total - remaining
        if copied - last_logged_pages >= 16_384 or remaining == 0:
            log(f"SQLite snapshot: {copied}/{total} pages copied")
            last_logged_pages = copied

    try:
        source.backup(destination, pages=4096, progress=progress, sleep=0.05)
        destination.commit()
    finally:
        destination.close()
        source.close()
    return snapshot_path


def remove_snapshot(snapshot_path: Path | None) -> None:
    if not snapshot_path:
        return
    parent = snapshot_path.parent
    if parent.name.startswith("deepsift-pg-migration-"):
        shutil.rmtree(parent, ignore_errors=True)


def check_sqlite_foreign_keys(connection: sqlite3.Connection) -> None:
    violations = connection.execute("PRAGMA foreign_key_check").fetchmany(20)
    if violations:
        rendered = [tuple(row) for row in violations]
        raise MigrationError(f"SQLite source has foreign-key violations: {rendered}")


def sqlite_schema_hash(connection: sqlite3.Connection) -> str:
    rows = connection.execute(
        """
        SELECT type, name, COALESCE(sql, '') AS sql
        FROM sqlite_master
        WHERE name NOT LIKE 'sqlite_%'
        ORDER BY type, name
        """
    ).fetchall()
    payload = "\n".join("|".join(str(value) for value in row) for row in rows)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def require_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise MigrationError(
            "psycopg is required. Install project requirements before running the migration."
        ) from exc
    return psycopg


def inspect_target_tables(connection, schema: str) -> list[str]:
    rows = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    ).fetchall()
    return [row[0] for row in rows]


def ensure_target_schema(connection, schema: str, reset_schema: bool) -> None:
    if not SAFE_SCHEMA_PATTERN.fullmatch(schema):
        raise MigrationError(f"Unsafe PostgreSQL schema name: {schema!r}")
    if schema.lower() == "public":
        raise MigrationError("This shadow-copy tool refuses to write to the public schema.")
    existing_tables = inspect_target_tables(connection, schema)
    if existing_tables and not reset_schema:
        raise MigrationError(
            f"Target schema {schema!r} already contains {len(existing_tables)} tables. "
            "Use a new schema or explicitly pass --reset-schema."
        )
    if reset_schema:
        lowered = schema.lower()
        if not lowered.startswith("deepsift_") or not any(
            token in lowered for token in ("shadow", "migration", "staging")
        ):
            raise MigrationError(
                "--reset-schema is only allowed for clearly named DeepSift shadow schemas."
            )
        connection.execute(f"DROP SCHEMA IF EXISTS {postgres_identifier(schema)} CASCADE")
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {postgres_identifier(schema)}")
    connection.commit()


def create_target_tables(connection, schema: str, tables: Sequence[TableSchema]) -> None:
    for table in tables:
        connection.execute(create_table_sql(schema, table))
    connection.commit()


def copy_table_data(
    source: sqlite3.Connection,
    target,
    schema: str,
    table: TableSchema,
    batch_size: int,
) -> DataFingerprint:
    column_sql = ", ".join(sqlite_identifier(column.name) for column in table.columns)
    order_sql = ", ".join(sqlite_identifier(column) for column in table.primary_key)
    select_sql = f"SELECT {column_sql} FROM {sqlite_identifier(table.name)}"
    if order_sql:
        select_sql += f" ORDER BY {order_sql}"
    source_cursor = source.execute(select_sql)
    target_columns = ", ".join(postgres_identifier(column.name) for column in table.columns)
    copy_sql = f"COPY {postgres_qualified_name(schema, table.name)} ({target_columns}) FROM STDIN"
    fingerprint = DataFingerprint()
    with target.cursor() as target_cursor:
        with target_cursor.copy(copy_sql) as copy:
            while True:
                rows = source_cursor.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    coerced = coerce_row(table, tuple(row))
                    fingerprint.add(coerced)
                    copy.write_row(coerced)
    target.commit()
    return fingerprint


def reset_identity_sequences(connection, schema: str, tables: Sequence[TableSchema]) -> None:
    for table in tables:
        identity_column = table.identity_column
        if not identity_column:
            continue
        relation_name = postgres_qualified_name(schema, table.name)
        sequence_row = connection.execute(
            "SELECT pg_get_serial_sequence(%s, %s)",
            (relation_name, identity_column),
        ).fetchone()
        sequence_name = sequence_row[0] if sequence_row else None
        if not sequence_name:
            raise MigrationError(f"No identity sequence found for {relation_name}.{identity_column}")
        maximum = connection.execute(
            f"SELECT MAX({postgres_identifier(identity_column)}) FROM {relation_name}"
        ).fetchone()[0]
        if maximum is None:
            connection.execute("SELECT setval(%s::regclass, 1, false)", (sequence_name,))
        else:
            connection.execute("SELECT setval(%s::regclass, %s, true)", (sequence_name, maximum))
    connection.commit()


def create_target_indexes(connection, schema: str, tables: Sequence[TableSchema]) -> int:
    created = 0
    for table in tables:
        for index in table.indexes:
            connection.execute(create_index_sql(schema, table, index))
            created += 1
    connection.commit()
    return created


def create_and_validate_foreign_keys(connection, schema: str, tables: Sequence[TableSchema]) -> int:
    constraints: list[tuple[str, str]] = []
    for table in tables:
        for position, foreign_key in enumerate(table.foreign_keys, start=1):
            constraint_name, statement = create_foreign_key_sql(
                schema,
                table,
                foreign_key,
                position,
            )
            connection.execute(statement)
            constraints.append((table.name, constraint_name))
    connection.commit()
    for table_name, constraint_name in constraints:
        connection.execute(
            f"ALTER TABLE {postgres_qualified_name(schema, table_name)} "
            f"VALIDATE CONSTRAINT {postgres_identifier(constraint_name)}"
        )
    connection.commit()
    return len(constraints)


def fingerprint_postgres_table(
    connection,
    schema: str,
    table: TableSchema,
    batch_size: int,
) -> DataFingerprint:
    column_sql = ", ".join(postgres_identifier(column.name) for column in table.columns)
    order_sql = ", ".join(postgres_identifier(column) for column in table.primary_key)
    select_sql = f"SELECT {column_sql} FROM {postgres_qualified_name(schema, table.name)}"
    if order_sql:
        select_sql += f" ORDER BY {order_sql}"
    fingerprint = DataFingerprint()
    cursor_name = safe_object_name("validate", table.name, hashlib.sha1(table.name.encode()).hexdigest()[:8])
    with connection.cursor(name=cursor_name) as cursor:
        cursor.itersize = batch_size
        cursor.execute(select_sql)
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                fingerprint.add(coerce_row(table, row))
    return fingerprint


def validate_target_data(
    connection,
    schema: str,
    tables: Sequence[TableSchema],
    source_fingerprints: dict[str, DataFingerprint],
    batch_size: int,
) -> dict[str, Any]:
    validation: dict[str, Any] = {}
    failures = []
    for table in tables:
        target_fingerprint = fingerprint_postgres_table(connection, schema, table, batch_size)
        source_fingerprint = source_fingerprints[table.name]
        matches = source_fingerprint.matches(target_fingerprint)
        validation[table.name] = {
            "matches": matches,
            "source": source_fingerprint.as_dict(),
            "target": target_fingerprint.as_dict(),
        }
        if not matches:
            failures.append(table.name)
    connection.commit()
    if failures:
        raise MigrationError(f"PostgreSQL data fingerprints differ for tables: {failures}")
    return validation


def fingerprint_sqlite_tables(
    source: sqlite3.Connection,
    tables: Sequence[TableSchema],
    batch_size: int,
) -> dict[str, DataFingerprint]:
    fingerprints: dict[str, DataFingerprint] = {}
    for table in tables:
        column_sql = ", ".join(sqlite_identifier(column.name) for column in table.columns)
        order_sql = ", ".join(sqlite_identifier(column) for column in table.primary_key)
        query = f"SELECT {column_sql} FROM {sqlite_identifier(table.name)}"
        if order_sql:
            query += f" ORDER BY {order_sql}"
        cursor = source.execute(query)
        fingerprint = DataFingerprint()
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            for row in rows:
                fingerprint.add(coerce_row(table, tuple(row)))
        fingerprints[table.name] = fingerprint
    return fingerprints


def build_plan(source: sqlite3.Connection, tables: Sequence[TableSchema], source_path: Path) -> dict[str, Any]:
    return {
        "source_path": str(source_path),
        "source_size_bytes": source_path.stat().st_size,
        "sqlite_version": sqlite3.sqlite_version,
        "schema_sha256": sqlite_schema_hash(source),
        "table_count": len(tables),
        "index_count": sum(len(table.indexes) for table in tables),
        "foreign_key_count": sum(len(table.foreign_keys) for table in tables),
        "identity_table_count": sum(1 for table in tables if table.identity_column),
        "tables": [table.name for table in tables],
    }


def write_report(path: str | None, report: dict[str, Any]) -> None:
    if not path:
        return
    report_path = Path(path).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.monotonic()
    source_path = Path(args.source).expanduser().resolve()
    snapshot_path: Path | None = None
    if args.plan or args.no_snapshot:
        working_source_path = source_path
    else:
        log(f"Creating a consistent SQLite snapshot from {source_path}")
        snapshot_path = create_sqlite_snapshot(source_path)
        working_source_path = snapshot_path

    source = open_sqlite_readonly(working_source_path)
    try:
        check_sqlite_foreign_keys(source)
        tables = inspect_sqlite_schema(source)
        plan = build_plan(source, tables, source_path)
        if args.plan:
            return {"mode": "plan", "target_schema": args.schema, **plan}

        target_url = (os.getenv(args.target_url_env) or "").strip()
        if not target_url:
            raise MigrationError(
                f"Set {args.target_url_env} to the isolated PostgreSQL connection URL."
            )
        psycopg = require_psycopg()
        target = psycopg.connect(
            target_url,
            connect_timeout=args.connect_timeout,
            application_name="deepsift-postgres-migration",
        )
        try:
            server_version = target.execute("SHOW server_version").fetchone()[0]
            if args.validate_only:
                existing_tables = inspect_target_tables(target, args.schema)
                expected_tables = [table.name for table in tables]
                if existing_tables != expected_tables:
                    raise MigrationError(
                        "Target table inventory does not match SQLite source: "
                        f"expected {len(expected_tables)}, found {len(existing_tables)}."
                    )
                log("Computing source fingerprints for validation")
                source_fingerprints = fingerprint_sqlite_tables(source, tables, args.batch_size)
                validation = validate_target_data(
                    target,
                    args.schema,
                    tables,
                    source_fingerprints,
                    args.batch_size,
                )
                return {
                    "mode": "validate",
                    "status": "validated",
                    "target_schema": args.schema,
                    "postgres_version": server_version,
                    "source": plan,
                    "validation": validation,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                }

            ensure_target_schema(target, args.schema, args.reset_schema)
            log(f"Creating {len(tables)} tables in PostgreSQL schema {args.schema}")
            create_target_tables(target, args.schema, tables)
            source_fingerprints: dict[str, DataFingerprint] = {}
            for position, table in enumerate(tables, start=1):
                log(f"Copying table {position}/{len(tables)}: {table.name}")
                source_fingerprints[table.name] = copy_table_data(
                    source,
                    target,
                    args.schema,
                    table,
                    args.batch_size,
                )
            reset_identity_sequences(target, args.schema, tables)
            index_count = create_target_indexes(target, args.schema, tables)
            foreign_key_count = create_and_validate_foreign_keys(target, args.schema, tables)
            log("Validating per-table row counts and order-independent SHA-256 fingerprints")
            validation = validate_target_data(
                target,
                args.schema,
                tables,
                source_fingerprints,
                args.batch_size,
            )
            return {
                "mode": "migrate",
                "status": "validated",
                "target_schema": args.schema,
                "postgres_version": server_version,
                "source": plan,
                "created_indexes": index_count,
                "created_foreign_keys": foreign_key_count,
                "validation": validation,
                "elapsed_seconds": round(time.monotonic() - started, 3),
            }
        finally:
            target.close()
    finally:
        source.close()
        remove_snapshot(snapshot_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default=os.getenv("CRM_DB_PATH", "crm.db"),
        help="SQLite database path (default: CRM_DB_PATH or ./crm.db).",
    )
    parser.add_argument(
        "--schema",
        default=os.getenv("POSTGRES_MIGRATION_SCHEMA", DEFAULT_TARGET_SCHEMA),
        help=f"Isolated PostgreSQL schema (default: {DEFAULT_TARGET_SCHEMA}).",
    )
    parser.add_argument(
        "--target-url-env",
        default=POSTGRES_URL_ENV,
        help=f"Environment variable containing the target DSN (default: {POSTGRES_URL_ENV}).",
    )
    parser.add_argument("--batch-size", type=int, default=2_000)
    parser.add_argument("--connect-timeout", type=int, default=15)
    parser.add_argument("--report", help="Optional path for the JSON migration report.")
    parser.add_argument("--plan", action="store_true", help="Inspect SQLite without connecting to PostgreSQL.")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Recompute fingerprints against an existing target schema without writing data.",
    )
    parser.add_argument(
        "--reset-schema",
        action="store_true",
        help="Drop and recreate a clearly named DeepSift shadow schema before copying.",
    )
    parser.add_argument(
        "--no-snapshot",
        action="store_true",
        help="Read the source directly. Use only when the SQLite database is not receiving writes.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    try:
        report = run(args)
        write_report(args.report, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        error = {
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        print(json.dumps(error, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
