"""Database compatibility layer for DeepSift's staged PostgreSQL migration."""

from __future__ import annotations

import atexit
import hashlib
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


SAFE_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
INSERT_TABLE_PATTERN = re.compile(
    r"^\s*INSERT\s+INTO\s+(?:(?:\"(?P<schema_q>(?:\"\"|[^\"])*)\"|(?P<schema_u>[A-Za-z_][A-Za-z0-9_]*))\s*\.\s*)?"
    r"(?:\"(?P<table_q>(?:\"\"|[^\"])*)\"|(?P<table_u>[A-Za-z_][A-Za-z0-9_]*))",
    flags=re.IGNORECASE | re.DOTALL,
)
DDL_PREFIX_PATTERN = re.compile(r"^\s*(?:CREATE\s+TABLE|ALTER\s+TABLE)\b", re.IGNORECASE)

_pool_lock = threading.Lock()
_postgres_pools: dict[str, Any] = {}
_identity_lock = threading.Lock()
_identity_tables: dict[str, set[str]] = {}


class DatabaseConfigurationError(RuntimeError):
    """Raised when a requested database backend is not safely configured."""


class SQLiteCompatibleRow:
    """Provide SQLite Row-style name and numeric access for PostgreSQL rows."""

    __slots__ = ("_keys", "_values", "_positions")

    def __init__(self, keys: Sequence[str], values: Sequence[Any]):
        self._keys = tuple(keys)
        self._values = tuple(values)
        self._positions = {key: index for index, key in enumerate(self._keys)}

    def __getitem__(self, key):
        if isinstance(key, (int, slice)):
            return self._values[key]
        return self._values[self._positions[key]]

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        return repr(dict(self))

    def keys(self) -> list[str]:
        return list(self._keys)

    def values(self) -> list[Any]:
        return list(self._values)

    def items(self) -> list[tuple[str, Any]]:
        return list(zip(self._keys, self._values))

    def get(self, key: str, default=None):
        position = self._positions.get(key)
        return default if position is None else self._values[position]


def sqlite_compatible_row_factory(cursor):
    columns = tuple(column.name for column in (cursor.description or ()))

    def make_row(values):
        return SQLiteCompatibleRow(columns, values)

    return make_row


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def configured_database_backend() -> str:
    backend = (os.getenv("CRM_DB_BACKEND") or "sqlite").strip().lower()
    if backend not in {"sqlite", "postgres", "postgresql"}:
        raise DatabaseConfigurationError(f"Unsupported CRM_DB_BACKEND: {backend!r}")
    return "postgres" if backend in {"postgres", "postgresql"} else "sqlite"


def is_postgres_backend() -> bool:
    return configured_database_backend() == "postgres"


def is_postgres_connection(connection) -> bool:
    return getattr(connection, "backend", "sqlite") == "postgres"


def recover_failed_database_transaction(connection) -> bool:
    """Roll back a PostgreSQL transaction only when it is already aborted."""
    if not is_postgres_connection(connection):
        return False
    raw = getattr(connection, "raw_connection", None)
    info = getattr(raw, "info", None)
    status = getattr(info, "transaction_status", None)
    if str(getattr(status, "name", "")).upper() != "INERROR":
        return False
    connection.rollback()
    return True


@contextmanager
def database_savepoint(connection, name: str):
    """Keep an optional PostgreSQL statement failure from poisoning its transaction."""
    if not is_postgres_connection(connection):
        yield
        return
    savepoint = _validate_identifier(name, "savepoint")
    quoted = postgres_identifier(savepoint)
    connection.execute(f"SAVEPOINT {quoted}")
    try:
        yield
    except Exception:
        connection.execute(f"ROLLBACK TO SAVEPOINT {quoted}")
        connection.execute(f"RELEASE SAVEPOINT {quoted}")
        raise
    else:
        connection.execute(f"RELEASE SAVEPOINT {quoted}")


def _validate_identifier(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not SAFE_IDENTIFIER_PATTERN.fullmatch(text):
        raise DatabaseConfigurationError(f"Unsafe {label}: {value!r}")
    return text


def postgres_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _pool_key(url: str, schema: str, max_size: int) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return f"{digest}:{schema}:{max_size}"


def _postgres_modules():
    try:
        import psycopg
        from psycopg_pool import ConnectionPool
    except ImportError as exc:
        raise DatabaseConfigurationError(
            "PostgreSQL mode requires psycopg and psycopg_pool. Install project requirements."
        ) from exc
    return psycopg, ConnectionPool


def _postgres_connect_kwargs(schema: str, connect_timeout: int) -> dict[str, Any]:
    return {
        "row_factory": sqlite_compatible_row_factory,
        "connect_timeout": max(1, int(connect_timeout)),
        "application_name": "deepsift",
        "options": f"-c search_path={schema},public -c timezone=UTC",
    }


def _get_postgres_pool(url: str, schema: str, connect_timeout: int):
    _psycopg, ConnectionPool = _postgres_modules()
    try:
        max_size = max(2, int(os.getenv("CRM_POSTGRES_POOL_MAX_SIZE", "12") or 12))
    except ValueError:
        max_size = 12
    try:
        min_size = max(0, min(max_size, int(os.getenv("CRM_POSTGRES_POOL_MIN_SIZE", "1") or 1)))
    except ValueError:
        min_size = 1
    key = _pool_key(url, schema, max_size)
    with _pool_lock:
        pool = _postgres_pools.get(key)
        if pool is None:
            pool = ConnectionPool(
                conninfo=url,
                min_size=min_size,
                max_size=max_size,
                kwargs=_postgres_connect_kwargs(schema, connect_timeout),
                open=True,
                name=f"deepsift-{schema}",
            )
            pool.wait(timeout=max(5, connect_timeout))
            _postgres_pools[key] = pool
    return key, pool


def close_postgres_pools() -> None:
    with _pool_lock:
        pools = list(_postgres_pools.values())
        _postgres_pools.clear()
    for pool in pools:
        try:
            pool.close()
        except Exception:
            pass


atexit.register(close_postgres_pools)


def _rewrite_code_segments(statement: str, transform) -> str:
    output: list[str] = []
    code: list[str] = []
    index = 0
    length = len(statement)

    def flush_code() -> None:
        if code:
            output.append(transform("".join(code)))
            code.clear()

    while index < length:
        char = statement[index]
        if char in {"'", '"'}:
            flush_code()
            quote = char
            literal = [char]
            index += 1
            while index < length:
                current = statement[index]
                literal.append(current)
                index += 1
                if current == quote:
                    if index < length and statement[index] == quote:
                        literal.append(statement[index])
                        index += 1
                        continue
                    break
            output.append("".join(literal))
            continue
        if statement.startswith("--", index):
            flush_code()
            end = statement.find("\n", index)
            if end < 0:
                output.append(statement[index:])
                index = length
            else:
                output.append(statement[index : end + 1])
                index = end + 1
            continue
        if statement.startswith("/*", index):
            flush_code()
            end = statement.find("*/", index + 2)
            if end < 0:
                output.append(statement[index:])
                index = length
            else:
                output.append(statement[index : end + 2])
                index = end + 2
            continue
        code.append(char)
        index += 1
    flush_code()
    return "".join(output)


def _translate_ddl(statement: str) -> str:
    def transform(code: str) -> str:
        translated = re.sub(
            r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b",
            "BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY",
            code,
            flags=re.IGNORECASE,
        )
        translated = re.sub(r"\bINTEGER\b", "BIGINT", translated, flags=re.IGNORECASE)
        translated = re.sub(r"\bREAL\b", "DOUBLE PRECISION", translated, flags=re.IGNORECASE)
        translated = re.sub(r"\bBLOB\b", "BYTEA", translated, flags=re.IGNORECASE)
        translated = re.sub(
            r"\bTEXT(\s+(?:NOT\s+NULL\s+)?)DEFAULT\s+CURRENT_TIMESTAMP\b",
            lambda match: (
                "TEXT"
                + (match.group(1) or " ")
                + "DEFAULT (to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', "
                + "'YYYY-MM-DD HH24:MI:SS'))"
            ),
            translated,
            flags=re.IGNORECASE,
        )
        return translated

    return _rewrite_code_segments(statement, transform)


def _translate_sqlite_functions(statement: str) -> str:
    def transform(code: str) -> str:
        translated = re.sub(r"\bdatetime\s*\(", "ds_datetime(", code, flags=re.IGNORECASE)
        translated = re.sub(r"\bdate\s*\(", "ds_date(", translated, flags=re.IGNORECASE)
        translated = re.sub(r"\bGROUP_CONCAT\s*\(", "STRING_AGG(", translated, flags=re.IGNORECASE)
        translated = re.sub(r"\bLIKE\b", "ILIKE", translated, flags=re.IGNORECASE)
        return translated

    return _rewrite_code_segments(statement, transform)


def _translate_insert_or_ignore(statement: str) -> str:
    if not re.match(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\b", statement, flags=re.IGNORECASE):
        return statement
    translated = re.sub(
        r"^(\s*)INSERT\s+OR\s+IGNORE\s+INTO\b",
        r"\1INSERT INTO",
        statement,
        count=1,
        flags=re.IGNORECASE,
    )
    if re.search(r"\bON\s+CONFLICT\b", translated, flags=re.IGNORECASE):
        return translated
    stripped = translated.rstrip()
    suffix = ";" if stripped.endswith(";") else ""
    if suffix:
        stripped = stripped[:-1].rstrip()
    return f"{stripped} ON CONFLICT DO NOTHING{suffix}"


def _convert_qmark_placeholders(statement: str, escape_percent: bool) -> tuple[str, int]:
    output: list[str] = []
    index = 0
    count = 0
    length = len(statement)
    quote: str | None = None
    line_comment = False
    block_comment = False
    while index < length:
        char = statement[index]
        next_char = statement[index + 1] if index + 1 < length else ""
        if line_comment:
            output.append("%%" if char == "%" and escape_percent else char)
            if char == "\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            output.append("%%" if char == "%" and escape_percent else char)
            if char == "*" and next_char == "/":
                output.append(next_char)
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote:
            output.append("%%" if char == "%" and escape_percent else char)
            if char == quote:
                if next_char == quote:
                    output.append(next_char)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            output.append(char)
            index += 1
            continue
        if char == "-" and next_char == "-":
            line_comment = True
            output.extend([char, next_char])
            index += 2
            continue
        if char == "/" and next_char == "*":
            block_comment = True
            output.extend([char, next_char])
            index += 2
            continue
        if char == "?":
            output.append("%s")
            count += 1
        elif char == "%" and escape_percent:
            output.append("%%")
        else:
            output.append(char)
        index += 1
    return "".join(output), count


def translate_sql(statement: str, has_parameters: bool = False) -> tuple[str, int]:
    translated = str(statement)
    translated = _translate_insert_or_ignore(translated)
    if DDL_PREFIX_PATTERN.match(translated):
        translated = _translate_ddl(translated)
    else:
        translated = _translate_sqlite_functions(translated)
    return _convert_qmark_placeholders(translated, escape_percent=has_parameters)


def split_sql_script(script: str) -> list[str]:
    script = str(script).lstrip("\ufeff")
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(script):
        char = script[index]
        next_char = script[index + 1] if index + 1 < len(script) else ""
        current.append(char)
        if quote:
            if char == quote:
                if next_char == quote:
                    current.append(next_char)
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"'}:
            quote = char
        elif char == ";":
            statement = "".join(current[:-1]).strip()
            if statement:
                statements.append(statement)
            current = []
        index += 1
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _normalize_params(params) -> tuple[Any, bool]:
    if params is None:
        return (), False
    if isinstance(params, dict):
        return params, bool(params)
    if isinstance(params, tuple):
        return params, bool(params)
    if isinstance(params, list):
        return tuple(params), bool(params)
    values = tuple(params)
    return values, bool(values)


def _extract_insert_table(statement: str) -> str | None:
    match = INSERT_TABLE_PATTERN.match(statement)
    if not match:
        return None
    quoted = match.group("table_q")
    return quoted.replace('""', '"') if quoted is not None else match.group("table_u")


def _append_returning_id(statement: str) -> str:
    stripped = statement.rstrip()
    if stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return f"{stripped} RETURNING {postgres_identifier('id')}"


class PostgresCursorAdapter:
    def __init__(self, connection: "PostgresConnectionAdapter", cursor):
        self._connection_adapter = connection
        self._cursor = cursor
        self.lastrowid = None

    @property
    def rowcount(self):
        return self._cursor.rowcount

    @property
    def description(self):
        return self._cursor.description

    def execute(self, statement: str, params=()):
        normalized_params, has_parameters = _normalize_params(params)
        translated, placeholder_count = translate_sql(statement, has_parameters=has_parameters)
        insert_table = _extract_insert_table(translated)
        capture_id = bool(
            insert_table
            and not re.search(r"\bRETURNING\b", translated, flags=re.IGNORECASE)
            and self._connection_adapter.table_has_identity(insert_table)
        )
        if capture_id:
            translated = _append_returning_id(translated)
        if has_parameters or placeholder_count:
            self._cursor.execute(translated, normalized_params)
        else:
            self._cursor.execute(translated)
        if capture_id:
            row = self._cursor.fetchone()
            self.lastrowid = row[0] if row else None
        return self

    def executemany(self, statement: str, params_seq: Iterable[Sequence[Any]]):
        translated, _placeholder_count = translate_sql(statement, has_parameters=True)
        self._cursor.executemany(translated, params_seq)
        self.lastrowid = None
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        return self._cursor.fetchmany(size) if size is not None else self._cursor.fetchmany()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        return self._cursor.close()

    def __iter__(self):
        return iter(self._cursor)


class PostgresConnectionAdapter:
    backend = "postgres"

    def __init__(self, connection, schema: str, *, pool=None, pool_key: str | None = None):
        self._connection = connection
        self._schema = schema
        self._pool = pool
        self._pool_key = pool_key or hashlib.sha256(schema.encode("utf-8")).hexdigest()
        self._closed = False
        self._load_identity_tables()

    @property
    def raw_connection(self):
        return self._connection

    @property
    def schema(self) -> str:
        return self._schema

    def _load_identity_tables(self, force: bool = False) -> set[str]:
        cache_key = f"{self._pool_key}:{self._schema}"
        with _identity_lock:
            if cache_key in _identity_tables and not force:
                return _identity_tables[cache_key]
        rows = self._connection.execute(
            """
            SELECT table_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND column_name = 'id'
              AND is_identity = 'YES'
            """,
            (self._schema,),
        ).fetchall()
        tables = {row[0] for row in rows}
        self._connection.commit()
        with _identity_lock:
            _identity_tables[cache_key] = tables
        return tables

    def refresh_identity_tables(self) -> set[str]:
        return self._load_identity_tables(force=True)

    def table_has_identity(self, table_name: str) -> bool:
        cache_key = f"{self._pool_key}:{self._schema}"
        with _identity_lock:
            cached = _identity_tables.get(cache_key, set())
            if table_name in cached:
                return True
        row = self._connection.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = 'id'
              AND is_identity = 'YES'
            LIMIT 1
            """,
            (self._schema, table_name),
        ).fetchone()
        if row:
            with _identity_lock:
                _identity_tables.setdefault(cache_key, set()).add(table_name)
            return True
        return False

    def cursor(self):
        return PostgresCursorAdapter(self, self._connection.cursor())

    def execute(self, statement: str, params=()):
        return self.cursor().execute(statement, params)

    def executemany(self, statement: str, params_seq: Iterable[Sequence[Any]]):
        return self.cursor().executemany(statement, params_seq)

    def executescript(self, script: str):
        last_cursor = None
        for statement in split_sql_script(script):
            if re.match(r"^\s*PRAGMA\b", statement, flags=re.IGNORECASE):
                continue
            last_cursor = self.execute(statement)
        return last_cursor

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._connection.rollback()
        except Exception:
            pass
        if self._pool is not None:
            self._pool.putconn(self._connection)
        else:
            self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()
        return False


def open_database_connection(
    sqlite_path: str | Path,
    *,
    sqlite_busy_timeout_ms: int = 60_000,
    sqlite_journal_size_limit_mb: int = 64,
    postgres_url: str | None = None,
    postgres_schema: str | None = None,
    postgres_connect_timeout: int = 15,
):
    if not is_postgres_backend():
        path = Path(sqlite_path).expanduser()
        busy_timeout_ms = max(1_000, int(sqlite_busy_timeout_ms))
        connection = sqlite3.connect(path, timeout=busy_timeout_ms / 1000)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA wal_autocheckpoint = 1000")
        journal_limit = max(8, int(sqlite_journal_size_limit_mb)) * 1024 * 1024
        connection.execute(f"PRAGMA journal_size_limit = {journal_limit}")
        return connection

    url = (postgres_url or os.getenv("CRM_POSTGRES_URL") or os.getenv("DATABASE_URL") or "").strip()
    if not url:
        raise DatabaseConfigurationError(
            "CRM_DB_BACKEND=postgres requires CRM_POSTGRES_URL or DATABASE_URL."
        )
    schema = _validate_identifier(
        postgres_schema or os.getenv("CRM_POSTGRES_SCHEMA") or "public",
        "PostgreSQL schema",
    )
    if env_flag("CRM_POSTGRES_POOL_ENABLED", True):
        pool_key, pool = _get_postgres_pool(url, schema, postgres_connect_timeout)
        try:
            timeout = max(1, int(os.getenv("CRM_POSTGRES_POOL_TIMEOUT_SECONDS", "15") or 15))
        except ValueError:
            timeout = 15
        connection = pool.getconn(timeout=timeout)
        return PostgresConnectionAdapter(connection, schema, pool=pool, pool_key=pool_key)

    psycopg, _ConnectionPool = _postgres_modules()
    connection = psycopg.connect(
        url,
        **_postgres_connect_kwargs(schema, postgres_connect_timeout),
    )
    direct_key = _pool_key(url, schema, 0)
    return PostgresConnectionAdapter(connection, schema, pool_key=direct_key)


def install_postgres_compatibility(connection) -> None:
    if not is_postgres_connection(connection):
        return
    schema = _validate_identifier(connection.schema, "PostgreSQL schema")
    qualified_parse = f"{postgres_identifier(schema)}.{postgres_identifier('ds_parse_datetime')}"
    qualified_datetime = f"{postgres_identifier(schema)}.{postgres_identifier('ds_datetime')}"
    qualified_date = f"{postgres_identifier(schema)}.{postgres_identifier('ds_date')}"
    raw = connection.raw_connection
    raw.execute(
        f"""
        CREATE OR REPLACE FUNCTION {qualified_parse}(value TEXT)
        RETURNS TIMESTAMP WITHOUT TIME ZONE
        LANGUAGE plpgsql
        STABLE
        AS $function$
        BEGIN
            IF value IS NULL OR btrim(value) = '' THEN
                RETURN NULL;
            END IF;
            IF lower(btrim(value)) = 'now' THEN
                RETURN CURRENT_TIMESTAMP AT TIME ZONE 'UTC';
            END IF;
            BEGIN
                RETURN CAST(value AS TIMESTAMP WITHOUT TIME ZONE);
            EXCEPTION WHEN OTHERS THEN
                RETURN NULL;
            END;
        END
        $function$
        """
    )
    raw.execute(
        f"""
        CREATE OR REPLACE FUNCTION {qualified_datetime}(value TEXT)
        RETURNS TEXT
        LANGUAGE SQL
        STABLE
        AS $function$
            SELECT CASE
                WHEN parsed IS NULL THEN NULL
                ELSE to_char(parsed, 'YYYY-MM-DD HH24:MI:SS')
            END
            FROM (SELECT {qualified_parse}(value) AS parsed) AS source
        $function$
        """
    )
    raw.execute(
        f"""
        CREATE OR REPLACE FUNCTION {qualified_datetime}(value TEXT, modifier TEXT)
        RETURNS TEXT
        LANGUAGE plpgsql
        STABLE
        AS $function$
        DECLARE
            parsed TIMESTAMP WITHOUT TIME ZONE;
        BEGIN
            parsed := {qualified_parse}(value);
            IF parsed IS NULL THEN
                RETURN NULL;
            END IF;
            IF modifier IS NOT NULL AND btrim(modifier) <> '' THEN
                BEGIN
                    parsed := parsed + CAST(modifier AS INTERVAL);
                EXCEPTION WHEN OTHERS THEN
                    RETURN NULL;
                END;
            END IF;
            RETURN to_char(parsed, 'YYYY-MM-DD HH24:MI:SS');
        END
        $function$
        """
    )
    raw.execute(
        f"""
        CREATE OR REPLACE FUNCTION {qualified_date}(value TEXT)
        RETURNS TEXT
        LANGUAGE SQL
        STABLE
        AS $function$
            SELECT CASE
                WHEN parsed IS NULL THEN NULL
                ELSE to_char(parsed, 'YYYY-MM-DD')
            END
            FROM (SELECT {qualified_parse}(value) AS parsed) AS source
        $function$
        """
    )
    raw.commit()


def database_table_columns(connection, table_name: str) -> set[str]:
    table = _validate_identifier(table_name, "table name")
    if is_postgres_connection(connection):
        rows = connection.execute(
            """
            SELECT column_name AS name
            FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
            """,
            (connection.schema, table),
        ).fetchall()
        return {str(row["name"]) for row in rows}
    rows = connection.execute(f"PRAGMA table_info({postgres_identifier(table)})").fetchall()
    return {str(row["name"]) for row in rows}


def is_database_operational_error(exc: BaseException) -> bool:
    if isinstance(exc, sqlite3.OperationalError):
        return True
    try:
        import psycopg

        return isinstance(exc, psycopg.OperationalError)
    except ImportError:
        return False


def is_database_integrity_error(exc: BaseException) -> bool:
    if isinstance(exc, sqlite3.IntegrityError):
        return True
    try:
        import psycopg

        return isinstance(exc, psycopg.IntegrityError)
    except ImportError:
        return False


def is_retryable_database_error(exc: BaseException) -> bool:
    if isinstance(exc, sqlite3.OperationalError):
        return "locked" in str(exc).lower() or "busy" in str(exc).lower()
    sqlstate = str(getattr(exc, "sqlstate", "") or "")
    if sqlstate in {"40001", "40P01", "55P03", "57P01", "57P02", "57P03"}:
        return True
    return is_database_operational_error(exc)
