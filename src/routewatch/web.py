"""A small read-only web UI for RouteWatch, served with the standard library.

The server answers GET requests only. Pages and assets come from the ui/
folder; JSON comes from /api/. It connects to PostgreSQL as a separate
read-only role and to Kafka with read-only admin calls. Python's documentation
advises against http.server in production, so it listens on the loopback
address by default and is reached through an SSH tunnel when deployed.
"""

import json
import logging
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import psycopg
from dotenv import dotenv_values
from psycopg import errors

from . import kafkaview, webdata
from .config import (
    KafkaSettings,
    PostgresSettings,
    load_consumer_group_id,
    load_kafka_settings,
    load_postgres_settings,
)

logger = logging.getLogger(__name__)

DEFAULT_UI_USER = "routewatch_ui"
STATIC_SUFFIXES = {".html", ".css", ".js", ".svg", ".png", ".ico", ".txt"}
SUMMARY_SECONDS = 5.0
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; img-src 'self' data:; style-src 'self'; "
    "script-src 'self'; connect-src 'self'; object-src 'none'; "
    "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
)


@dataclass(frozen=True)
class UiSettings:
    postgres: PostgresSettings
    kafka: KafkaSettings
    group_id: str
    transform_version: int = 1
    static_dir: Path = field(default_factory=lambda: Path("ui"))


def load_ui_postgres_settings(env_file: str | Path = ".env") -> PostgresSettings:
    """The main connection settings, with the UI's own role and password."""
    base = load_postgres_settings(env_file)
    values = {**dotenv_values(env_file), **os.environ}

    user = (values.get("ROUTEWATCH_UI_POSTGRES_USER") or DEFAULT_UI_USER).strip()
    password = values.get("ROUTEWATCH_UI_POSTGRES_PASSWORD")
    if password is None or not password.strip():
        raise ValueError("ROUTEWATCH_UI_POSTGRES_PASSWORD must be set")
    if user == base.user:
        raise ValueError(
            "ROUTEWATCH_UI_POSTGRES_USER must differ from ROUTEWATCH_POSTGRES_USER"
        )
    return replace(base, user=user, password=password)


def load_ui_settings(
    env_file: str | Path = ".env",
    *,
    transform_version: int = 1,
    static_dir: Path,
) -> UiSettings:
    if transform_version <= 0:
        raise ValueError("transform_version must be positive")
    return UiSettings(
        postgres=load_ui_postgres_settings(env_file),
        kafka=load_kafka_settings(env_file),
        group_id=load_consumer_group_id(env_file),
        transform_version=transform_version,
        static_dir=static_dir,
    )


class ApiError(Exception):
    def __init__(self, status: HTTPStatus, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


class RouteWatchUi:
    """State shared by all requests: settings and a short summary cache."""

    def __init__(self, settings: UiSettings, *, allowed_hosts: set[str]) -> None:
        self.settings = settings
        self.allowed_hosts = allowed_hosts
        self._summary_lock = threading.Lock()
        self._summary: tuple[float, dict[str, Any]] | None = None

    def connect(self) -> psycopg.Connection:
        s = self.settings.postgres
        try:
            return psycopg.connect(
                host=s.host,
                port=s.port,
                dbname=s.dbname,
                user=s.user,
                password=s.password,
                connect_timeout=5,
                autocommit=True,
                application_name="routewatch-ui",
                options=(
                    "-c statement_timeout=5000"
                    " -c default_transaction_read_only=on"
                    " -c TimeZone=UTC"
                ),
            )
        except psycopg.OperationalError as error:
            raise ApiError(
                HTTPStatus.SERVICE_UNAVAILABLE,
                "postgres_unavailable",
                "PostgreSQL refused the UI's connection. Check that the stack is "
                "running and that scripts/init_routewatch_ui_role.py has been run. "
                f"Details: {_first_line(error)}",
            ) from error

    def with_database(self, work: Callable[[psycopg.Connection], Any]) -> Any:
        with self.connect() as connection:
            try:
                return work(connection)
            except errors.UndefinedTable as error:
                raise ApiError(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "tables_missing",
                    "A RouteWatch table does not exist yet. Run "
                    "scripts/init_routewatch_db.py, then start "
                    "scripts/clean_routewatch.py once to create the clean tables. "
                    f"Details: {_first_line(error)}",
                ) from error
            except errors.InsufficientPrivilege as error:
                raise ApiError(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "not_allowed",
                    "The UI role cannot read a RouteWatch table. Run "
                    "scripts/init_routewatch_ui_role.py again. "
                    f"Details: {_first_line(error)}",
                ) from error
            except errors.QueryCanceled as error:
                raise ApiError(
                    HTTPStatus.GATEWAY_TIMEOUT,
                    "query_timeout",
                    "The query took longer than 5 seconds. Narrow the filters "
                    "and try again.",
                ) from error

    def summary(self) -> dict[str, Any]:
        with self._summary_lock:
            if self._summary and monotonic() - self._summary[0] < SUMMARY_SECONDS:
                return self._summary[1]
        result = self.with_database(
            lambda connection: webdata.fetch_summary(
                connection,
                topic=self.settings.kafka.topic,
                transform_version=self.settings.transform_version,
            )
        )
        with self._summary_lock:
            self._summary = (monotonic(), result)
        return result


def _first_line(error: BaseException) -> str:
    text = str(error).strip()
    return text.splitlines()[0] if text else type(error).__name__


class RouteWatchRequestHandler(SimpleHTTPRequestHandler):
    server_version = "RouteWatchUI/0.1"
    sys_version = ""

    def __init__(self, *args: Any, app: RouteWatchUi, **kwargs: Any) -> None:
        self.app = app
        super().__init__(*args, directory=str(app.settings.static_dir), **kwargs)

    def version_string(self) -> str:
        return self.server_version

    # --- routing ----------------------------------------------------------

    def do_GET(self) -> None:
        if not self._host_allowed():
            return
        url = urlsplit(self.path)
        if url.path.startswith("/api/"):
            self._api(url.path, parse_qs(url.query, keep_blank_values=False))
        elif url.path == "/":
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "/data.html")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self._static_allowed(url.path):
            super().do_GET()
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_HEAD(self) -> None:
        if not self._host_allowed():
            return
        url = urlsplit(self.path)
        if url.path.startswith("/api/") or not self._static_allowed(url.path):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
        else:
            super().do_HEAD()

    def _host_allowed(self) -> bool:
        """Refuse other Host names, which guards against DNS rebinding."""
        host = (self.headers.get("Host") or "").strip().lower()
        if host in self.app.allowed_hosts:
            return True
        self.send_error(HTTPStatus.MISDIRECTED_REQUEST, "Unexpected Host header")
        return False

    @staticmethod
    def _static_allowed(path: str) -> bool:
        parts = [part for part in unquote(path).split("/") if part]
        if not parts or any(part.startswith(".") for part in parts):
            return False
        return Path(parts[-1]).suffix.lower() in STATIC_SUFFIXES

    def list_directory(self, path: str | os.PathLike[str]) -> None:
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    # --- API --------------------------------------------------------------

    def _api(self, path: str, params: dict[str, list[str]]) -> None:
        settings = self.app.settings
        topic = settings.kafka.topic
        version = settings.transform_version
        try:
            if path == "/api/summary":
                body = self.app.summary()
            elif path == "/api/raw":
                body = self.app.with_database(
                    lambda c: webdata.fetch_raw_page(
                        c, params, topic=topic, transform_version=version
                    )
                )
            elif path == "/api/raw/message":
                body = self.app.with_database(
                    lambda c: webdata.fetch_raw_message(
                        c, params, transform_version=version
                    )
                )
                if body is None:
                    raise ApiError(
                        HTTPStatus.NOT_FOUND,
                        "not_found",
                        "No raw message is stored at that Kafka position.",
                    )
            elif path == "/api/clean":
                body = self.app.with_database(
                    lambda c: webdata.fetch_clean_page(
                        c, params, topic=topic, transform_version=version
                    )
                )
            elif path == "/api/kafka":
                body = kafkaview.describe_kafka(
                    settings.kafka.bootstrap_servers,
                    group_id=settings.group_id,
                    include_internal=webdata.first(params, "internal") == "1",
                )
                body["pipeline_topic"] = topic
            elif path == "/api/kafka/peek":
                limit = webdata.whole_number(params, "limit", low=1, high=50) or 10
                body = kafkaview.peek_records(
                    settings.kafka.bootstrap_servers,
                    topic=webdata.first(params, "topic") or topic,
                    partition=webdata.whole_number(params, "partition", high=2**31 - 1)
                    or 0,
                    limit=limit,
                )
            else:
                raise ApiError(HTTPStatus.NOT_FOUND, "not_found", "No such endpoint.")
        except ApiError as error:
            self._json(error.status, {"error": error.code, "message": error.message})
        except (webdata.BadRequest, kafkaview.PeekRequestError) as error:
            self._json(
                HTTPStatus.BAD_REQUEST, {"error": "bad_request", "message": str(error)}
            )
        except kafkaview.KafkaUnavailable as error:
            self._json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "kafka_unavailable", "message": str(error)},
            )
        except Exception:
            logger.exception("Unexpected error in %s", path)
            self._json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "error": "internal",
                    "message": "Unexpected error; see the server log.",
                },
            )
        else:
            self._json(HTTPStatus.OK, body)

    def _json(self, status: HTTPStatus, body: Any) -> None:
        data = json.dumps(
            body, default=webdata.json_default, separators=(",", ":")
        ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    # --- headers and logging ---------------------------------------------

    def end_headers(self) -> None:
        self.send_header("Content-Security-Policy", CONTENT_SECURITY_POLICY)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("X-Frame-Options", "DENY")
        if not getattr(self, "path", "").startswith("/api/"):
            self.send_header("Cache-Control", "no-cache")
        super().end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s %s", self.address_string(), format % args)


def allowed_host_names(host: str, port: int, extra: list[str]) -> set[str]:
    names = {host, "localhost", "127.0.0.1", "[::1]", *extra}
    if ":" in host and not host.startswith("["):
        names.add(f"[{host}]")
    return {f"{name.lower()}:{port}" for name in names} | {
        name.lower() for name in extra
    }


def make_server(
    settings: UiSettings,
    *,
    host: str,
    port: int,
    extra_hosts: list[str] | None = None,
) -> ThreadingHTTPServer:
    if not settings.static_dir.is_dir():
        raise FileNotFoundError(f"UI folder not found: {settings.static_dir}")
    app = RouteWatchUi(
        settings, allowed_hosts=allowed_host_names(host, port, extra_hosts or [])
    )
    handler = partial(RouteWatchRequestHandler, app=app)
    return ThreadingHTTPServer((host, port), handler)
