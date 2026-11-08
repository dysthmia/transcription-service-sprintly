"""Structured, stdout-only logging for the background worker."""

from __future__ import annotations

import logging
import re
import sys
import traceback
from collections.abc import Mapping, MutableMapping
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import structlog

LogFormat = Literal["json", "console"]

_CONTEXT_KEYS = frozenset(
    {
        "job_id",
        "correlation_id",
        "material_id",
        "attempt",
        "stage",
        "pipeline_version",
    }
)
_SENSITIVE_KEY_PARTS = (
    "password",
    "secret",
    "token",
    "authorization",
    "api_key",
    "access_key",
    "sasl",
)
_CONTENT_KEY_PARTS = (
    "body",
    "payload",
    "prompt",
    "transcript",
    "summary",
    "response",
    "file_content",
    "file_data",
)
_REDACTED = "[REDACTED]"
_SENSITIVE_TEXT = re.compile(
    r"(?i)\b(password|secret|token|authorization|api[_-]?key|access[_-]?key|sasl)"
    r"\s*([=:])\s*([^\n,;]+)"
)
_URL_IN_TEXT = re.compile(r"https?://[^\s\"']+")


def bind_log_context(**values: Any) -> None:
    """Bind job metadata to the current asyncio task's context.

    ``material_id`` must be a non-PII identifier. Context variables are copied
    when asyncio creates a task, so parallel jobs remain isolated.
    """
    unknown_keys = values.keys() - _CONTEXT_KEYS
    if unknown_keys:
        raise ValueError(f"Unsupported logging context keys: {sorted(unknown_keys)!r}")
    structlog.contextvars.bind_contextvars(**values)


def clear_log_context() -> None:
    """Clear all job metadata from the current context."""
    structlog.contextvars.clear_contextvars()


def configure_logging(
    log_level: str,
    *,
    log_format: LogFormat | None = None,
    service_name: str = "transcription-service",
    service_version: str = "unknown",
    app_env: str = "development",
) -> None:
    """Configure idempotent stdlib/structlog logging to stdout.

    Production defaults to JSON Lines. Development defaults to the console
    renderer unless ``LOG_FORMAT`` explicitly selects JSON.
    """
    level = logging.getLevelNamesMapping().get(log_level.upper())
    if level is None:
        raise ValueError(f"Unsupported log level: {log_level}")

    selected_format: LogFormat = log_format or (
        "json" if app_env.lower() == "production" else "console"
    )
    if selected_format not in {"json", "console"}:
        raise ValueError(f"Unsupported log format: {selected_format}")

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        _normalise_standard_library_event,
        _add_timestamp,
        _add_service_metadata(service_name, service_version, app_env),
        _format_exception,
        redact_event,
    ]
    renderer: structlog.types.Processor
    if selected_format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    # The application owns root logging. Replacing handlers prevents repeated
    # setup and legacy basicConfig handlers from emitting duplicate records.
    root_logger.handlers[:] = [handler]
    root_logger.setLevel(level)

    for logger_name in ("httpx", "httpcore", "aiokafka", "botocore", "sqlalchemy"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )


def _add_timestamp(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    event_dict["timestamp"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return event_dict


def _normalise_standard_library_event(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Give legacy stdlib records a stable event name and retain their text."""
    if event_dict.get("_from_structlog") is False:
        event_dict["message"] = str(event_dict.get("event", ""))
        event_dict["event"] = "stdlib_log"
    return event_dict


def _add_service_metadata(
    service_name: str, service_version: str, app_env: str
) -> structlog.types.Processor:
    def add_metadata(
        _: Any, __: str, event_dict: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        event_dict["service"] = service_name
        event_dict["environment"] = app_env
        event_dict["version"] = service_version
        event_dict.setdefault("event", "log_event")
        return event_dict

    return add_metadata


def _format_exception(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    exc_info = event_dict.pop("exc_info", None)
    if not exc_info:
        return event_dict

    exception_type, exception, exception_traceback = _normalise_exc_info(exc_info)
    if exception_type is None or exception is None:
        return event_dict

    rendered_traceback = "".join(
        traceback.format_exception(exception_type, exception, exception_traceback)
    )
    event_dict["exception"] = {
        "type": exception_type.__name__,
        "message": _redact_text(str(exception)),
        "traceback": _redact_text(rendered_traceback),
    }
    return event_dict


def _normalise_exc_info(
    exc_info: Any,
) -> tuple[type[BaseException] | None, BaseException | None, TracebackType | None]:
    if exc_info is True:
        exception_type, exception, exception_traceback = sys.exc_info()
    elif isinstance(exc_info, BaseException):
        exception_type = type(exc_info)
        exception = exc_info
        exception_traceback = exc_info.__traceback__
    else:
        exception_type, exception, exception_traceback = exc_info
    return exception_type, exception, exception_traceback


def redact_event(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Recursively remove credentials, unsafe content, and URL query strings."""
    return _redact_value(event_dict)


def _redact_value(value: Any, key: str | None = None) -> Any:
    if key is not None and _is_sensitive_key(key):
        return _REDACTED
    if key is not None and _is_content_key(key):
        return _REDACTED
    if isinstance(value, Mapping):
        return {
            str(item_key): _redact_value(item_value, str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    if isinstance(value, str):
        return _strip_query_string(_redact_text(value))
    return value


def _is_sensitive_key(key: str) -> bool:
    normalised = key.lower().replace("-", "_")
    return any(part in normalised for part in _SENSITIVE_KEY_PARTS)


def _is_content_key(key: str) -> bool:
    normalised = key.lower().replace("-", "_")
    return any(part in normalised for part in _CONTENT_KEY_PARTS)


def _redact_text(value: str) -> str:
    return _SENSITIVE_TEXT.sub(
        lambda match: f"{match.group(1)}{match.group(2)}{_REDACTED}", value
    )


def _strip_query_string(value: str) -> str:
    return _URL_IN_TEXT.sub(_strip_query_from_url, value)


def _strip_query_from_url(match: re.Match[str]) -> str:
    value = match.group(0)
    parsed = urlsplit(value)
    if parsed.scheme and parsed.netloc and parsed.query:
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", parsed.fragment))
    return value
