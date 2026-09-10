# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# The helpers below are private to the package but consumed from __init__.py,
# which pyright's strict mode reports as unused.
# pyright: reportUnusedFunction=false

"""Helpers shared by the sync and async Valkey wrappers.

Everything in this module is a pure function over the Valkey client objects and
the stable database semantic conventions.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, cast

from valkey.exceptions import ResponseError

from opentelemetry.semconv.attributes.db_attributes import (
    DB_NAMESPACE,
)
from opentelemetry.semconv.attributes.network_attributes import (
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
    NETWORK_TRANSPORT,
    NetworkTransportValues,
)
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)

if TYPE_CHECKING:
    from opentelemetry.instrumentation.valkey.types import (
        AsyncPipelineInstance,
        AsyncValkeyInstance,
        PipelineInstance,
        ValkeyInstance,
    )
    from opentelemetry.trace import Span
    from opentelemetry.util.types import AttributeValue

# ``db.system.name`` has no generated enum member for Valkey: it is absent from
# the semantic conventions registry, both in the stable and in the incubating
# module. Track https://github.com/open-telemetry/semantic-conventions and swap
# this literal for ``DbSystemNameValues.VALKEY`` once the value is registered.
DB_SYSTEM_NAME_VALKEY = "valkey"

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 6379
_DEFAULT_NAMESPACE = "0"

_CMD_MAX_LEN = 1000
_VALUE_TOO_LONG_MARK = "..."

_FIELD_TYPES = ("NUMERIC", "TEXT", "GEO", "TAG", "VECTOR")

# https://opentelemetry.io/docs/specs/semconv/db/redis/ requires pipelined and
# transactional calls to be named MULTI or PIPELINE rather than the generic
# BATCH term used by the database conventions.
_MULTI_OPERATION_NAME = "MULTI"
_PIPELINE_OPERATION_NAME = "PIPELINE"

# Commands whose first argument names a Lua script or a function. EVAL and
# EVAL_RO are excluded on purpose: their first argument is the script body, not
# a name or a sha1 digest.
_STORED_PROCEDURE_COMMANDS = frozenset(
    {"EVALSHA", "EVALSHA_RO", "FCALL", "FCALL_RO"}
)

# Attributes valkey-py uses for the "this pipeline is a transaction" flag. The
# name differs between the sync client, the async client and .multi().
_TRANSACTION_FLAGS = ("transaction", "is_transaction", "explicit_transaction")

# Valkey server error replies start with an upper case error code, e.g.
# "WRONGTYPE Operation against a key holding the wrong kind of value".
_ERROR_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _format_command_args(args: tuple[Any, ...] | list[Any]) -> str:
    """Format and sanitize command arguments, and trim them as needed."""
    # Sanitized query format: "COMMAND ? ?"
    args_length = len(args)
    if args_length == 0:
        return ""

    out_str = " ".join([str(args[0])] + ["?"] * (args_length - 1))
    if len(out_str) > _CMD_MAX_LEN:
        out_str = out_str[: _CMD_MAX_LEN - len(_VALUE_TOO_LONG_MARK)] + _VALUE_TOO_LONG_MARK
    return out_str


def _get_connection_kwargs(
    instance: ValkeyInstance | AsyncValkeyInstance,
) -> dict[str, Any] | None:
    """Return the connection kwargs of a client, or ``None`` when unavailable.

    Cluster clients hold a node manager rather than a single connection pool,
    and clients built in tests may have a mocked pool, so callers must handle
    the attributes being absent.
    """
    connection_pool = getattr(instance, "connection_pool", None)
    connection_kwargs = getattr(connection_pool, "connection_kwargs", None)
    if isinstance(connection_kwargs, dict):
        return cast("dict[str, Any]", connection_kwargs)
    return None


def _extract_connection_attributes(
    connection_kwargs: dict[str, Any],
) -> dict[str, AttributeValue]:
    """Transform Valkey connection info into stable semconv attributes."""
    attributes: dict[str, AttributeValue] = {}

    db = connection_kwargs.get("db")
    # Written directly rather than through a helper because the default index 0
    # is falsy and must still be reported.
    attributes[DB_NAMESPACE] = _DEFAULT_NAMESPACE if db is None else str(db)

    # A non-cluster client talks to exactly one node, so the peer is always the
    # configured server; there is no separate node to resolve per operation.
    if "path" in connection_kwargs:
        path = connection_kwargs.get("path", "")
        attributes[SERVER_ADDRESS] = path
        attributes[NETWORK_PEER_ADDRESS] = path
        attributes[NETWORK_TRANSPORT] = NetworkTransportValues.UNIX.value
    else:
        host = connection_kwargs.get("host", _DEFAULT_HOST)
        port = int(connection_kwargs.get("port", _DEFAULT_PORT))
        attributes[SERVER_ADDRESS] = host
        attributes[SERVER_PORT] = port
        attributes[NETWORK_PEER_ADDRESS] = host
        attributes[NETWORK_PEER_PORT] = port
        attributes[NETWORK_TRANSPORT] = NetworkTransportValues.TCP.value

    return attributes


def _build_span_name(operation_name: str) -> str:
    """Build the span name from ``db.operation.name``.

    The Redis conventions exclude ``db.namespace`` from the span name because a
    numeric database index reads confusingly, which leaves the operation name as
    the whole name.
    """
    # A command always carries an operation, but fall back to the system name so
    # that a malformed call can never produce an empty span name.
    return operation_name or DB_SYSTEM_NAME_VALKEY


def _get_operation_name(args: tuple[Any, ...]) -> str:
    """Return ``db.operation.name`` for a single command."""
    if args and args[0]:
        return str(args[0])
    return ""


def _get_command_stack(
    instance: PipelineInstance | AsyncPipelineInstance,
) -> list[tuple[Any, ...]]:
    """Return the arguments of every command queued on a pipeline.

    ``Pipeline`` queues ``(args, options)`` tuples while ``ClusterPipeline``
    queues ``PipelineCommand`` objects, and the async cluster pipeline keeps
    them on a private attribute.
    """
    command_stack = getattr(instance, "command_stack", None)
    if command_stack is None:
        command_stack = getattr(instance, "_command_stack", None)
    if not command_stack:
        return []

    commands: list[tuple[Any, ...]] = []
    for command in command_stack:
        args = getattr(command, "args", None)
        if args is None:
            try:
                args = command[0]
            except (IndexError, KeyError, TypeError):
                continue
        commands.append(tuple(args))
    return commands


def _get_stored_procedure_name(args: tuple[Any, ...]) -> str | None:
    """Return ``db.stored_procedure.name`` for a Lua script or function call."""
    if len(args) < 2 or not args[0]:
        return None
    if str(args[0]).upper() not in _STORED_PROCEDURE_COMMANDS:
        return None
    return str(args[1])


def _is_transaction(instance: PipelineInstance | AsyncPipelineInstance) -> bool:
    """Return whether a pipeline is executed as a MULTI/EXEC transaction."""
    for flag in _TRANSACTION_FLAGS:
        value = getattr(instance, flag, False)
        # Valkey.transaction is also the name of a method, which the async
        # pipeline does not shadow with a flag, so only accept a real boolean.
        if isinstance(value, bool) and value:
            return True
    return False


def _get_shared_command(command_stack: list[tuple[Any, ...]]) -> str | None:
    """Return the command shared by every queued operation, if there is one."""
    if not command_stack:
        return None
    commands = {_get_operation_name(command) for command in command_stack}
    if len(commands) != 1:
        return None
    return commands.pop() or None


def _get_batch_operation_name(
    instance: PipelineInstance | AsyncPipelineInstance,
    command_stack: list[tuple[Any, ...]],
) -> str:
    """Return ``db.operation.name`` for a pipeline or transaction.

    The Redis conventions ask for ``MULTI`` or ``PIPELINE``, with the command
    prepended to it when every queued operation shares the same one.
    """
    name = _MULTI_OPERATION_NAME if _is_transaction(instance) else _PIPELINE_OPERATION_NAME
    shared_command = _get_shared_command(command_stack)
    if shared_command is None:
        return name
    return f"{name} {shared_command}"


def _get_batch_stored_procedure_name(
    command_stack: list[tuple[Any, ...]],
) -> str | None:
    """Return the stored procedure shared by every queued operation, if any."""
    names = {_get_stored_procedure_name(command) for command in command_stack}
    if len(names) != 1:
        return None
    return names.pop()


def _get_batch_query_text(command_stack: list[tuple[Any, ...]]) -> str:
    """Return ``db.query.text`` for a pipeline or transaction.

    Commands are joined with a newline, the separator the Redis CLI uses, and
    collapse to a single entry when every queued operation has the same text.
    """
    queries = [_format_command_args(command) for command in command_stack]
    if len(set(queries)) == 1:
        return queries[0]
    return "\n".join(queries)


def _get_error_status_code(exception: BaseException) -> str | None:
    """Return ``db.response.status_code``, i.e. the Valkey server error code."""
    if not isinstance(exception, ResponseError):
        return None
    message = str(exception).split(maxsplit=1)
    if message and _ERROR_CODE_PATTERN.match(message[0]):
        return message[0]
    return None


def _set_span_attribute_if_value(span: Span, name: str, value: AttributeValue | None) -> None:
    if value is not None and value != "":
        span.set_attribute(name, value)


def _value_or_none(values: Any, index: int) -> Any:
    try:
        return values[index]
    except (IndexError, KeyError, TypeError):
        return None


def _add_create_index_attributes(span: Span, args: tuple[Any, ...]) -> None:
    """Attach ``valkey.create_index.*`` attributes for an ``FT.CREATE`` command."""
    _set_span_attribute_if_value(span, "valkey.create_index.index", _value_or_none(args, 1))
    # The schema is the last argument of the command, see
    # https://github.com/valkey-io/valkey-py/blob/main/valkey/commands/search/commands.py
    try:
        schema_index = args.index("SCHEMA")
    except ValueError:
        return
    schema = args[schema_index:]
    # Schema in format:
    # [first_field_name, first_field_type, first_field_some_attribute1, ..., second_field_name, ...]
    field_attribute = "".join(
        f"Field(name: {schema[index - 1]}, type: {schema[index]});"
        for index in range(1, len(schema))
        if schema[index] in _FIELD_TYPES
    )
    _set_span_attribute_if_value(span, "valkey.create_index.fields", field_attribute)


def _add_search_attributes(span: Span, response: Any, args: tuple[Any, ...]) -> None:
    """Attach ``valkey.search.*`` attributes for an ``FT.SEARCH`` command."""
    _set_span_attribute_if_value(span, "valkey.search.index", _value_or_none(args, 1))
    _set_span_attribute_if_value(span, "valkey.search.query", _value_or_none(args, 2))
    # Response in format:
    # [number_of_returned_documents, index_of_first_returned_doc, first_doc(as a list), ...]
    # Returned documents in array format:
    # [first_field_name, first_field_value, second_field_name, second_field_value ...]
    number_of_returned_documents = _value_or_none(response, 0)
    _set_span_attribute_if_value(span, "valkey.search.total", number_of_returned_documents)
    if "NOCONTENT" in args or not number_of_returned_documents:
        return
    for document_number in range(number_of_returned_documents):
        document_index = _value_or_none(response, 1 + 2 * document_number)
        if document_index:
            document = response[2 + 2 * document_number]
            for attribute_name_index in range(0, len(document), 2):
                _set_span_attribute_if_value(
                    span,
                    f"valkey.search.xdoc_{document_index}.{document[attribute_name_index]}",
                    document[attribute_name_index + 1],
                )
