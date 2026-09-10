# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""
Instrument `valkey`_ to report Valkey queries.

There are two options for instrumenting code. The first option is to use the
``opentelemetry-instrument`` executable which will automatically
instrument your Valkey client. The second is to programmatically enable
instrumentation via the following code:

.. _valkey: https://pypi.org/project/valkey/

Usage
-----

Instrument All Clients
**********************

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    import valkey

    # Instrument valkey
    ValkeyInstrumentor().instrument()

    # This will report a span with the default settings
    client = valkey.Valkey(host="localhost", port=6379)
    client.get("my-key")

    async def main():
        client = valkey.asyncio.Valkey(host="localhost", port=6379)
        await client.get("my-key")

    asyncio.run(main())

Instrument Single Client
************************

.. code:: python

    from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
    import valkey

    client = valkey.Valkey(host="localhost", port=6379)
    ValkeyInstrumentor().instrument_client(client)

    client.get("my-key")

Request/Response Hooks
**********************

.. code:: python

    def request_hook(span, instance, args, kwargs):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_request_hook", "some-value")

    def response_hook(span, instance, response):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_response_hook", "some-value")

    ValkeyInstrumentor().instrument(request_hook=request_hook, response_hook=response_hook)

Both hooks are invoked for single commands and for pipelines, and an exception
raised by a hook is logged and swallowed so that it never breaks the traced
call.

Suppress Instrumentation
************************

Instrumentation can be suppressed for a block of code, for example to avoid
tracing a client used by another instrumentation:

.. code:: python

    from opentelemetry.instrumentation.utils import suppress_instrumentation

    with suppress_instrumentation():
        client.get("my-key")

Semantic Conventions
--------------------

This instrumentation emits only the stable database, network and server
attribute conventions; there is no ``OTEL_SEMCONV_STABILITY_OPT_IN`` migration
mode. Alongside its spans it reports the ``db.client.operation.duration``
metric.

Its semconv status is nevertheless ``development``, because the Redis/Valkey
specific conventions are themselves still in development -- there is no
registered ``db.system.name`` value for Valkey, and ``db.redis.database_index``
was removed without a replacement. The attributes reported here may therefore
still change.

Two behaviours the conventions ask instrumentations to document explicitly:

* ``db.namespace`` reports the database index supplied when the connection was
  established. A connection's index can change later through ``SELECT``, but
  reading the current one would cost an extra round trip on every command, so
  the conventions sanction this fallback.
* ``error.type`` is the canonical name of the exception class raised by the
  client. When the server replied with an error, ``db.response.status_code``
  additionally carries the Valkey error prefix, such as ``WRONGTYPE`` or
  ``CLUSTERDOWN``.

API
---
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import valkey
import valkey.asyncio
from valkey.exceptions import WatchError

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import is_instrumentation_enabled, unwrap
from opentelemetry.instrumentation.valkey.metrics import (
    _create_duration_histogram,
    _extract_metric_attributes,
    _set_error_metric_attributes,
)
from opentelemetry.instrumentation.valkey.package import _instruments
from opentelemetry.instrumentation.valkey.utils import (
    DB_SYSTEM_NAME_VALKEY,
    _add_create_index_attributes,
    _add_search_attributes,
    _build_span_name,
    _extract_connection_attributes,
    _format_command_args,
    _get_batch_operation_name,
    _get_batch_query_text,
    _get_batch_stored_procedure_name,
    _get_command_stack,
    _get_connection_kwargs,
    _get_error_status_code,
    _get_operation_name,
    _get_stored_procedure_name,
)
from opentelemetry.instrumentation.valkey.version import __version__
from opentelemetry.metrics import get_meter
from opentelemetry.semconv.attributes.db_attributes import (
    DB_OPERATION_BATCH_SIZE,
    DB_OPERATION_NAME,
    DB_QUERY_TEXT,
    DB_RESPONSE_STATUS_CODE,
    DB_STORED_PROCEDURE_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.schemas import Schemas
from opentelemetry.trace import SpanKind, Status, StatusCode, get_tracer

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Collection, Iterator

    from opentelemetry.instrumentation.valkey.types import (
        RequestHook,
        ResponseHook,
    )
    from opentelemetry.metrics import Histogram, MeterProvider
    from opentelemetry.trace import Span, Tracer, TracerProvider
    from opentelemetry.util.types import AttributeValue

    # wrapt hands a wrapper the wrapped callable, the bound instance and the
    # call's positional and keyword arguments.
    _WrappedFunc = Callable[..., Any]
    _AsyncWrappedFunc = Callable[..., Awaitable[Any]]
    _Wrapper = Callable[[_WrappedFunc, Any, tuple[Any, ...], dict[str, Any]], Any]
    _AsyncWrapper = Callable[
        [_AsyncWrappedFunc, Any, tuple[Any, ...], dict[str, Any]],
        Awaitable[Any],
    ]

    # wrapt ships no type information, so declare the one function used here.
    def _wrap_function_wrapper(
        module: Any,
        name: str,
        wrapper: Callable[..., Any],
    ) -> None: ...

else:
    from wrapt import wrap_function_wrapper as _wrap_function_wrapper

_logger = logging.getLogger(__name__)

_SCHEMA_URL = Schemas.V1_25_0.value

_INSTRUMENTATION_ATTR = "_is_instrumented_by_opentelemetry"

_FT_CREATE_COMMAND = "FT.CREATE"
_FT_SEARCH_COMMAND = "FT.SEARCH"

# Wrap targets as (module, class name, method name) so that the same table
# drives both wrapping and unwrapping.
_COMMAND_TARGETS = (
    ("valkey", "Valkey", "execute_command"),
    ("valkey.client", "Pipeline", "immediate_execute_command"),
    ("valkey.cluster", "ValkeyCluster", "execute_command"),
)
_PIPELINE_TARGETS = (
    ("valkey.client", "Pipeline", "execute"),
    ("valkey.cluster", "ClusterPipeline", "execute"),
)
_ASYNC_COMMAND_TARGETS = (
    ("valkey.asyncio", "Valkey", "execute_command"),
    ("valkey.asyncio.client", "Pipeline", "immediate_execute_command"),
    ("valkey.asyncio.cluster", "ValkeyCluster", "execute_command"),
)
_ASYNC_PIPELINE_TARGETS = (
    ("valkey.asyncio.client", "Pipeline", "execute"),
    ("valkey.asyncio.cluster", "ClusterPipeline", "execute"),
)


def _execute_hook(hook: Callable[..., None], *args: Any) -> None:
    """Call a user supplied hook, logging and swallowing any exception."""
    try:
        hook(*args)
    except Exception:  # pylint: disable=broad-except
        _logger.warning("Exception raised by hook %r", hook, exc_info=True)


@dataclass
class _CallContext:
    """Carries the wrapped call's response back into the tracing helper."""

    span: Span
    response: Any = None


class _ValkeyTelemetry:
    """Span and metric handling shared by the sync and async wrappers."""

    def __init__(
        self,
        tracer: Tracer,
        duration_histogram: Histogram,
        request_hook: RequestHook | None = None,
        response_hook: ResponseHook | None = None,
    ) -> None:
        self._tracer = tracer
        self._duration_histogram = duration_histogram
        self._request_hook = request_hook
        self._response_hook = response_hook

    @contextmanager
    def trace_command(
        self,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Iterator[_CallContext]:
        """Trace a single Valkey command."""
        operation_name = _get_operation_name(args)
        attributes = self._build_attributes(instance, operation_name)
        attributes[DB_QUERY_TEXT] = _format_command_args(args)
        stored_procedure_name = _get_stored_procedure_name(args)
        if stored_procedure_name is not None:
            attributes[DB_STORED_PROCEDURE_NAME] = stored_procedure_name

        with self._trace(instance, operation_name, attributes, args, kwargs) as ctx:
            if operation_name == _FT_CREATE_COMMAND and ctx.span.is_recording():
                _add_create_index_attributes(ctx.span, args)
            yield ctx
            if operation_name == _FT_SEARCH_COMMAND and ctx.span.is_recording():
                _add_search_attributes(ctx.span, ctx.response, args)

    @contextmanager
    def trace_pipeline(
        self,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Iterator[_CallContext]:
        """Trace the execution of a Valkey pipeline."""
        command_stack = _get_command_stack(instance)
        operation_name = _get_batch_operation_name(instance, command_stack)
        attributes = self._build_attributes(instance, operation_name)
        attributes[DB_QUERY_TEXT] = _get_batch_query_text(command_stack)
        if len(command_stack) != 1:
            # A request holding a single operation is not a batch, while an
            # empty one still is and reports a size of zero.
            attributes[DB_OPERATION_BATCH_SIZE] = len(command_stack)
        stored_procedure_name = _get_batch_stored_procedure_name(command_stack)
        if stored_procedure_name is not None:
            attributes[DB_STORED_PROCEDURE_NAME] = stored_procedure_name

        with self._trace(instance, operation_name, attributes, args, kwargs) as ctx:
            yield ctx

    @staticmethod
    def _build_attributes(instance: Any, operation_name: str) -> dict[str, AttributeValue]:
        attributes: dict[str, AttributeValue] = {DB_SYSTEM_NAME: DB_SYSTEM_NAME_VALKEY}
        if operation_name:
            attributes[DB_OPERATION_NAME] = operation_name
        connection_kwargs = _get_connection_kwargs(instance)
        if connection_kwargs is not None:
            attributes.update(_extract_connection_attributes(connection_kwargs))
        return attributes

    @contextmanager
    def _trace(
        self,
        instance: Any,
        operation_name: str,
        attributes: dict[str, AttributeValue],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Iterator[_CallContext]:
        span_name = _build_span_name(operation_name)
        metric_attributes = _extract_metric_attributes(attributes)

        start_time = time.perf_counter()
        # Exceptions are recorded by hand so that a WatchError, which is control
        # flow rather than a failure, leaves the span status untouched.
        with self._tracer.start_as_current_span(
            span_name,
            kind=SpanKind.CLIENT,
            attributes=attributes,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            ctx = _CallContext(span=span)
            if self._request_hook is not None:
                _execute_hook(self._request_hook, span, instance, args, kwargs)
            try:
                yield ctx
            except WatchError:
                raise
            except BaseException as exc:  # pylint: disable=broad-except
                error_type = type(exc).__qualname__
                status_code = _get_error_status_code(exc)
                _set_error_metric_attributes(metric_attributes, error_type, status_code)
                self._record_error(span, exc, error_type, status_code)
                raise
            else:
                if self._response_hook is not None:
                    _execute_hook(self._response_hook, span, instance, ctx.response)
            finally:
                self._duration_histogram.record(
                    time.perf_counter() - start_time,
                    attributes=metric_attributes,
                )

    @staticmethod
    def _record_error(
        span: Span,
        exception: BaseException,
        error_type: str,
        status_code: str | None,
    ) -> None:
        if not span.is_recording():
            return
        span.set_attribute(ERROR_TYPE, error_type)
        if status_code is not None:
            span.set_attribute(DB_RESPONSE_STATUS_CODE, status_code)
        span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, str(exception)))


def _traced_execute_command_factory(telemetry: _ValkeyTelemetry) -> _Wrapper:
    def _traced_execute_command(
        func: _WrappedFunc,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled():
            return func(*args, **kwargs)
        with telemetry.trace_command(instance, args, kwargs) as ctx:
            ctx.response = func(*args, **kwargs)
        return ctx.response

    return _traced_execute_command


def _traced_execute_pipeline_factory(telemetry: _ValkeyTelemetry) -> _Wrapper:
    def _traced_execute_pipeline(
        func: _WrappedFunc,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled():
            return func(*args, **kwargs)
        with telemetry.trace_pipeline(instance, args, kwargs) as ctx:
            ctx.response = func(*args, **kwargs)
        return ctx.response

    return _traced_execute_pipeline


def _async_traced_execute_command_factory(
    telemetry: _ValkeyTelemetry,
) -> _AsyncWrapper:
    async def _async_traced_execute_command(
        func: _AsyncWrappedFunc,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled():
            return await func(*args, **kwargs)
        with telemetry.trace_command(instance, args, kwargs) as ctx:
            ctx.response = await func(*args, **kwargs)
        return ctx.response

    return _async_traced_execute_command


def _async_traced_execute_pipeline_factory(
    telemetry: _ValkeyTelemetry,
) -> _AsyncWrapper:
    async def _async_traced_execute_pipeline(
        func: _AsyncWrappedFunc,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        if not is_instrumentation_enabled():
            return await func(*args, **kwargs)
        with telemetry.trace_pipeline(instance, args, kwargs) as ctx:
            ctx.response = await func(*args, **kwargs)
        return ctx.response

    return _async_traced_execute_pipeline


def _pipeline_wrapper_factory(telemetry: _ValkeyTelemetry, is_async: bool) -> _Wrapper:
    """Wrap ``Valkey.pipeline`` so pipelines of a single client are traced."""
    traced_command: _Wrapper | _AsyncWrapper
    traced_pipeline: _Wrapper | _AsyncWrapper
    if is_async:
        traced_command = _async_traced_execute_command_factory(telemetry)
        traced_pipeline = _async_traced_execute_pipeline_factory(telemetry)
    else:
        traced_command = _traced_execute_command_factory(telemetry)
        traced_pipeline = _traced_execute_pipeline_factory(telemetry)

    def _wrapper(
        func: _WrappedFunc,
        instance: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        pipeline: Any = func(*args, **kwargs)
        _wrap_function_wrapper(pipeline, "execute", traced_pipeline)
        if hasattr(pipeline, "immediate_execute_command"):
            # Cluster pipelines do not support immediate execution.
            _wrap_function_wrapper(pipeline, "immediate_execute_command", traced_command)
        return pipeline

    return _wrapper


def _instrument(telemetry: _ValkeyTelemetry) -> None:
    traced_command = _traced_execute_command_factory(telemetry)
    traced_pipeline = _traced_execute_pipeline_factory(telemetry)
    async_traced_command = _async_traced_execute_command_factory(telemetry)
    async_traced_pipeline = _async_traced_execute_pipeline_factory(telemetry)

    targets_and_wrappers: tuple[tuple[tuple[tuple[str, str, str], ...], _Wrapper | _AsyncWrapper], ...] = (
        (_COMMAND_TARGETS, traced_command),
        (_PIPELINE_TARGETS, traced_pipeline),
        (_ASYNC_COMMAND_TARGETS, async_traced_command),
        (_ASYNC_PIPELINE_TARGETS, async_traced_pipeline),
    )
    for targets, wrapper in targets_and_wrappers:
        for module, class_name, method in targets:
            _wrap_function_wrapper(module, f"{class_name}.{method}", wrapper)


def _instrument_client(client: Any, telemetry: _ValkeyTelemetry) -> None:
    is_async = isinstance(client, (valkey.asyncio.Valkey, valkey.asyncio.ValkeyCluster))
    traced_command: _Wrapper | _AsyncWrapper
    if is_async:
        traced_command = _async_traced_execute_command_factory(telemetry)
    else:
        traced_command = _traced_execute_command_factory(telemetry)

    _wrap_function_wrapper(client, "execute_command", traced_command)
    _wrap_function_wrapper(client, "pipeline", _pipeline_wrapper_factory(telemetry, is_async))


class ValkeyInstrumentor(BaseInstrumentor):
    """An instrumentor for Valkey.

    See `BaseInstrumentor`.
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def instrument(
        self,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        request_hook: RequestHook | None = None,
        response_hook: ResponseHook | None = None,
        **kwargs: Any,
    ) -> None:
        """Instruments all Valkey clients.

        Args:
            tracer_provider: A TracerProvider, defaults to global.
            meter_provider: A MeterProvider, defaults to global.
            request_hook: A hook that receives the span, the client instance and
                the arguments of the call before it is issued.
            response_hook: A hook that receives the span, the client instance and
                the response of the call.
        """
        super().instrument(
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            request_hook=request_hook,
            response_hook=response_hook,
            **kwargs,
        )

    def _instrument(self, **kwargs: Any) -> None:
        _instrument(_build_telemetry(**kwargs))

    def _uninstrument(self, **kwargs: Any) -> None:
        for targets in (
            _COMMAND_TARGETS,
            _PIPELINE_TARGETS,
            _ASYNC_COMMAND_TARGETS,
            _ASYNC_PIPELINE_TARGETS,
        ):
            for module, class_name, method in targets:
                unwrap(f"{module}.{class_name}", method)

    @staticmethod
    def instrument_client(
        client: Any,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        request_hook: RequestHook | None = None,
        response_hook: ResponseHook | None = None,
    ) -> None:
        """Instrument a single Valkey client.

        Args:
            client: The Valkey client to instrument.
            tracer_provider: A TracerProvider, defaults to global.
            meter_provider: A MeterProvider, defaults to global.
            request_hook: A hook that receives the span, the client instance and
                the arguments of the call before it is issued.
            response_hook: A hook that receives the span, the client instance and
                the response of the call.
        """
        if getattr(client, _INSTRUMENTATION_ATTR, False):
            _logger.warning("Attempting to instrument Valkey connection while already instrumented")
            return
        _instrument_client(
            client,
            _build_telemetry(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                request_hook=request_hook,
                response_hook=response_hook,
            ),
        )
        setattr(client, _INSTRUMENTATION_ATTR, True)

    @staticmethod
    def uninstrument_client(client: Any) -> None:
        """Un-instrument a single Valkey client.

        Pipelines created before this call remain instrumented.
        """
        if not getattr(client, _INSTRUMENTATION_ATTR, False):
            _logger.warning("Attempting to un-instrument Valkey connection that wasn't instrumented")
            return
        unwrap(client, "execute_command")
        unwrap(client, "pipeline")
        setattr(client, _INSTRUMENTATION_ATTR, False)


def _build_telemetry(**kwargs: Any) -> _ValkeyTelemetry:
    tracer = get_tracer(
        __name__,
        __version__,
        tracer_provider=kwargs.get("tracer_provider"),
        schema_url=_SCHEMA_URL,
    )
    meter = get_meter(
        __name__,
        __version__,
        meter_provider=kwargs.get("meter_provider"),
        schema_url=_SCHEMA_URL,
    )
    duration_histogram = _create_duration_histogram(meter)
    return _ValkeyTelemetry(
        tracer,
        duration_histogram,
        request_hook=kwargs.get("request_hook"),
        response_hook=kwargs.get("response_hook"),
    )
