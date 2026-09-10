# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import logging
from unittest import IsolatedAsyncioTestCase, mock

import valkey
import valkey.asyncio
import valkey.asyncio.cluster
import valkey.cluster
from fakeredis import FakeAsyncValkey, FakeServer, FakeStrictValkey

from opentelemetry import trace
from opentelemetry.instrumentation.utils import suppress_instrumentation
from opentelemetry.instrumentation.valkey import ValkeyInstrumentor
from opentelemetry.semconv.attributes.db_attributes import (
    DB_NAMESPACE,
    DB_OPERATION_BATCH_SIZE,
    DB_OPERATION_NAME,
    DB_QUERY_TEXT,
    DB_RESPONSE_STATUS_CODE,
    DB_STORED_PROCEDURE_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.network_attributes import (
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
    NETWORK_TRANSPORT,
)
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from opentelemetry.semconv.metrics.db_metrics import DB_CLIENT_OPERATION_DURATION
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace import SpanKind, StatusCode

_LOGGER_NAME = "opentelemetry.instrumentation.valkey"


def _assert_duration_metric(test_case, expected_attributes):
    """Assert the operation duration metric holds exactly the expected points."""
    metrics = test_case.get_sorted_metrics()
    test_case.assertEqual(len(metrics), 1)
    metric = metrics[0]
    test_case.assertEqual(metric.name, DB_CLIENT_OPERATION_DURATION)
    test_case.assertEqual(metric.unit, "s")
    data_points = list(metric.data.data_points)
    test_case.assertEqual(len(data_points), len(expected_attributes))
    for data_point, expected in zip(data_points, expected_attributes):
        test_case.assertEqual(dict(data_point.attributes), expected)
        test_case.assertEqual(data_point.count, 1)


class _ValkeyTestBase(TestBase):
    """Instruments every client for the duration of the test."""

    def setUp(self):
        super().setUp()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        super().tearDown()
        ValkeyInstrumentor().uninstrument()

    @staticmethod
    def _mocked_client(**kwargs):
        """A real client whose connection is mocked out, so nothing is sent."""
        return valkey.Valkey(**kwargs)


class TestValkey(_ValkeyTestBase):
    def test_span_name_and_kind(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "GET")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertIs(spans[0].status.status_code, StatusCode.UNSET)

    def test_instrumentation_scope(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.instrumentation_scope.name, "opentelemetry.instrumentation.valkey")
        self.assertEqual(
            span.instrumentation_scope.schema_url,
            "https://opentelemetry.io/schemas/1.25.0",
        )

class TestValkeyAttributes(_ValkeyTestBase):
    """Span attributes reported for the different connection shapes."""

    def test_attributes_default(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.set("key", "value")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(
            dict(span.attributes),
            {
                DB_SYSTEM_NAME: "valkey",
                DB_OPERATION_NAME: "SET",
                DB_NAMESPACE: "0",
                DB_QUERY_TEXT: "SET ? ?",
                SERVER_ADDRESS: "localhost",
                SERVER_PORT: 6379,
                NETWORK_PEER_ADDRESS: "localhost",
                NETWORK_PEER_PORT: 6379,
                NETWORK_TRANSPORT: "tcp",
            },
        )

    def test_attribute_value_types(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.get("key")

        attributes = self.memory_exporter.get_finished_spans()[0].attributes
        self.assertIsInstance(attributes[DB_SYSTEM_NAME], str)
        self.assertIsInstance(attributes[DB_OPERATION_NAME], str)
        # db.namespace is a string even though the database index is numeric.
        self.assertIsInstance(attributes[DB_NAMESPACE], str)
        self.assertIsInstance(attributes[DB_QUERY_TEXT], str)
        self.assertIsInstance(attributes[SERVER_ADDRESS], str)
        self.assertIsInstance(attributes[SERVER_PORT], int)
        self.assertIsInstance(attributes[NETWORK_TRANSPORT], str)
        self.assertIsInstance(attributes[NETWORK_PEER_ADDRESS], str)
        self.assertIsInstance(attributes[NETWORK_PEER_PORT], int)

    def test_attributes_tcp_from_url(self):
        client = valkey.Valkey.from_url("valkey://foo:bar@1.1.1.1:6380/1")
        with mock.patch.object(client, "connection"):
            client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "GET")
        self.assertEqual(span.attributes[DB_NAMESPACE], "1")
        self.assertEqual(span.attributes[SERVER_ADDRESS], "1.1.1.1")
        self.assertEqual(span.attributes[SERVER_PORT], 6380)
        self.assertEqual(span.attributes[NETWORK_PEER_ADDRESS], "1.1.1.1")
        self.assertEqual(span.attributes[NETWORK_PEER_PORT], 6380)
        self.assertEqual(span.attributes[NETWORK_TRANSPORT], "tcp")

    def test_attributes_unix_socket(self):
        client = valkey.Valkey.from_url("unix://foo@/path/to/socket.sock?db=3&password=bar")
        with mock.patch.object(client, "connection"):
            client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes[DB_NAMESPACE], "3")
        self.assertEqual(span.attributes[SERVER_ADDRESS], "/path/to/socket.sock")
        self.assertEqual(span.attributes[NETWORK_PEER_ADDRESS], "/path/to/socket.sock")
        self.assertEqual(span.attributes[NETWORK_TRANSPORT], "unix")
        self.assertNotIn(SERVER_PORT, span.attributes)
        self.assertNotIn(NETWORK_PEER_PORT, span.attributes)

    def test_attributes_explicit_db_none(self):
        client = self._mocked_client(db=None)
        with mock.patch.object(client, "connection"):
            client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "GET")
        self.assertEqual(span.attributes[DB_NAMESPACE], "0")

    def test_attributes_without_connection_pool(self):
        client = self._mocked_client()
        client.connection_pool = mock.Mock(spec=["disconnect"])
        with mock.patch.object(client, "connection"):
            client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        # Without a connection pool the span still carries the command details.
        self.assertEqual(span.name, "GET")
        self.assertEqual(span.attributes[DB_SYSTEM_NAME], "valkey")
        self.assertEqual(span.attributes[DB_QUERY_TEXT], "GET ?")
        self.assertNotIn(DB_NAMESPACE, span.attributes)
        self.assertNotIn(SERVER_ADDRESS, span.attributes)

    def test_stored_procedure_name(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.evalsha("abc123", 1, "key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "EVALSHA")
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "EVALSHA")
        self.assertEqual(span.attributes[DB_STORED_PROCEDURE_NAME], "abc123")

    def test_stored_procedure_name_for_functions(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.fcall("myfunc", 0)

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes[DB_STORED_PROCEDURE_NAME], "myfunc")

    def test_no_stored_procedure_name_for_eval(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.eval("return 1", 0)

        span = self.memory_exporter.get_finished_spans()[0]
        # EVAL carries the script body, which is not a stored procedure name.
        self.assertNotIn(DB_STORED_PROCEDURE_NAME, span.attributes)

    def test_query_text_is_sanitized(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.set("key", "a-secret-value")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes[DB_QUERY_TEXT], "SET ? ?")
        self.assertNotIn("a-secret-value", span.attributes[DB_QUERY_TEXT])

    def test_query_text_is_truncated(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.mget(*[f"key-{index}" for index in range(1000)])

        query_text = self.memory_exporter.get_finished_spans()[0].attributes[DB_QUERY_TEXT]
        self.assertEqual(len(query_text), 1000)
        self.assertTrue(query_text.endswith("..."))

class TestValkeyBehaviour(_ValkeyTestBase):
    """Instrumentation lifecycle, pipelines, errors, hooks and suppression."""

    def test_not_recording(self):
        client = self._mocked_client()
        mock_tracer = mock.Mock()
        mock_span = mock.Mock()
        mock_span.is_recording.return_value = False
        mock_tracer.start_span.return_value = mock_span
        with mock.patch("opentelemetry.trace.get_tracer") as tracer:
            tracer.return_value = mock_tracer
            with mock.patch.object(client, "connection"):
                client.get("key")
            self.assertFalse(mock_span.is_recording())
            self.assertTrue(mock_span.is_recording.called)
            self.assertFalse(mock_span.set_attribute.called)

    def test_no_op_tracer_provider(self):
        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(tracer_provider=trace.NoOpTracerProvider())
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.get("key")

        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

    def test_instrument_uninstrument_instrument(self):
        client = self._mocked_client()

        ValkeyInstrumentor().uninstrument()
        with mock.patch.object(client, "connection"):
            client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )
        with mock.patch.object(client, "connection"):
            client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

    def test_pipeline(self):
        client = FakeStrictValkey()
        with client.pipeline(transaction=False) as pipeline:
            pipeline.set("key", "value")
            pipeline.get("key")
            pipeline.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.name, "PIPELINE")
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "PIPELINE")
        self.assertEqual(span.attributes[DB_QUERY_TEXT], "SET ? ?\nGET ?")
        self.assertEqual(span.attributes[DB_OPERATION_BATCH_SIZE], 2)
        self.assertIsInstance(span.attributes[DB_OPERATION_BATCH_SIZE], int)

    def test_pipeline_with_a_shared_command(self):
        client = FakeStrictValkey()
        with client.pipeline(transaction=False) as pipeline:
            pipeline.get("one")
            pipeline.get("two")
            pipeline.execute()

        span = self.memory_exporter.get_finished_spans()[0]
        # The shared command is appended, and the identical query texts collapse.
        self.assertEqual(span.name, "PIPELINE GET")
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "PIPELINE GET")
        self.assertEqual(span.attributes[DB_QUERY_TEXT], "GET ?")
        self.assertEqual(span.attributes[DB_OPERATION_BATCH_SIZE], 2)

    def test_transaction_is_named_multi(self):
        client = FakeStrictValkey()
        # A pipeline is a transaction unless transaction=False is passed.
        with client.pipeline() as pipeline:
            pipeline.get("one")
            pipeline.get("two")
            pipeline.execute()

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "MULTI GET")
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "MULTI GET")

    def test_pipeline_of_one_command_is_not_a_batch(self):
        client = FakeStrictValkey()
        with client.pipeline(transaction=False) as pipeline:
            pipeline.get("key")
            pipeline.execute()

        span = self.memory_exporter.get_finished_spans()[0]
        # A request holding a single operation is not a batch.
        self.assertEqual(span.name, "PIPELINE GET")
        self.assertNotIn(DB_OPERATION_BATCH_SIZE, span.attributes)

    def test_empty_pipeline(self):
        client = FakeStrictValkey()
        with client.pipeline(transaction=False) as pipeline:
            pipeline.execute()

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "PIPELINE")
        self.assertEqual(span.attributes[DB_QUERY_TEXT], "")
        # An empty batch is still a batch, and reports a size of zero.
        self.assertEqual(span.attributes[DB_OPERATION_BATCH_SIZE], 0)

    def test_watch_error_is_not_an_error(self):
        client = FakeStrictValkey()
        with self.assertRaises(valkey.WatchError):
            with client.pipeline() as pipeline:
                pipeline.watch("key")
                # Change the value from outside of the transaction.
                client.set("key", "other")
                pipeline.multi()
                pipeline.set("key", "value")
                pipeline.execute()

        spans = self.memory_exporter.get_finished_spans()
        batch_spans = [span for span in spans if span.name.startswith("MULTI")]
        self.assertEqual(len(batch_spans), 1)
        self.assertIs(batch_spans[0].status.status_code, StatusCode.UNSET)
        self.assertNotIn(ERROR_TYPE, batch_spans[0].attributes)
        self.assertEqual(len(batch_spans[0].events), 0)

    def test_response_error(self):
        client = FakeStrictValkey()
        client.lpush("mylist", "value")
        with self.assertRaises(valkey.ResponseError):
            client.incr("mylist")

        span = self.memory_exporter.get_finished_spans()[-1]
        self.assertEqual(span.name, "INCRBY")
        self.assertIs(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.attributes[ERROR_TYPE], "ResponseError")
        self.assertEqual(span.attributes[DB_RESPONSE_STATUS_CODE], "WRONGTYPE")
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")

    def test_connection_error(self):
        server = FakeServer()
        server.connected = False
        client = FakeStrictValkey(server=server)
        with self.assertRaises(valkey.ConnectionError):
            client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertIs(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.attributes[ERROR_TYPE], "ConnectionError")
        # A transport failure carries no server error code.
        self.assertNotIn(DB_RESPONSE_STATUS_CODE, span.attributes)

    def test_metric(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.get("key")

        _assert_duration_metric(
            self,
            [
                {
                    DB_SYSTEM_NAME: "valkey",
                    DB_NAMESPACE: "0",
                    DB_OPERATION_NAME: "GET",
                    SERVER_ADDRESS: "localhost",
                    SERVER_PORT: 6379,
                    NETWORK_PEER_ADDRESS: "localhost",
                    NETWORK_PEER_PORT: 6379,
                }
            ],
        )

    def test_metric_on_error(self):
        client = FakeStrictValkey()
        client.lpush("mylist", "value")
        self.memory_exporter.clear()
        with self.assertRaises(valkey.ResponseError):
            client.incr("mylist")

        metric = self.get_sorted_metrics()[0]
        error_points = [
            point for point in metric.data.data_points if ERROR_TYPE in dict(point.attributes)
        ]
        self.assertEqual(len(error_points), 1)
        attributes = dict(error_points[0].attributes)
        self.assertEqual(attributes[ERROR_TYPE], "ResponseError")
        self.assertEqual(attributes[DB_RESPONSE_STATUS_CODE], "WRONGTYPE")
        self.assertEqual(attributes[DB_OPERATION_NAME], "INCRBY")

    def test_request_and_response_hooks(self):
        def request_hook(span, instance, args, kwargs):
            span.set_attribute("request_hook_args_count", len(args))

        def response_hook(span, instance, response):
            span.set_attribute("response_hook_response", str(response))

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )

        client = FakeStrictValkey()
        client.get("key")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["request_hook_args_count"], 2)
        self.assertEqual(span.attributes["response_hook_response"], "None")

    def test_hooks_are_called_for_pipelines(self):
        calls = []

        def request_hook(span, instance, args, kwargs):
            calls.append("request")

        def response_hook(span, instance, response):
            calls.append("response")

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )

        client = FakeStrictValkey()
        with client.pipeline(transaction=False) as pipeline:
            pipeline.get("key")
            pipeline.execute()

        self.assertEqual(calls, ["request", "response"])

    def test_hook_exception_is_swallowed(self):
        def request_hook(span, instance, args, kwargs):
            raise ValueError("request hook failed")

        def response_hook(span, instance, response):
            raise ValueError("response hook failed")

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )

        client = FakeStrictValkey()
        with self.assertLogs(_LOGGER_NAME, level=logging.WARNING) as logs:
            client.get("key")

        self.assertEqual(len(logs.records), 2)
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

    def test_suppress_instrumentation(self):
        client = self._mocked_client()
        with suppress_instrumentation():
            with mock.patch.object(client, "connection"):
                client.get("key")

        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

    def test_suppress_instrumentation_pipeline(self):
        client = FakeStrictValkey()
        with suppress_instrumentation():
            with client.pipeline(transaction=False) as pipeline:
                pipeline.get("key")
                pipeline.execute()

        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

    def test_cluster_classes_are_wrapped(self):
        for cls, method in (
            (valkey.cluster.ValkeyCluster, "execute_command"),
            (valkey.cluster.ClusterPipeline, "execute"),
            (valkey.asyncio.cluster.ValkeyCluster, "execute_command"),
            (valkey.asyncio.cluster.ClusterPipeline, "execute"),
        ):
            self.assertTrue(
                hasattr(getattr(cls, method), "__wrapped__"),
                f"{cls.__name__}.{method} is not instrumented",
            )

        ValkeyInstrumentor().uninstrument()

        for cls, method in (
            (valkey.cluster.ValkeyCluster, "execute_command"),
            (valkey.cluster.ClusterPipeline, "execute"),
            (valkey.asyncio.cluster.ValkeyCluster, "execute_command"),
            (valkey.asyncio.cluster.ClusterPipeline, "execute"),
        ):
            self.assertFalse(
                hasattr(getattr(cls, method), "__wrapped__"),
                f"{cls.__name__}.{method} is still instrumented",
            )

    def test_ft_create_and_search_attributes(self):
        client = self._mocked_client()
        with mock.patch.object(client, "connection"):
            client.execute_command(
                "FT.CREATE",
                "idx",
                "SCHEMA",
                "title",
                "TEXT",
                "published_at",
                "NUMERIC",
            )

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "FT.CREATE")
        self.assertEqual(span.attributes[DB_OPERATION_NAME], "FT.CREATE")
        self.assertEqual(span.attributes["valkey.create_index.index"], "idx")
        self.assertEqual(
            span.attributes["valkey.create_index.fields"],
            "Field(name: title, type: TEXT);Field(name: published_at, type: NUMERIC);",
        )

        self.memory_exporter.clear()
        # A real connection is used so that the retry wrapper actually calls
        # through to the patched parse_response.
        connection = valkey.connection.Connection()
        client.connection = connection
        with mock.patch.object(connection, "send_command"):
            with mock.patch.object(
                client,
                "parse_response",
                return_value=[1, "doc:1", ["title", "hello"]],
            ):
                client.execute_command("FT.SEARCH", "idx", "hello")

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.name, "FT.SEARCH")
        self.assertEqual(span.attributes["valkey.search.index"], "idx")
        self.assertEqual(span.attributes["valkey.search.query"], "hello")
        self.assertEqual(span.attributes["valkey.search.total"], 1)
        self.assertEqual(span.attributes["valkey.search.xdoc_doc:1.title"], "hello")


class TestValkeyAsync(_ValkeyTestBase, IsolatedAsyncioTestCase):
    async def test_command(self):
        client = FakeAsyncValkey()
        await client.get("key")

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "GET")
        self.assertEqual(spans[0].kind, SpanKind.CLIENT)
        self.assertEqual(spans[0].attributes[DB_SYSTEM_NAME], "valkey")
        self.assertEqual(spans[0].attributes[DB_QUERY_TEXT], "GET ?")
        self.assertEqual(spans[0].attributes[DB_NAMESPACE], "0")

    async def test_pipeline(self):
        client = FakeAsyncValkey()
        async with client.pipeline(transaction=False) as pipeline:
            pipeline.set("key", "value")
            pipeline.get("key")
            await pipeline.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "PIPELINE")
        self.assertEqual(spans[0].attributes[DB_QUERY_TEXT], "SET ? ?\nGET ?")
        self.assertEqual(spans[0].attributes[DB_OPERATION_BATCH_SIZE], 2)

    async def test_pipeline_is_not_reported_as_a_transaction(self):
        client = FakeAsyncValkey()
        # The async client keeps Valkey.transaction as a method and stores the
        # flag under a different name, so a bare getattr would read as truthy.
        async with client.pipeline(transaction=False) as pipeline:
            pipeline.get("one")
            pipeline.get("two")
            await pipeline.execute()

        self.assertEqual(self.memory_exporter.get_finished_spans()[0].name, "PIPELINE GET")

    async def test_transaction_is_named_multi(self):
        client = FakeAsyncValkey()
        async with client.pipeline() as pipeline:
            pipeline.get("one")
            pipeline.get("two")
            await pipeline.execute()

        self.assertEqual(self.memory_exporter.get_finished_spans()[0].name, "MULTI GET")

    async def test_response_error(self):
        client = FakeAsyncValkey()
        # The error is raised through a patched parse_response because the
        # asyncio side of fakeredis reports redis-py's exception types even when
        # it fakes a Valkey client.
        error = valkey.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value")
        with mock.patch.object(client, "parse_response", mock.AsyncMock(side_effect=error)):
            with self.assertRaises(valkey.ResponseError):
                await client.incr("mylist")

        span = self.memory_exporter.get_finished_spans()[-1]
        self.assertIs(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.attributes[ERROR_TYPE], "ResponseError")
        self.assertEqual(span.attributes[DB_RESPONSE_STATUS_CODE], "WRONGTYPE")

    async def test_ft_search_attributes(self):
        client = FakeAsyncValkey()
        with mock.patch.object(
            client,
            "parse_response",
            mock.AsyncMock(return_value=[1, "doc:1", ["title", "hello"]]),
        ):
            await client.execute_command("FT.SEARCH", "idx", "hello")

        span = self.memory_exporter.get_finished_spans()[0]
        # The async path enriches FT.SEARCH exactly like the sync one.
        self.assertEqual(span.attributes["valkey.search.index"], "idx")
        self.assertEqual(span.attributes["valkey.search.total"], 1)
        self.assertEqual(span.attributes["valkey.search.xdoc_doc:1.title"], "hello")

    async def test_metric(self):
        client = FakeAsyncValkey()
        await client.get("key")

        metric = self.get_sorted_metrics()[0]
        self.assertEqual(metric.name, DB_CLIENT_OPERATION_DURATION)
        attributes = dict(list(metric.data.data_points)[0].attributes)
        self.assertEqual(attributes[DB_SYSTEM_NAME], "valkey")
        self.assertEqual(attributes[DB_OPERATION_NAME], "GET")
        self.assertNotIn(DB_QUERY_TEXT, attributes)

    async def test_hooks(self):
        calls = []

        def request_hook(span, instance, args, kwargs):
            calls.append("request")

        def response_hook(span, instance, response):
            calls.append("response")

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            request_hook=request_hook,
            response_hook=response_hook,
        )

        client = FakeAsyncValkey()
        await client.get("key")
        async with client.pipeline(transaction=False) as pipeline:
            pipeline.get("key")
            await pipeline.execute()

        self.assertEqual(calls, ["request", "response", "request", "response"])

    async def test_hook_exception_is_swallowed(self):
        def request_hook(span, instance, args, kwargs):
            raise ValueError("request hook failed")

        ValkeyInstrumentor().uninstrument()
        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            request_hook=request_hook,
        )

        client = FakeAsyncValkey()
        with self.assertLogs(_LOGGER_NAME, level=logging.WARNING):
            await client.get("key")

        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

    async def test_suppress_instrumentation(self):
        client = FakeAsyncValkey()
        with suppress_instrumentation():
            await client.get("key")

        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

    async def test_instrument_uninstrument_instrument(self):
        client = FakeAsyncValkey()

        ValkeyInstrumentor().uninstrument()
        await client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

        ValkeyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )
        await client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)


class TestValkeyInstrumentClient(TestBase):
    def test_only_the_instrumented_client_is_traced(self):
        instrumented = FakeStrictValkey()
        other = FakeStrictValkey()
        ValkeyInstrumentor.instrument_client(
            instrumented,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

        other.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

        instrumented.get("key")
        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "GET")

    def test_pipeline_of_instrumented_client(self):
        client = FakeStrictValkey()
        ValkeyInstrumentor.instrument_client(
            client,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

        with client.pipeline(transaction=False) as pipeline:
            pipeline.set("key", "value")
            pipeline.get("key")
            pipeline.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "PIPELINE")
        self.assertEqual(spans[0].attributes[DB_OPERATION_BATCH_SIZE], 2)

    def test_uninstrument_client(self):
        client = FakeStrictValkey()
        ValkeyInstrumentor.instrument_client(client, tracer_provider=self.tracer_provider)
        client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

        ValkeyInstrumentor.uninstrument_client(client)
        client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

    def test_client_can_be_reinstrumented(self):
        client = FakeStrictValkey()
        ValkeyInstrumentor.instrument_client(client, tracer_provider=self.tracer_provider)
        ValkeyInstrumentor.uninstrument_client(client)
        ValkeyInstrumentor.instrument_client(client, tracer_provider=self.tracer_provider)

        client.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

    def test_instrument_client_twice_warns(self):
        client = FakeStrictValkey()
        ValkeyInstrumentor.instrument_client(client, tracer_provider=self.tracer_provider)
        with self.assertLogs(_LOGGER_NAME, level=logging.WARNING) as logs:
            ValkeyInstrumentor.instrument_client(client, tracer_provider=self.tracer_provider)
        self.assertIn("already instrumented", logs.output[0])

    def test_uninstrument_client_that_was_never_instrumented(self):
        client = FakeStrictValkey()
        with self.assertLogs(_LOGGER_NAME, level=logging.WARNING) as logs:
            ValkeyInstrumentor.uninstrument_client(client)
        self.assertIn("wasn't instrumented", logs.output[0])


class TestValkeyAsyncInstrumentClient(TestBase, IsolatedAsyncioTestCase):
    async def test_only_the_instrumented_client_is_traced(self):
        instrumented = FakeAsyncValkey()
        other = FakeAsyncValkey()
        ValkeyInstrumentor.instrument_client(
            instrumented,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

        await other.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 0)

        await instrumented.get("key")
        self.assertEqual(len(self.memory_exporter.get_finished_spans()), 1)

    async def test_pipeline_of_instrumented_client(self):
        client = FakeAsyncValkey()
        ValkeyInstrumentor.instrument_client(client, tracer_provider=self.tracer_provider)

        async with client.pipeline(transaction=False) as pipeline:
            pipeline.set("key", "value")
            pipeline.get("key")
            await pipeline.execute()

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].name, "PIPELINE")
