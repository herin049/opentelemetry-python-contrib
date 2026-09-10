# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the helper modules backing the Valkey instrumentation."""

import fakeredis
import valkey

from opentelemetry.instrumentation.valkey.metrics import (
    _create_duration_histogram,
    _extract_metric_attributes,
    _set_error_metric_attributes,
)
from opentelemetry.instrumentation.valkey.utils import (
    _build_span_name,
    _get_batch_operation_name,
    _get_batch_query_text,
    _get_command_stack,
    _get_error_status_code,
    _get_stored_procedure_name,
)
from opentelemetry.semconv.attributes.db_attributes import (
    DB_NAMESPACE,
    DB_OPERATION_BATCH_SIZE,
    DB_OPERATION_NAME,
    DB_QUERY_TEXT,
    DB_RESPONSE_STATUS_CODE,
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


class TestValkeyUtil(TestBase):
    def test_get_command_stack_for_pipeline(self):
        client = fakeredis.FakeStrictValkey()
        pipeline = client.pipeline(transaction=False)
        pipeline.set("key", "value")
        pipeline.get("key")

        self.assertEqual(
            _get_command_stack(pipeline),
            [("SET", "key", "value"), ("GET", "key")],
        )

    def test_get_command_stack_for_cluster_pipeline(self):
        class _FakeCommand:
            def __init__(self, args):
                self.args = args

        class _FakeClusterPipeline:
            command_stack = [_FakeCommand(("SET", "key", "value")), _FakeCommand(("GET", "key"))]

        class _FakeAsyncClusterPipeline:
            _command_stack = [_FakeCommand(("GET", "key"))]

        self.assertEqual(
            _get_command_stack(_FakeClusterPipeline()),
            [("SET", "key", "value"), ("GET", "key")],
        )
        self.assertEqual(_get_command_stack(_FakeAsyncClusterPipeline()), [("GET", "key")])

    def test_get_error_status_code(self):
        self.assertEqual(
            _get_error_status_code(valkey.ResponseError("WRONGTYPE Operation against a key")),
            "WRONGTYPE",
        )
        self.assertIsNone(_get_error_status_code(valkey.ResponseError("unknown command 'FOO'")))
        self.assertIsNone(_get_error_status_code(valkey.ConnectionError("connection refused")))

    def test_build_span_name(self):
        # The database index is deliberately absent from the span name.
        self.assertEqual(_build_span_name("GET"), "GET")
        # A command without arguments must still get a usable span name.
        self.assertEqual(_build_span_name(""), "valkey")

    def test_get_stored_procedure_name(self):
        self.assertEqual(_get_stored_procedure_name(("EVALSHA", "abc123", 1, "k")), "abc123")
        self.assertEqual(_get_stored_procedure_name(("EVALSHA_RO", "abc123", 0)), "abc123")
        self.assertEqual(_get_stored_procedure_name(("FCALL", "myfunc", 0)), "myfunc")
        self.assertEqual(_get_stored_procedure_name(("FCALL_RO", "myfunc", 0)), "myfunc")
        # EVAL carries the script body rather than a name or a sha1 digest.
        self.assertIsNone(_get_stored_procedure_name(("EVAL", "return 1", 0)))
        self.assertIsNone(_get_stored_procedure_name(("GET", "key")))
        self.assertIsNone(_get_stored_procedure_name(("EVALSHA",)))

    def test_get_batch_operation_name(self):
        class _FakePipeline:
            transaction = False

        class _FakeTransaction:
            transaction = True

        class _FakeAsyncTransaction:
            is_transaction = True

        class _FakeExplicitTransaction:
            explicit_transaction = True

        shared = [("GET", "one"), ("GET", "two")]
        mixed = [("SET", "one", 1), ("GET", "two")]

        self.assertEqual(_get_batch_operation_name(_FakePipeline(), shared), "PIPELINE GET")
        self.assertEqual(_get_batch_operation_name(_FakePipeline(), mixed), "PIPELINE")
        self.assertEqual(_get_batch_operation_name(_FakePipeline(), []), "PIPELINE")
        self.assertEqual(_get_batch_operation_name(_FakeTransaction(), shared), "MULTI GET")
        self.assertEqual(_get_batch_operation_name(_FakeAsyncTransaction(), mixed), "MULTI")
        self.assertEqual(_get_batch_operation_name(_FakeExplicitTransaction(), mixed), "MULTI")

    def test_get_batch_query_text(self):
        # Identical query texts collapse to a single entry.
        self.assertEqual(_get_batch_query_text([("GET", "one"), ("GET", "two")]), "GET ?")
        self.assertEqual(
            _get_batch_query_text([("SET", "one", 1), ("GET", "two")]),
            "SET ? ?\nGET ?",
        )
        self.assertEqual(_get_batch_query_text([]), "")


class TestValkeyMetrics(TestBase):
    def test_extract_metric_attributes(self):
        attributes = {
            DB_SYSTEM_NAME: "valkey",
            DB_OPERATION_NAME: "GET",
            DB_NAMESPACE: "0",
            SERVER_ADDRESS: "localhost",
            SERVER_PORT: 6379,
            NETWORK_PEER_ADDRESS: "localhost",
            NETWORK_PEER_PORT: 6379,
            NETWORK_TRANSPORT: "tcp",
            DB_QUERY_TEXT: "GET ?",
        }

        # Only the low cardinality subset is carried over to the metric.
        self.assertEqual(
            _extract_metric_attributes(attributes),
            {
                DB_SYSTEM_NAME: "valkey",
                DB_OPERATION_NAME: "GET",
                DB_NAMESPACE: "0",
                SERVER_ADDRESS: "localhost",
                SERVER_PORT: 6379,
                NETWORK_PEER_ADDRESS: "localhost",
                NETWORK_PEER_PORT: 6379,
            },
        )

    def test_extract_metric_attributes_skips_missing_keys(self):
        # A cluster client reports neither a namespace nor a server address.
        attributes = {
            DB_SYSTEM_NAME: "valkey",
            DB_OPERATION_NAME: "PIPELINE",
            DB_QUERY_TEXT: "GET ?",
            DB_OPERATION_BATCH_SIZE: 2,
        }

        self.assertEqual(
            _extract_metric_attributes(attributes),
            {DB_SYSTEM_NAME: "valkey", DB_OPERATION_NAME: "PIPELINE"},
        )

    def test_set_error_metric_attributes(self):
        attributes = {DB_SYSTEM_NAME: "valkey", DB_OPERATION_NAME: "INCRBY"}

        _set_error_metric_attributes(attributes, "ResponseError", "WRONGTYPE")

        self.assertEqual(
            attributes,
            {
                DB_SYSTEM_NAME: "valkey",
                DB_OPERATION_NAME: "INCRBY",
                ERROR_TYPE: "ResponseError",
                DB_RESPONSE_STATUS_CODE: "WRONGTYPE",
            },
        )

    def test_set_error_metric_attributes_without_status_code(self):
        attributes = {DB_SYSTEM_NAME: "valkey"}

        _set_error_metric_attributes(attributes, "ConnectionError", None)

        self.assertEqual(
            attributes,
            {DB_SYSTEM_NAME: "valkey", ERROR_TYPE: "ConnectionError"},
        )

    def test_create_duration_histogram(self):
        meter = self.meter_provider.get_meter(__name__)

        histogram = _create_duration_histogram(meter)
        histogram.record(0.25, attributes={DB_SYSTEM_NAME: "valkey"})

        metrics = self.get_sorted_metrics()
        self.assertEqual(len(metrics), 1)
        self.assertEqual(metrics[0].name, DB_CLIENT_OPERATION_DURATION)
        self.assertEqual(metrics[0].unit, "s")
        self.assertEqual(metrics[0].description, "Duration of database client operations.")
        data_point = list(metrics[0].data.data_points)[0]
        self.assertEqual(dict(data_point.attributes), {DB_SYSTEM_NAME: "valkey"})
        self.assertEqual(data_point.count, 1)
