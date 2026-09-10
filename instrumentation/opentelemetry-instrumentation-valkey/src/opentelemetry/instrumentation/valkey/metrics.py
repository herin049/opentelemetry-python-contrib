# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

# The helpers below are private to the package but consumed from __init__.py,
# which pyright's strict mode reports as unused.
# pyright: reportUnusedFunction=false

"""Metric instruments and attributes for the Valkey instrumentation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from opentelemetry.semconv.attributes.db_attributes import (
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_RESPONSE_STATUS_CODE,
    DB_STORED_PROCEDURE_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.network_attributes import (
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
)
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from opentelemetry.semconv.metrics.db_metrics import DB_CLIENT_OPERATION_DURATION

if TYPE_CHECKING:
    from opentelemetry.metrics import Histogram, Meter
    from opentelemetry.util.types import AttributeValue

# https://opentelemetry.io/docs/specs/semconv/database/database-metrics/
_DB_DURATION_BUCKETS = [
    0.001,
    0.005,
    0.01,
    0.05,
    0.1,
    0.5,
    1,
    5,
    10,
]

# The subset of the span attributes that is also reported on the duration
# metric, per the database metrics conventions. db.query.text is opt-in there
# and db.operation.batch.size is span only, so neither is included.
_METRIC_ATTRIBUTE_KEYS = (
    DB_SYSTEM_NAME,
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_STORED_PROCEDURE_NAME,
    SERVER_ADDRESS,
    SERVER_PORT,
    NETWORK_PEER_ADDRESS,
    NETWORK_PEER_PORT,
)


def _create_duration_histogram(meter: Meter) -> Histogram:
    """Create the ``db.client.operation.duration`` histogram."""
    return meter.create_histogram(
        name=DB_CLIENT_OPERATION_DURATION,
        description="Duration of database client operations.",
        unit="s",
        explicit_bucket_boundaries_advisory=_DB_DURATION_BUCKETS,
    )


def _extract_metric_attributes(
    attributes: dict[str, AttributeValue],
) -> dict[str, AttributeValue]:
    """Project the span attributes down to the metric attribute subset."""
    return {key: attributes[key] for key in _METRIC_ATTRIBUTE_KEYS if key in attributes}


def _set_error_metric_attributes(
    metric_attributes: dict[str, AttributeValue],
    error_type: str,
    status_code: str | None,
) -> None:
    """Add the error dimensions to an already extracted metric attribute set."""
    metric_attributes[ERROR_TYPE] = error_type
    if status_code is not None:
        metric_attributes[DB_RESPONSE_STATUS_CODE] = status_code
