# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Types used by the Valkey instrumentation.

This module imports ``valkey`` at module scope, so it must only be imported
under ``typing.TYPE_CHECKING``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

import valkey.asyncio.client
import valkey.asyncio.cluster
import valkey.client
import valkey.cluster
import valkey.connection

from opentelemetry.trace import Span

RequestHook = Callable[[Span, valkey.connection.Connection, list[Any], dict[str, Any]], None]
ResponseHook = Callable[[Span, valkey.connection.Connection, Any], None]

AsyncPipelineInstance = TypeVar(
    "AsyncPipelineInstance",
    valkey.asyncio.client.Pipeline,
    valkey.asyncio.cluster.ClusterPipeline,
)
AsyncValkeyInstance = TypeVar(
    "AsyncValkeyInstance",
    valkey.asyncio.Valkey,
    valkey.asyncio.ValkeyCluster,
)
PipelineInstance = TypeVar(
    "PipelineInstance",
    valkey.client.Pipeline,
    valkey.cluster.ClusterPipeline,
)
ValkeyInstance = TypeVar("ValkeyInstance", valkey.client.Valkey, valkey.cluster.ValkeyCluster)
