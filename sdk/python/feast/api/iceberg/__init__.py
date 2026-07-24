"""
Iceberg REST Catalog API — translation layer for the Feast server.

Adds a second API surface to the Feast server process:
  /v1/{prefix}/namespaces/...       — Namespace CRUD  (→ Feast Projects)
  /v1/{prefix}/namespaces/{ns}/tables/... — Table CRUD (→ SavedDatasets)
  /v1/{prefix}/namespaces/{ns}/volumes/...— Volume CRUD (→ SavedDatasets, asset_type=volume)
  /v1/{prefix}/connections/...      — Connection CRUD (→ Feast DataSources)

All Iceberg REST endpoints coexist with the Feast REST API (/api/v1/*) on
the same FastAPI app, same container, same pod (port 6572).

Integration: one call in register_all_routes():
    register_iceberg_routes(app, grpc_handler)
"""

from __future__ import annotations

from fastapi import FastAPI

from .connections import get_connection_router
from .namespaces import get_namespace_router
from .tables import get_table_router
from .volumes import get_volume_router

_ICEBERG_API_PREFIX = "/v1"


def register_iceberg_routes(app: FastAPI, grpc_handler) -> None:
    """Register Iceberg REST Catalog API routes on the Feast FastAPI app."""

    app.include_router(
        get_namespace_router(grpc_handler),
        prefix=_ICEBERG_API_PREFIX,
        tags=["Iceberg REST — Namespaces"],
    )
    app.include_router(
        get_table_router(grpc_handler),
        prefix=_ICEBERG_API_PREFIX,
        tags=["Iceberg REST — Tables"],
    )
    app.include_router(
        get_volume_router(grpc_handler),
        prefix=_ICEBERG_API_PREFIX,
        tags=["Iceberg REST — Volumes"],
    )
    app.include_router(
        get_connection_router(grpc_handler),
        prefix=_ICEBERG_API_PREFIX,
        tags=["Iceberg REST — Connections"],
    )
