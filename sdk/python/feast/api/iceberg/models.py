"""
Pydantic models for the Iceberg REST Catalog API.

Covers namespaces, tables, volumes, connections, and credential vending
per the Iceberg REST Catalog OpenAPI spec with RHOAI extensions.

Reference: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Namespace models
# ---------------------------------------------------------------------------


class CreateNamespaceRequest(BaseModel):
    namespace: List[str] = Field(
        ..., description="Multi-level namespace identifier, e.g. ['project', 'schema']"
    )
    properties: Dict[str, str] = Field(default_factory=dict)


class NamespaceResponse(BaseModel):
    namespace: List[str]
    properties: Dict[str, str] = Field(default_factory=dict)


class ListNamespacesResponse(BaseModel):
    model_config = {"populate_by_name": True}

    namespaces: List[List[str]]
    next_page_token: Optional[str] = Field(None, alias="next-page-token")


class UpdateNamespacePropertiesRequest(BaseModel):
    removals: List[str] = Field(default_factory=list)
    updates: Dict[str, str] = Field(default_factory=dict)


class UpdateNamespacePropertiesResponse(BaseModel):
    updated: List[str] = Field(default_factory=list)
    removed: List[str] = Field(default_factory=list)
    missing: List[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Schema & table models
# ---------------------------------------------------------------------------


class Schema(BaseModel):
    model_config = {"populate_by_name": True}

    type: str = Field(default="struct")
    schema_id: int = Field(default=0, alias="schema-id")
    fields: List[Dict[str, Any]] = Field(default_factory=list)


class TableIdentifier(BaseModel):
    namespace: List[str]
    name: str


class TableMetadata(BaseModel):
    model_config = {"populate_by_name": True}

    format_version: int = Field(default=2, alias="format-version")
    table_uuid: str = Field(..., alias="table-uuid")
    location: str
    schemas: List[Schema] = Field(default_factory=list)
    current_schema_id: int = Field(default=0, alias="current-schema-id")
    properties: Dict[str, str] = Field(default_factory=dict)


class CreateTableRequest(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    schema_: Optional[Schema] = Field(None, alias="schema")
    location: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    data_source_format: Optional[str] = Field(
        None, alias="data-source-format", description="PARQUET, ORC, DELTA, etc."
    )
    data_source_ref: Optional[str] = Field(
        None,
        alias="data-source-ref",
        description="Reference to a Feast DataSource (connection)",
    )


class LoadTableResult(BaseModel):
    model_config = {"populate_by_name": True}

    metadata_location: str = Field(..., alias="metadata-location")
    metadata: TableMetadata
    config: Dict[str, str] = Field(default_factory=dict)


class ListTablesResponse(BaseModel):
    model_config = {"populate_by_name": True}

    identifiers: List[TableIdentifier]
    next_page_token: Optional[str] = Field(None, alias="next-page-token")


# ---------------------------------------------------------------------------
# Volume models (RHOAI extension — UC-aligned)
# ---------------------------------------------------------------------------


class VolumeType(str, Enum):
    MANAGED = "MANAGED"
    EXTERNAL = "EXTERNAL"


class VolumeInfo(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    volume_type: VolumeType = Field(default=VolumeType.EXTERNAL, alias="volume-type")
    storage_location: str = Field(..., alias="storage-location")
    namespace: List[str] = Field(default_factory=list)
    comment: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    data_source_ref: Optional[str] = Field(
        None,
        alias="data-source-ref",
        description="Reference to a Feast DataSource (connection)",
    )
    created_at: Optional[str] = Field(None, alias="created-at")
    updated_at: Optional[str] = Field(None, alias="updated-at")


class CreateVolumeRequest(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    volume_type: VolumeType = Field(default=VolumeType.EXTERNAL, alias="volume-type")
    storage_location: str = Field(..., alias="storage-location")
    comment: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    data_source_ref: Optional[str] = Field(None, alias="data-source-ref")


class ListVolumesResponse(BaseModel):
    model_config = {"populate_by_name": True}

    volumes: List[VolumeInfo]
    next_page_token: Optional[str] = Field(None, alias="next-page-token")


# ---------------------------------------------------------------------------
# Connection models (RHOAI extension — DataSource-backed)
# ---------------------------------------------------------------------------


class ConnectionInfo(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    connection_type: str = Field(..., alias="connection-type")
    properties: Dict[str, str] = Field(default_factory=dict)
    owner: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[str] = Field(None, alias="created-at")
    updated_at: Optional[str] = Field(None, alias="updated-at")


class CreateConnectionRequest(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    connection_type: str = Field(..., alias="connection-type")
    project: str
    properties: Dict[str, str] = Field(default_factory=dict)
    owner: Optional[str] = None
    description: Optional[str] = None


class ListConnectionsResponse(BaseModel):
    model_config = {"populate_by_name": True}

    connections: List[ConnectionInfo]
    next_page_token: Optional[str] = Field(None, alias="next-page-token")


# ---------------------------------------------------------------------------
# Credential vending models
# ---------------------------------------------------------------------------


class CredentialResponse(BaseModel):
    model_config = {"populate_by_name": True}

    config: Dict[str, str] = Field(
        default_factory=dict,
        description="Vended credentials keyed by storage type (s3.*, gcs.*, adls.*)",
    )
    expiration_ms: Optional[int] = Field(
        None,
        alias="expiration-ms",
        description="Credential expiration in epoch milliseconds",
    )


# ---------------------------------------------------------------------------
# Search extension model
# ---------------------------------------------------------------------------


class SearchResult(BaseModel):
    model_config = {"populate_by_name": True}

    name: str
    namespace: List[str] = Field(default_factory=list)
    asset_type: str = Field(default="table", alias="asset-type")
    properties: Dict[str, str] = Field(default_factory=dict)


class SearchResponse(BaseModel):
    model_config = {"populate_by_name": True}

    results: List[SearchResult]
    total_count: int = Field(0, alias="total-count")


# ---------------------------------------------------------------------------
# Error model
# ---------------------------------------------------------------------------


class IcebergErrorResponse(BaseModel):
    message: str
    type: str
    code: int = Field(ge=400, lt=600)
    stack: Optional[List[str]] = None
