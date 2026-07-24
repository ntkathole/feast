"""
Feast ↔ Iceberg REST Catalog translation layer.

Translates between Feast registry objects (proto dicts from grpc_call) and
Iceberg REST Catalog API response shapes (Pydantic models from models.py).

Covers four mapping domains:
  1. Projects        → Iceberg Namespaces
  2. SavedDatasets   → Iceberg Tables
  3. SavedDatasets   → Iceberg Volumes (filtered by asset_type=volume tag)
  4. DataSources     → RHOAI Connections (extension)

The mapping is lossy by design — Feast and Iceberg have different data models.
Feast-specific metadata is preserved in the Iceberg properties bag.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from .models import (
    ConnectionInfo,
    CredentialResponse,
    ListConnectionsResponse,
    ListNamespacesResponse,
    ListVolumesResponse,
    LoadTableResult,
    NamespaceResponse,
    Schema,
    TableIdentifier,
    TableMetadata,
    VolumeInfo,
    VolumeType,
)

# ---- Type mapping: Feast ValueType → Iceberg type string -----------------

_FEAST_TO_ICEBERG_TYPE = {
    "INT32": "int",
    "INT64": "long",
    "FLOAT": "float",
    "DOUBLE": "double",
    "STRING": "string",
    "BYTES": "binary",
    "BOOL": "boolean",
    "UNIX_TIMESTAMP": "timestamptz",
    "BOOL_LIST": "list<boolean>",
    "INT32_LIST": "list<int>",
    "INT64_LIST": "list<long>",
    "FLOAT_LIST": "list<float>",
    "DOUBLE_LIST": "list<double>",
    "STRING_LIST": "list<string>",
    "BYTES_LIST": "list<binary>",
}


def feast_type_to_iceberg(feast_type: str) -> str:
    return _FEAST_TO_ICEBERG_TYPE.get(feast_type.upper(), "string")


# ==========================================================================
# 1. NAMESPACE MAPPING  (Project → Iceberg Namespace)
# ==========================================================================


def feast_projects_to_namespaces(
    projects_response: Dict[str, Any],
) -> ListNamespacesResponse:
    projects = projects_response.get("projects", [])
    namespaces: List[List[str]] = []
    for proj in projects:
        name = proj.get("spec", {}).get("name", proj.get("name", ""))
        if name:
            namespaces.append([name])
    return ListNamespacesResponse(namespaces=namespaces)


def feast_project_to_namespace(
    project_response: Dict[str, Any],
) -> NamespaceResponse:
    spec = project_response.get("spec", project_response)
    name = spec.get("name", "")
    tags = spec.get("tags", {})
    description = spec.get("description", "")
    properties = dict(tags)
    if description:
        properties["description"] = description
    return NamespaceResponse(namespace=[name], properties=properties)


# ==========================================================================
# 2. TABLE MAPPING  (SavedDataset → Iceberg Table)
# ==========================================================================


def _deterministic_uuid(prefix: str, namespace: str, name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{prefix}://{namespace}/{name}"))


def _extract_storage_path(saved_dataset: Dict[str, Any]) -> str:
    """Extract storage location from SavedDataset proto dict."""
    spec = saved_dataset.get("spec", saved_dataset)

    storage = spec.get("storage", {})
    for key in [
        "fileStorage",
        "bigqueryStorage",
        "redshiftStorage",
        "snowflakeStorage",
        "trinoStorage",
        "sparkStorage",
        "athenaStorage",
        "customStorage",
    ]:
        opts = storage.get(key, {})
        if opts:
            for field in ["uri", "path", "table", "query"]:
                val = opts.get(field)
                if val:
                    return str(val)

    return ""


def _extract_schema_from_dataset(saved_dataset: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build Iceberg schema fields from SavedDataset features + schema."""
    spec = saved_dataset.get("spec", saved_dataset)
    fields: List[Dict[str, Any]] = []
    field_id = 1

    schema = spec.get("schema", {})
    if schema:
        for col in schema.get("fields", schema.get("columns", [])):
            fname = col.get("name", "")
            ftype = col.get("type", col.get("valueType", "string"))
            if fname:
                fields.append(
                    {
                        "id": field_id,
                        "name": fname,
                        "type": feast_type_to_iceberg(str(ftype)),
                        "required": False,
                    }
                )
                field_id += 1

    if not fields:
        features = spec.get("features", [])
        for f in features:
            if isinstance(f, str) and ":" in f:
                fname = f.split(":")[-1]
            elif isinstance(f, str):
                fname = f
            elif isinstance(f, dict):
                fname = f.get("name", "")
            else:
                continue
            if fname:
                fields.append(
                    {
                        "id": field_id,
                        "name": fname,
                        "type": "string",
                        "required": False,
                    }
                )
                field_id += 1

    return fields


def saved_dataset_to_load_table_result(
    saved_dataset: Dict[str, Any],
    namespace: str,
) -> LoadTableResult:
    spec = saved_dataset.get("spec", saved_dataset)
    name = spec.get("name", "")
    location = _extract_storage_path(saved_dataset)
    tags = spec.get("tags", {})

    properties: Dict[str, str] = {}
    properties.update(tags)
    properties["asset_type"] = tags.get("asset_type", "table")

    sd_namespace = spec.get("namespace", "")
    sd_collection = spec.get("collection", "")
    sd_description = spec.get("description", "")
    if sd_namespace:
        properties["rhoai.namespace"] = sd_namespace
    if sd_collection:
        properties["rhoai.collection"] = sd_collection
    if sd_description:
        properties["description"] = sd_description

    schema_fields = _extract_schema_from_dataset(saved_dataset)
    table_uuid = _deterministic_uuid("feast", namespace, name)

    metadata = TableMetadata(
        **{
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": location,
            "schemas": [
                Schema(type="struct", **{"schema-id": 0}, fields=schema_fields)
            ],
            "current-schema-id": 0,
            "properties": properties,
        }
    )

    return LoadTableResult(
        **{
            "metadata-location": f"feast://{namespace}/{name}/metadata",
            "metadata": metadata,
            "config": {},
        }
    )


def saved_datasets_to_table_identifiers(
    saved_datasets: List[Dict[str, Any]],
    namespace: str,
    exclude_volumes: bool = True,
) -> List[TableIdentifier]:
    identifiers: List[TableIdentifier] = []
    for sd in saved_datasets:
        spec = sd.get("spec", sd)
        name = spec.get("name", "")
        if not name:
            continue
        tags = spec.get("tags", {})
        if exclude_volumes and tags.get("asset_type") == "volume":
            continue
        identifiers.append(TableIdentifier(namespace=[namespace], name=name))
    return identifiers


# ==========================================================================
# 3. VOLUME MAPPING  (SavedDataset with asset_type=volume → Iceberg Volume)
# ==========================================================================


def saved_dataset_to_volume_info(
    saved_dataset: Dict[str, Any],
    namespace: str,
) -> VolumeInfo:
    spec = saved_dataset.get("spec", saved_dataset)
    name = spec.get("name", "")
    location = _extract_storage_path(saved_dataset)
    tags = spec.get("tags", {})
    meta = saved_dataset.get("meta", {})

    volume_type_str = tags.get("volume_type", "EXTERNAL").upper()
    try:
        volume_type = VolumeType(volume_type_str)
    except ValueError:
        volume_type = VolumeType.EXTERNAL

    properties: Dict[str, str] = {
        k: v for k, v in tags.items() if k not in ("asset_type", "volume_type")
    }

    return VolumeInfo(
        name=name,
        **{
            "volume-type": volume_type,
            "storage-location": location,
        },
        namespace=[namespace],
        comment=spec.get("description", ""),
        properties=properties,
        **{
            "data-source-ref": tags.get("rhoai.data-source-ref"),
            "created-at": meta.get("createdTimestamp", ""),
            "updated-at": meta.get("lastUpdatedTimestamp", ""),
        },
    )


def saved_datasets_to_volumes(
    saved_datasets: List[Dict[str, Any]],
    namespace: str,
) -> ListVolumesResponse:
    volumes: List[VolumeInfo] = []
    for sd in saved_datasets:
        spec = sd.get("spec", sd)
        tags = spec.get("tags", {})
        if tags.get("asset_type") == "volume":
            volumes.append(saved_dataset_to_volume_info(sd, namespace))
    return ListVolumesResponse(volumes=volumes)


# ==========================================================================
# 4. CONNECTION MAPPING  (DataSource → RHOAI Connection)
# ==========================================================================

_SOURCE_TYPE_MAP = {
    "BATCH_FILE": "S3",
    "BATCH_BIGQUERY": "BigQuery",
    "BATCH_SNOWFLAKE": "Snowflake",
    "BATCH_REDSHIFT": "Redshift",
    "BATCH_SPARK": "Spark",
    "BATCH_TRINO": "Trino",
    "BATCH_ATHENA": "Athena",
    "BATCH_ICEBERG": "Iceberg",
    "STREAM_KAFKA": "Kafka",
    "STREAM_KINESIS": "Kinesis",
    "REQUEST_SOURCE": "Request",
    "PUSH_SOURCE": "Push",
    "CUSTOM_SOURCE": "Custom",
}


def _extract_connection_properties(data_source: Dict[str, Any]) -> Dict[str, str]:
    """Extract non-sensitive connection properties from a DataSource proto dict."""
    props: Dict[str, str] = {}
    spec = data_source.get("spec", data_source)

    for key, extract_fields in {
        "fileOptions": ["uri", "s3EndpointOverride", "fileFormat"],
        "bigqueryOptions": ["table", "query"],
        "snowflakeOptions": ["table", "database", "schema", "warehouse"],
        "redshiftOptions": ["table", "database", "schema"],
        "kafkaOptions": ["kafkaBootstrapServers", "topic"],
        "sparkOptions": ["table", "path", "fileFormat", "tableFormat"],
        "trinoOptions": ["table", "query"],
        "athenaOptions": ["table", "database", "dataSource"],
        "kinesisOptions": ["region", "streamName"],
        "customOptions": ["configuration"],
    }.items():
        opts = spec.get(key, {})
        if opts:
            for field in extract_fields:
                val = opts.get(field)
                if val:
                    props[field] = str(val)[:200]
            break

    return props


def _resolve_source_type(data_source: Dict[str, Any]) -> str:
    spec = data_source.get("spec", data_source)
    raw_type = spec.get("type", "UNKNOWN")
    return _SOURCE_TYPE_MAP.get(str(raw_type), str(raw_type))


def data_source_to_connection_info(
    data_source: Dict[str, Any],
) -> ConnectionInfo:
    spec = data_source.get("spec", data_source)
    name = spec.get("name", data_source.get("name", ""))
    meta = data_source.get("meta", {})

    return ConnectionInfo(
        name=name,
        **{"connection-type": _resolve_source_type(data_source)},
        properties=_extract_connection_properties(data_source),
        owner=spec.get("owner", ""),
        description=spec.get("description", ""),
        **{
            "created-at": meta.get("createdTimestamp", ""),
            "updated-at": meta.get("lastUpdatedTimestamp", ""),
        },
    )


def data_sources_to_connections(
    data_sources_response: Dict[str, Any],
) -> ListConnectionsResponse:
    data_sources = data_sources_response.get("dataSources", [])
    connections = [data_source_to_connection_info(ds) for ds in data_sources]
    return ListConnectionsResponse(connections=connections)


# ==========================================================================
# 5. CREDENTIAL VENDING  (DataSource → Iceberg config block)
# ==========================================================================


def _extract_location_from_data_source(data_source: Dict[str, Any]) -> str:
    """Best-effort location extraction from a DataSource proto dict."""
    spec = data_source.get("spec", data_source)
    for key in [
        "fileOptions",
        "bigqueryOptions",
        "redshiftOptions",
        "snowflakeOptions",
        "sparkOptions",
        "customOptions",
    ]:
        opts = spec.get(key, {})
        if opts:
            for field in ["uri", "path", "table", "query"]:
                val = opts.get(field)
                if val:
                    return str(val)
    return ""


def build_credential_config(
    data_source: Dict[str, Any],
    k8s_secret: Optional[Dict[str, str]] = None,
) -> CredentialResponse:
    """Build an Iceberg-spec-compliant credential config block.

    In Phase 1 this returns static credentials from the RHAI Connection
    (K8s Secret). In Phase 2 this would call STS for scoped temp creds.
    """
    config: Dict[str, str] = {}

    if k8s_secret:
        access_key = k8s_secret.get("AWS_ACCESS_KEY_ID", "")
        secret_key = k8s_secret.get("AWS_SECRET_ACCESS_KEY", "")
        endpoint = k8s_secret.get("AWS_S3_ENDPOINT", "")
        region = k8s_secret.get("AWS_DEFAULT_REGION", "")

        if access_key:
            config["s3.access-key-id"] = access_key
        if secret_key:
            config["s3.secret-access-key"] = secret_key
        if endpoint:
            config["s3.endpoint"] = endpoint
        if region:
            config["s3.region"] = region

        gcs_token = k8s_secret.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if gcs_token:
            config["gcs.oauth2.token"] = gcs_token

    return CredentialResponse(config=config)
