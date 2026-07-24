"""
Connection endpoints — RHOAI extension to the Iceberg REST Catalog API.

Maps Feast DataSources to connections, following the Databricks
``CREATE CONNECTION`` pattern. Connections represent typed storage
backends with non-sensitive configuration metadata.

  GET    /{prefix}/connections                → List all connections
  GET    /{prefix}/connections/all            → List across all projects
  GET    /{prefix}/connections/{name}         → Get connection details
  POST   /{prefix}/connections                → Create connection (ApplyDataSource)
  DELETE /{prefix}/connections/{name}         → Delete connection
  POST   /{prefix}/connections/{name}/test    → Test connection (validate)
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Query, Response

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.registry import RegistryServer_pb2

from .mapping import (
    data_source_to_connection_info,
    data_sources_to_connections,
)
from .models import (
    ConnectionInfo,
    CreateConnectionRequest,
    IcebergErrorResponse,
    ListConnectionsResponse,
)

logger = logging.getLogger(__name__)

_CONNECTION_TYPE_TO_SOURCE_TYPE = {
    "S3": DataSourceProto.BATCH_FILE,
    "File": DataSourceProto.BATCH_FILE,
    "BigQuery": DataSourceProto.BATCH_BIGQUERY,
    "Snowflake": DataSourceProto.BATCH_SNOWFLAKE,
    "Redshift": DataSourceProto.BATCH_REDSHIFT,
    "Spark": DataSourceProto.BATCH_SPARK,
    "Trino": DataSourceProto.BATCH_TRINO,
    "Athena": DataSourceProto.BATCH_ATHENA,
    "Iceberg": DataSourceProto.BATCH_ICEBERG,
    "Kafka": DataSourceProto.STREAM_KAFKA,
    "Kinesis": DataSourceProto.STREAM_KINESIS,
    "Custom": DataSourceProto.CUSTOM_SOURCE,
}


def get_connection_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/{prefix}/connections",
        response_model=ListConnectionsResponse,
        summary="List connections in a project (RHOAI extension)",
    )
    def list_connections(prefix: str, project: str = Query(...)):
        req = RegistryServer_pb2.ListDataSourcesRequest(
            project=project, allow_cache=True
        )
        try:
            response = grpc_call(grpc_handler.ListDataSources, req)
        except Exception:
            return ListConnectionsResponse(connections=[])
        return data_sources_to_connections(response)

    @router.get(
        "/{prefix}/connections/all",
        response_model=ListConnectionsResponse,
        summary="List connections across all projects (RHOAI extension)",
    )
    def list_all_connections(prefix: str):
        proj_req = RegistryServer_pb2.ListProjectsRequest(allow_cache=True)
        try:
            proj_response = grpc_call(grpc_handler.ListProjects, proj_req)
        except Exception:
            return ListConnectionsResponse(connections=[])

        all_connections = []
        for proj in proj_response.get("projects", []):
            proj_name = proj.get("spec", {}).get("name", proj.get("name", ""))
            if not proj_name:
                continue
            try:
                ds_req = RegistryServer_pb2.ListDataSourcesRequest(
                    project=proj_name, allow_cache=True
                )
                ds_response = grpc_call(grpc_handler.ListDataSources, ds_req)
                result = data_sources_to_connections(ds_response)
                for conn in result.connections:
                    conn.properties["project"] = proj_name
                all_connections.extend(result.connections)
            except Exception:
                continue

        return ListConnectionsResponse(connections=all_connections)

    @router.get(
        "/{prefix}/connections/{name}",
        response_model=ConnectionInfo,
        summary="Get connection details (RHOAI extension)",
    )
    def get_connection(
        prefix: str,
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.GetDataSourceRequest(
            name=name, project=project, allow_cache=True
        )
        try:
            data_source = grpc_call(grpc_handler.GetDataSource, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Connection does not exist: {name}",
                    type="NoSuchConnectionException",
                    code=404,
                ).model_dump(),
            )
        return data_source_to_connection_info(data_source)

    @router.post(
        "/{prefix}/connections",
        response_model=ConnectionInfo,
        status_code=201,
        summary="Create a connection (RHOAI extension)",
    )
    def create_connection(prefix: str, body: CreateConnectionRequest):
        source_type = _CONNECTION_TYPE_TO_SOURCE_TYPE.get(
            body.connection_type, DataSourceProto.CUSTOM_SOURCE
        )

        ds_proto = DataSourceProto(
            name=body.name,
            description=body.description or "",
            owner=body.owner or "",
            tags=body.properties,
        )
        ds_proto.type = source_type

        _apply_source_options(ds_proto, body.connection_type, body.properties)

        req = RegistryServer_pb2.ApplyDataSourceRequest(
            data_source=ds_proto,
            project=body.project,
            commit=True,
        )
        try:
            grpc_call(grpc_handler.ApplyDataSource, req)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=IcebergErrorResponse(
                    message=f"Failed to create connection: {e}",
                    type="ServerError",
                    code=500,
                ).model_dump(),
            ) from e

        get_req = RegistryServer_pb2.GetDataSourceRequest(
            name=body.name, project=body.project, allow_cache=False
        )
        data_source = grpc_call(grpc_handler.GetDataSource, get_req)
        return data_source_to_connection_info(data_source)

    @router.delete(
        "/{prefix}/connections/{name}",
        status_code=204,
        summary="Delete a connection (RHOAI extension)",
    )
    def delete_connection(
        prefix: str,
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.DeleteDataSourceRequest(
            name=name, project=project, commit=True
        )
        try:
            grpc_call(grpc_handler.DeleteDataSource, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Connection does not exist: {name}",
                    type="NoSuchConnectionException",
                    code=404,
                ).model_dump(),
            )
        return Response(status_code=204)

    @router.post(
        "/{prefix}/connections/{name}/test",
        summary="Test a connection (RHOAI extension)",
    )
    def test_connection(
        prefix: str,
        name: str,
        project: str = Query(...),
    ):
        req = RegistryServer_pb2.GetDataSourceRequest(
            name=name, project=project, allow_cache=False
        )
        try:
            grpc_call(grpc_handler.GetDataSource, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Connection does not exist: {name}",
                    type="NoSuchConnectionException",
                    code=404,
                ).model_dump(),
            )

        return {
            "name": name,
            "status": "ok",
            "message": f"Connection '{name}' exists and is configured. "
            "Live connectivity test requires runtime validation.",
        }

    return router


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_source_options(
    ds_proto: DataSourceProto,
    connection_type: str,
    properties: dict,
):
    """Populate DataSource-specific option protos from connection properties."""
    conn_type = connection_type.upper()

    if conn_type in ("S3", "FILE"):
        uri = properties.get("uri", properties.get("path", ""))
        endpoint = properties.get("s3EndpointOverride", properties.get("endpoint", ""))
        if uri:
            ds_proto.file_options.uri = uri
        if endpoint:
            ds_proto.file_options.s3_endpoint_override = endpoint

    elif conn_type == "BIGQUERY":
        table = properties.get("table", "")
        query = properties.get("query", "")
        if table:
            ds_proto.bigquery_options.table = table
        if query:
            ds_proto.bigquery_options.query = query

    elif conn_type == "SNOWFLAKE":
        for field in ["table", "database", "schema", "warehouse", "query"]:
            val = properties.get(field, "")
            if val:
                setattr(ds_proto.snowflake_options, field, val)

    elif conn_type == "REDSHIFT":
        for field in ["table", "database", "schema", "query"]:
            val = properties.get(field, "")
            if val:
                setattr(ds_proto.redshift_options, field, val)

    elif conn_type == "KAFKA":
        servers = properties.get(
            "kafkaBootstrapServers", properties.get("bootstrap_servers", "")
        )
        topic = properties.get("topic", "")
        if servers:
            ds_proto.kafka_options.kafka_bootstrap_servers = servers
        if topic:
            ds_proto.kafka_options.topic = topic

    elif conn_type == "SPARK":
        for field in ["table", "path", "query", "file_format"]:
            val = properties.get(field, "")
            if val:
                setattr(ds_proto.spark_options, field.replace("_", ""), val)

    elif conn_type == "TRINO":
        table = properties.get("table", "")
        query = properties.get("query", "")
        if table:
            ds_proto.trino_options.table = table
        if query:
            ds_proto.trino_options.query = query

    elif conn_type == "ATHENA":
        for field in ["table", "database", "query"]:
            val = properties.get(field, "")
            if val:
                setattr(ds_proto.athena_options, field, val)
        ds_val = properties.get("dataSource", "")
        if ds_val:
            ds_proto.athena_options.data_source = ds_val

    elif conn_type == "KINESIS":
        region = properties.get("region", "")
        stream = properties.get("streamName", properties.get("stream_name", ""))
        if region:
            ds_proto.kinesis_options.region = region
        if stream:
            ds_proto.kinesis_options.stream_name = stream

    elif conn_type == "ICEBERG":
        config = {
            "catalog_type": properties.get("catalog_type", "rest"),
            "endpoint": properties.get("endpoint", ""),
            "warehouse": properties.get("warehouse", ""),
            "namespace": properties.get("namespace", ""),
            "table": properties.get("table", ""),
        }
        ds_proto.custom_options.configuration = json.dumps(config).encode("utf-8")
        ds_proto.data_source_class_type = (
            "feast.infra.data_sources.contrib.iceberg_catalog"
            ".iceberg_source.IcebergSource"
        )

    else:
        config_str = properties.get("configuration", json.dumps(properties))
        ds_proto.custom_options.configuration = config_str.encode("utf-8")
