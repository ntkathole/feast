"""
Table endpoints following the Iceberg REST Catalog API spec.

Maps SavedDatasets to Iceberg Tables with full CRUD.
  GET    /{prefix}/namespaces/{ns}/tables          → ListSavedDatasets (excluding volumes)
  GET    /{prefix}/namespaces/{ns}/tables/{t}       → GetSavedDataset → LoadTableResult
  POST   /{prefix}/namespaces/{ns}/tables           → ApplySavedDataset (create)
  PUT    /{prefix}/namespaces/{ns}/tables/{t}        → ApplySavedDataset (update)
  DELETE /{prefix}/namespaces/{ns}/tables/{t}        → DeleteSavedDataset
  HEAD   /{prefix}/namespaces/{ns}/tables/{t}        → Check existence
  GET    /{prefix}/namespaces/{ns}/tables/{t}/credentials → Credential vending
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Response

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDataset as SavedDatasetProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetMeta,
    SavedDatasetSpec,
)
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.protos.feast.registry import RegistryServer_pb2

from .mapping import (
    build_credential_config,
    saved_dataset_to_load_table_result,
    saved_datasets_to_table_identifiers,
)
from .models import (
    CreateTableRequest,
    CredentialResponse,
    IcebergErrorResponse,
    ListTablesResponse,
    LoadTableResult,
)

logger = logging.getLogger(__name__)


def get_table_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/{prefix}/namespaces/{namespace}/tables",
        response_model=ListTablesResponse,
        summary="List tables in a namespace (Iceberg REST)",
    )
    def list_tables(
        prefix: str,
        namespace: str,
        page_token: Optional[str] = Query(None, alias="pageToken"),
        page_size: Optional[int] = Query(None, alias="pageSize"),
    ):
        identifiers = _list_saved_datasets_as_tables(grpc_handler, namespace)
        return ListTablesResponse(identifiers=identifiers)

    @router.get(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        response_model=LoadTableResult,
        summary="Load a table (Iceberg REST)",
    )
    def load_table(prefix: str, namespace: str, table: str):
        return _get_saved_dataset_as_table(grpc_handler, namespace, table)

    @router.head(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        summary="Check if a table exists (Iceberg REST)",
    )
    def table_exists(prefix: str, namespace: str, table: str):
        try:
            _get_saved_dataset_as_table(grpc_handler, namespace, table)
            return Response(status_code=204)
        except HTTPException:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Table does not exist: {namespace}.{table}",
                    type="NoSuchTableException",
                    code=404,
                ).model_dump(),
            )

    @router.post(
        "/{prefix}/namespaces/{namespace}/tables",
        response_model=LoadTableResult,
        status_code=200,
        summary="Create a table (Iceberg REST)",
    )
    def create_table(prefix: str, namespace: str, body: CreateTableRequest):
        try:
            req = RegistryServer_pb2.GetSavedDatasetRequest(
                name=body.name, project=namespace, allow_cache=False
            )
            grpc_call(grpc_handler.GetSavedDataset, req)
            raise HTTPException(
                status_code=409,
                detail=IcebergErrorResponse(
                    message=f"Table already exists: {namespace}.{body.name}",
                    type="AlreadyExistsException",
                    code=409,
                ).model_dump(),
            )
        except HTTPException:
            raise
        except Exception:
            pass

        storage_proto = SavedDatasetStorageProto()
        location = body.location or ""
        if location:
            storage_proto.file_storage.CopyFrom(
                DataSourceProto.FileOptions(uri=location)
            )

        tags = dict(body.properties)
        tags["asset_type"] = tags.get("asset_type", "table")
        if body.data_source_ref:
            tags["rhoai.data-source-ref"] = body.data_source_ref
        if body.data_source_format:
            tags["rhoai.data-source-format"] = body.data_source_format

        sd_namespace = tags.pop("rhoai.namespace", "")
        sd_collection = tags.pop("rhoai.collection", "")
        sd_description = tags.pop("description", "")

        spec = SavedDatasetSpec(
            name=body.name,
            project=namespace,
            features=[],
            join_keys=[],
            storage=storage_proto,
            tags=tags,
            namespace=sd_namespace,
            collection=sd_collection,
            description=sd_description,
        )

        saved_dataset_proto = SavedDatasetProto(spec=spec, meta=SavedDatasetMeta())

        apply_req = RegistryServer_pb2.ApplySavedDatasetRequest(
            saved_dataset=saved_dataset_proto,
            project=namespace,
            commit=True,
        )
        try:
            grpc_call(grpc_handler.ApplySavedDataset, apply_req)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=IcebergErrorResponse(
                    message=f"Failed to create table: {e}",
                    type="ServerError",
                    code=500,
                ).model_dump(),
            ) from e

        return _get_saved_dataset_as_table(grpc_handler, namespace, body.name)

    @router.put(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        response_model=LoadTableResult,
        summary="Update a table (Iceberg REST — whole-object upsert)",
    )
    def update_table(prefix: str, namespace: str, table: str, body: CreateTableRequest):
        try:
            _get_saved_dataset_as_table(grpc_handler, namespace, table)
        except HTTPException:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Table does not exist: {namespace}.{table}",
                    type="NoSuchTableException",
                    code=404,
                ).model_dump(),
            )

        storage_proto = SavedDatasetStorageProto()
        location = body.location or ""
        if location:
            storage_proto.file_storage.CopyFrom(
                DataSourceProto.FileOptions(uri=location)
            )

        tags = dict(body.properties)
        tags["asset_type"] = tags.get("asset_type", "table")
        if body.data_source_ref:
            tags["rhoai.data-source-ref"] = body.data_source_ref

        sd_namespace = tags.pop("rhoai.namespace", "")
        sd_collection = tags.pop("rhoai.collection", "")
        sd_description = tags.pop("description", "")

        spec = SavedDatasetSpec(
            name=table,
            project=namespace,
            features=[],
            join_keys=[],
            storage=storage_proto,
            tags=tags,
            namespace=sd_namespace,
            collection=sd_collection,
            description=sd_description,
        )
        saved_dataset_proto = SavedDatasetProto(spec=spec, meta=SavedDatasetMeta())
        apply_req = RegistryServer_pb2.ApplySavedDatasetRequest(
            saved_dataset=saved_dataset_proto,
            project=namespace,
            commit=True,
        )
        grpc_call(grpc_handler.ApplySavedDataset, apply_req)

        return _get_saved_dataset_as_table(grpc_handler, namespace, table)

    @router.delete(
        "/{prefix}/namespaces/{namespace}/tables/{table}",
        status_code=204,
        summary="Drop a table (Iceberg REST)",
    )
    def drop_table(prefix: str, namespace: str, table: str):
        req = RegistryServer_pb2.DeleteSavedDatasetRequest(
            name=table, project=namespace, commit=True
        )
        try:
            grpc_call(grpc_handler.DeleteSavedDataset, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Table does not exist: {namespace}.{table}",
                    type="NoSuchTableException",
                    code=404,
                ).model_dump(),
            )
        return Response(status_code=204)

    @router.get(
        "/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
        response_model=CredentialResponse,
        summary="Vend credentials for a table (Iceberg REST extension)",
    )
    def vend_table_credentials(prefix: str, namespace: str, table: str):
        sd = _get_saved_dataset_raw(grpc_handler, namespace, table)
        spec = sd.get("spec", sd)
        tags = spec.get("tags", {})
        ds_ref = tags.get("rhoai.data-source-ref", "")

        if not ds_ref:
            return CredentialResponse(config={})

        try:
            ds_req = RegistryServer_pb2.GetDataSourceRequest(
                name=ds_ref, project=namespace, allow_cache=True
            )
            data_source = grpc_call(grpc_handler.GetDataSource, ds_req)
        except Exception:
            return CredentialResponse(config={})

        return build_credential_config(data_source, k8s_secret=None)

    return router


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _list_saved_datasets_as_tables(grpc_handler, namespace: str):
    try:
        req = RegistryServer_pb2.ListSavedDatasetsRequest(
            project=namespace, allow_cache=True
        )
        response = grpc_call(grpc_handler.ListSavedDatasets, req)
        saved_datasets = response.get("savedDatasets", [])
        return saved_datasets_to_table_identifiers(
            saved_datasets, namespace, exclude_volumes=True
        )
    except Exception:
        logger.debug("No saved datasets found for namespace '%s'", namespace)
        return []


def _get_saved_dataset_raw(grpc_handler, namespace: str, name: str):
    try:
        req = RegistryServer_pb2.GetSavedDatasetRequest(
            name=name, project=namespace, allow_cache=True
        )
        return grpc_call(grpc_handler.GetSavedDataset, req)
    except Exception:
        raise HTTPException(
            status_code=404,
            detail=IcebergErrorResponse(
                message=f"Table does not exist: {namespace}.{name}",
                type="NoSuchTableException",
                code=404,
            ).model_dump(),
        )


def _get_saved_dataset_as_table(
    grpc_handler, namespace: str, name: str
) -> LoadTableResult:
    sd = _get_saved_dataset_raw(grpc_handler, namespace, name)
    return saved_dataset_to_load_table_result(sd, namespace)
