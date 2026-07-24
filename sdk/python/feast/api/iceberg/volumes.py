"""
Volume endpoints — RHOAI extension to the Iceberg REST Catalog API.

Volumes represent unstructured data (PDFs, images, documents) stored
in S3/GCS/MinIO. Backed by SavedDatasets with tags["asset_type"] = "volume".

  GET    /{prefix}/namespaces/{ns}/volumes           → List volumes
  GET    /{prefix}/namespaces/{ns}/volumes/{v}        → Get volume
  POST   /{prefix}/namespaces/{ns}/volumes            → Create volume
  PUT    /{prefix}/namespaces/{ns}/volumes/{v}         → Update volume
  DELETE /{prefix}/namespaces/{ns}/volumes/{v}         → Delete volume
  HEAD   /{prefix}/namespaces/{ns}/volumes/{v}         → Check existence
  GET    /{prefix}/namespaces/{ns}/volumes/{v}/credentials → Credential vending
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
    saved_dataset_to_volume_info,
    saved_datasets_to_volumes,
)
from .models import (
    CreateVolumeRequest,
    CredentialResponse,
    IcebergErrorResponse,
    ListVolumesResponse,
    VolumeInfo,
)

logger = logging.getLogger(__name__)


def get_volume_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/{prefix}/namespaces/{namespace}/volumes",
        response_model=ListVolumesResponse,
        summary="List volumes in a namespace (RHOAI extension)",
    )
    def list_volumes(
        prefix: str,
        namespace: str,
        page_token: Optional[str] = Query(None, alias="pageToken"),
        page_size: Optional[int] = Query(None, alias="pageSize"),
    ):
        try:
            req = RegistryServer_pb2.ListSavedDatasetsRequest(
                project=namespace, allow_cache=True
            )
            response = grpc_call(grpc_handler.ListSavedDatasets, req)
            saved_datasets = response.get("savedDatasets", [])
            return saved_datasets_to_volumes(saved_datasets, namespace)
        except Exception:
            return ListVolumesResponse(volumes=[])

    @router.get(
        "/{prefix}/namespaces/{namespace}/volumes/{volume}",
        response_model=VolumeInfo,
        summary="Get a volume (RHOAI extension)",
    )
    def get_volume(prefix: str, namespace: str, volume: str):
        sd = _get_volume_raw(grpc_handler, namespace, volume)
        return saved_dataset_to_volume_info(sd, namespace)

    @router.head(
        "/{prefix}/namespaces/{namespace}/volumes/{volume}",
        summary="Check volume existence (RHOAI extension)",
    )
    def volume_exists(prefix: str, namespace: str, volume: str):
        try:
            _get_volume_raw(grpc_handler, namespace, volume)
            return Response(status_code=204)
        except HTTPException:
            raise HTTPException(status_code=404)

    @router.post(
        "/{prefix}/namespaces/{namespace}/volumes",
        response_model=VolumeInfo,
        status_code=201,
        summary="Create a volume (RHOAI extension)",
    )
    def create_volume(prefix: str, namespace: str, body: CreateVolumeRequest):
        try:
            req = RegistryServer_pb2.GetSavedDatasetRequest(
                name=body.name, project=namespace, allow_cache=False
            )
            grpc_call(grpc_handler.GetSavedDataset, req)
            raise HTTPException(
                status_code=409,
                detail=IcebergErrorResponse(
                    message=f"Volume already exists: {namespace}.{body.name}",
                    type="AlreadyExistsException",
                    code=409,
                ).model_dump(),
            )
        except HTTPException:
            raise
        except Exception:
            pass

        storage_proto = SavedDatasetStorageProto()
        storage_proto.file_storage.CopyFrom(
            DataSourceProto.FileOptions(uri=body.storage_location)
        )

        tags = dict(body.properties)
        tags["asset_type"] = "volume"
        tags["volume_type"] = body.volume_type.value
        if body.data_source_ref:
            tags["rhoai.data-source-ref"] = body.data_source_ref

        spec = SavedDatasetSpec(
            name=body.name,
            project=namespace,
            features=[],
            join_keys=[],
            storage=storage_proto,
            tags=tags,
            description=body.comment or "",
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
                    message=f"Failed to create volume: {e}",
                    type="ServerError",
                    code=500,
                ).model_dump(),
            ) from e

        sd = _get_volume_raw(grpc_handler, namespace, body.name)
        return saved_dataset_to_volume_info(sd, namespace)

    @router.put(
        "/{prefix}/namespaces/{namespace}/volumes/{volume}",
        response_model=VolumeInfo,
        summary="Update a volume (RHOAI extension)",
    )
    def update_volume(
        prefix: str, namespace: str, volume: str, body: CreateVolumeRequest
    ):
        _get_volume_raw(grpc_handler, namespace, volume)

        storage_proto = SavedDatasetStorageProto()
        storage_proto.file_storage.CopyFrom(
            DataSourceProto.FileOptions(uri=body.storage_location)
        )

        tags = dict(body.properties)
        tags["asset_type"] = "volume"
        tags["volume_type"] = body.volume_type.value
        if body.data_source_ref:
            tags["rhoai.data-source-ref"] = body.data_source_ref

        spec = SavedDatasetSpec(
            name=volume,
            project=namespace,
            features=[],
            join_keys=[],
            storage=storage_proto,
            tags=tags,
            description=body.comment or "",
        )
        saved_dataset_proto = SavedDatasetProto(spec=spec, meta=SavedDatasetMeta())
        apply_req = RegistryServer_pb2.ApplySavedDatasetRequest(
            saved_dataset=saved_dataset_proto,
            project=namespace,
            commit=True,
        )
        grpc_call(grpc_handler.ApplySavedDataset, apply_req)

        sd = _get_volume_raw(grpc_handler, namespace, volume)
        return saved_dataset_to_volume_info(sd, namespace)

    @router.delete(
        "/{prefix}/namespaces/{namespace}/volumes/{volume}",
        status_code=204,
        summary="Delete a volume (RHOAI extension)",
    )
    def delete_volume(prefix: str, namespace: str, volume: str):
        _get_volume_raw(grpc_handler, namespace, volume)
        req = RegistryServer_pb2.DeleteSavedDatasetRequest(
            name=volume, project=namespace, commit=True
        )
        try:
            grpc_call(grpc_handler.DeleteSavedDataset, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Volume does not exist: {namespace}.{volume}",
                    type="NoSuchVolumeException",
                    code=404,
                ).model_dump(),
            )
        return Response(status_code=204)

    @router.get(
        "/{prefix}/namespaces/{namespace}/volumes/{volume}/credentials",
        response_model=CredentialResponse,
        summary="Vend credentials for a volume (RHOAI extension)",
    )
    def vend_volume_credentials(prefix: str, namespace: str, volume: str):
        sd = _get_volume_raw(grpc_handler, namespace, volume)
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


def _get_volume_raw(grpc_handler, namespace: str, name: str):
    try:
        req = RegistryServer_pb2.GetSavedDatasetRequest(
            name=name, project=namespace, allow_cache=True
        )
        sd = grpc_call(grpc_handler.GetSavedDataset, req)
    except Exception:
        raise HTTPException(
            status_code=404,
            detail=IcebergErrorResponse(
                message=f"Volume does not exist: {namespace}.{name}",
                type="NoSuchVolumeException",
                code=404,
            ).model_dump(),
        )

    spec = sd.get("spec", sd)
    tags = spec.get("tags", {})
    if tags.get("asset_type") != "volume":
        raise HTTPException(
            status_code=404,
            detail=IcebergErrorResponse(
                message=f"'{name}' exists but is not a volume (asset_type={tags.get('asset_type', 'table')})",
                type="NoSuchVolumeException",
                code=404,
            ).model_dump(),
        )
    return sd
