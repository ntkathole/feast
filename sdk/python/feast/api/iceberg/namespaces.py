"""
Namespace endpoints following the Iceberg REST Catalog API spec.

Maps Feast Projects to Iceberg Namespaces with full CRUD.
  GET    /{prefix}/namespaces          → ListProjects
  GET    /{prefix}/namespaces/{ns}     → GetProject
  POST   /{prefix}/namespaces          → ApplyProject (create)
  POST   /{prefix}/namespaces/{ns}/properties → UpdateProject properties
  DELETE /{prefix}/namespaces/{ns}     → DeleteProject
  HEAD   /{prefix}/namespaces/{ns}     → Check existence
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Response

from feast.api.registry.rest.rest_utils import grpc_call
from feast.protos.feast.registry import RegistryServer_pb2

from .mapping import feast_project_to_namespace, feast_projects_to_namespaces
from .models import (
    CreateNamespaceRequest,
    IcebergErrorResponse,
    ListNamespacesResponse,
    NamespaceResponse,
    UpdateNamespacePropertiesRequest,
    UpdateNamespacePropertiesResponse,
)

logger = logging.getLogger(__name__)


def get_namespace_router(grpc_handler) -> APIRouter:
    router = APIRouter()

    @router.get(
        "/{prefix}/namespaces",
        response_model=ListNamespacesResponse,
        summary="List namespaces (Iceberg REST)",
    )
    def list_namespaces(
        prefix: str,
        page_token: Optional[str] = Query(None, alias="pageToken"),
        page_size: Optional[int] = Query(None, alias="pageSize"),
    ):
        req = RegistryServer_pb2.ListProjectsRequest(allow_cache=True)
        response = grpc_call(grpc_handler.ListProjects, req)
        return feast_projects_to_namespaces(response)

    @router.get(
        "/{prefix}/namespaces/{namespace}",
        response_model=NamespaceResponse,
        summary="Get namespace properties (Iceberg REST)",
    )
    def get_namespace(prefix: str, namespace: str):
        req = RegistryServer_pb2.GetProjectRequest(name=namespace, allow_cache=True)
        try:
            response = grpc_call(grpc_handler.GetProject, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Namespace does not exist: {namespace}",
                    type="NoSuchNamespaceException",
                    code=404,
                ).model_dump(),
            )
        return feast_project_to_namespace(response)

    @router.head(
        "/{prefix}/namespaces/{namespace}",
        summary="Check namespace existence (Iceberg REST)",
    )
    def namespace_exists(prefix: str, namespace: str):
        req = RegistryServer_pb2.GetProjectRequest(name=namespace, allow_cache=True)
        try:
            grpc_call(grpc_handler.GetProject, req)
            return Response(status_code=204)
        except Exception:
            raise HTTPException(status_code=404)

    @router.post(
        "/{prefix}/namespaces",
        response_model=NamespaceResponse,
        status_code=200,
        summary="Create a namespace (Iceberg REST)",
    )
    def create_namespace(prefix: str, body: CreateNamespaceRequest):
        if not body.namespace:
            raise HTTPException(
                status_code=400,
                detail=IcebergErrorResponse(
                    message="Namespace must not be empty",
                    type="BadRequestException",
                    code=400,
                ).model_dump(),
            )

        namespace_name = body.namespace[0]

        from feast.protos.feast.core import Project_pb2

        project_spec = Project_pb2.ProjectSpec(
            name=namespace_name,
            tags=body.properties,
        )
        if "description" in body.properties:
            project_spec.description = body.properties["description"]

        project_proto = Project_pb2.Project(spec=project_spec)
        req = RegistryServer_pb2.ApplyProjectRequest(project=project_proto, commit=True)
        try:
            grpc_call(grpc_handler.ApplyProject, req)
        except Exception as e:
            raise HTTPException(
                status_code=409,
                detail=IcebergErrorResponse(
                    message=f"Namespace already exists or could not be created: {namespace_name}",
                    type="AlreadyExistsException",
                    code=409,
                ).model_dump(),
            ) from e

        return NamespaceResponse(
            namespace=body.namespace,
            properties=body.properties,
        )

    @router.post(
        "/{prefix}/namespaces/{namespace}/properties",
        response_model=UpdateNamespacePropertiesResponse,
        summary="Update namespace properties (Iceberg REST)",
    )
    def update_namespace_properties(
        prefix: str,
        namespace: str,
        body: UpdateNamespacePropertiesRequest,
    ):
        req = RegistryServer_pb2.GetProjectRequest(name=namespace, allow_cache=False)
        try:
            project_response = grpc_call(grpc_handler.GetProject, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Namespace does not exist: {namespace}",
                    type="NoSuchNamespaceException",
                    code=404,
                ).model_dump(),
            )

        spec = project_response.get("spec", project_response)
        current_tags = dict(spec.get("tags", {}))

        removed = []
        missing = []
        for key in body.removals:
            if key in current_tags:
                del current_tags[key]
                removed.append(key)
            else:
                missing.append(key)

        current_tags.update(body.updates)
        updated = list(body.updates.keys())

        from feast.protos.feast.core import Project_pb2

        project_spec = Project_pb2.ProjectSpec(
            name=namespace,
            tags=current_tags,
        )
        project_proto = Project_pb2.Project(spec=project_spec)
        apply_req = RegistryServer_pb2.ApplyProjectRequest(
            project=project_proto, commit=True
        )
        grpc_call(grpc_handler.ApplyProject, apply_req)

        return UpdateNamespacePropertiesResponse(
            updated=updated, removed=removed, missing=missing
        )

    @router.delete(
        "/{prefix}/namespaces/{namespace}",
        status_code=204,
        summary="Delete a namespace (Iceberg REST)",
    )
    def delete_namespace(prefix: str, namespace: str):
        req = RegistryServer_pb2.DeleteProjectRequest(name=namespace, commit=True)
        try:
            grpc_call(grpc_handler.DeleteProject, req)
        except Exception:
            raise HTTPException(
                status_code=404,
                detail=IcebergErrorResponse(
                    message=f"Namespace does not exist: {namespace}",
                    type="NoSuchNamespaceException",
                    code=404,
                ).model_dump(),
            )
        return Response(status_code=204)

    return router
