"""Standalone entry point for the centralized Feast management UI.

This server starts WITHOUT requiring a feature_store.yaml or registry connection.
It only serves the React frontend and the operator management/federation endpoints.
Used by the central UI deployment managed by the Feast operator.

Usage:
    python -m feast.central_ui_server --host 0.0.0.0 --port 8888
"""

import argparse
import logging
import os
from importlib import resources as importlib_resources
from typing import Dict

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

_registry_map: Dict[str, str] = {}


def _read_sa_token() -> str:
    try:
        with open(_TOKEN_PATH) as f:
            return f.read().strip()
    except OSError:
        return ""


def _build_registry_map(custom_api, group, version, plural) -> Dict[str, str]:
    """Build a mapping of project_id -> internal registry REST base URL."""
    mapping: Dict[str, str] = {}
    try:
        result = custom_api.list_cluster_custom_object(group, version, plural)
        for fs in result.get("items", []):
            metadata = fs.get("metadata", {})
            fs_status = fs.get("status", {})
            name = metadata.get("name", "")
            namespace = metadata.get("namespace", "")
            feast_project = fs.get("spec", {}).get("feastProject", name)

            hostnames = fs_status.get("serviceHostnames", {})
            registry_rest = hostnames.get("registryRest", "")
            if not registry_rest:
                registry_rest = (
                    f"feast-{name}-registry-rest.{namespace}.svc.cluster.local"
                )

            if ":443" in registry_rest or not registry_rest.endswith(":80"):
                base_url = f"https://{registry_rest}"
            else:
                base_url = f"http://{registry_rest}"

            mapping[feast_project] = base_url
    except Exception as e:
        logger.warning(f"Failed to build registry map: {e}")
    return mapping


def create_app() -> FastAPI:
    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    operator_enabled = False
    if os.environ.get("FEAST_OPERATOR_MANAGED", "").lower() == "true":
        try:
            from feast.ui_operator_routes import mount_operator_routes

            mount_operator_routes(app)
            operator_enabled = True
            logger.info("Operator management routes enabled at /api/operator/*")
        except ImportError:
            logger.warning("kubernetes package not installed; operator routes disabled")
        except Exception as e:
            logger.warning(f"Failed to enable operator routes: {e}")

    @app.get("/api/operator/status")
    def operator_status():
        return {"enabled": operator_enabled}

    @app.get("/api/debug/headers")
    def debug_headers(request: Request):
        return {k: v for k, v in request.headers.items()}

    @app.get("/healthz")
    def healthz():
        return Response(status_code=status.HTTP_200_OK)

    @app.get("/health")
    def health():
        return Response(status_code=status.HTTP_200_OK)

    @app.get("/projects-list.json")
    async def projects_list(request: Request):
        """Dynamically build project list from discovered FeatureStores."""
        global _registry_map
        if not operator_enabled:
            return {"projects": []}
        try:
            from kubernetes import client as k8s_client

            from feast.ui_operator_routes import (
                FEAST_GROUP,
                FEAST_PLURAL,
                FEAST_VERSION,
                _make_k8s_configuration,
            )

            configuration = _make_k8s_configuration()
            custom_api = k8s_client.CustomObjectsApi(
                k8s_client.ApiClient(configuration)
            )
            result = custom_api.list_cluster_custom_object(
                FEAST_GROUP, FEAST_VERSION, FEAST_PLURAL
            )

            _registry_map = _build_registry_map(
                custom_api, FEAST_GROUP, FEAST_VERSION, FEAST_PLURAL
            )

            projects = []
            for fs in result.get("items", []):
                name = fs.get("metadata", {}).get("name", "")
                namespace = fs.get("metadata", {}).get("namespace", "")
                feast_project = fs.get("spec", {}).get("feastProject", name)
                status_phase = fs.get("status", {}).get("phase", "")

                if status_phase.lower() in ("ready", ""):
                    projects.append(
                        {
                            "id": feast_project,
                            "name": f"{feast_project} ({namespace})",
                            "description": f"FeatureStore '{name}' in namespace '{namespace}'",
                            "registryPath": f"/api/registry-proxy/{feast_project}",
                        }
                    )
            return {"projects": projects}
        except Exception as e:
            logger.warning(f"Failed to build dynamic projects list: {e}")
            return {"projects": []}

    @app.api_route("/api/registry-proxy/{project_id}/{path:path}", methods=["GET"])
    async def registry_proxy(project_id: str, path: str, request: Request):
        """Proxy registry REST API requests to internal cluster services."""
        global _registry_map

        if not _registry_map:
            if operator_enabled:
                try:
                    from kubernetes import client as k8s_client

                    from feast.ui_operator_routes import (
                        FEAST_GROUP,
                        FEAST_PLURAL,
                        FEAST_VERSION,
                        _make_k8s_configuration,
                    )

                    configuration = _make_k8s_configuration()
                    custom_api = k8s_client.CustomObjectsApi(
                        k8s_client.ApiClient(configuration)
                    )
                    _registry_map = _build_registry_map(
                        custom_api, FEAST_GROUP, FEAST_VERSION, FEAST_PLURAL
                    )
                except Exception as e:
                    logger.warning(f"Failed to refresh registry map: {e}")

        base_url = _registry_map.get(project_id)
        if not base_url:
            return JSONResponse(
                status_code=404,
                content={"detail": f"No registry found for project '{project_id}'"},
            )

        target_url = f"{base_url}/api/v1/{path}"
        params = dict(request.query_params)
        token = _read_sa_token()
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            async with httpx.AsyncClient(verify=False) as client:
                resp = await client.get(
                    target_url, params=params, headers=headers, timeout=10.0
                )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                media_type=resp.headers.get("content-type", "application/json"),
            )
        except Exception as e:
            logger.warning(f"Registry proxy error for {project_id}/{path}: {e}")
            return JSONResponse(
                status_code=502,
                content={"detail": f"Failed to reach registry: {e}"},
            )

    ui_dir_ref = importlib_resources.files("feast") / "ui/build/"
    with importlib_resources.as_file(ui_dir_ref) as ui_dir:
        if ui_dir.exists():
            app.mount(
                "/",
                StaticFiles(directory=str(ui_dir), html=True),
                name="static",
            )
        else:
            logger.warning(f"UI build directory not found at {ui_dir}")

    return app


def main():
    parser = argparse.ArgumentParser(description="Feast Central UI Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8888, help="Bind port")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    app = create_app()

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
