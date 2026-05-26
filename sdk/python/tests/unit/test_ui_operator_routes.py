"""Unit tests for Feast UI operator management routes and RBAC."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def set_operator_env(monkeypatch):
    monkeypatch.setenv("FEAST_OPERATOR_MANAGED", "true")


@pytest.fixture
def mock_k8s():
    """Mock the kubernetes module to avoid needing a real cluster."""
    with (
        patch("feast.ui_operator_rbac.config") as mock_config,
        patch("feast.ui_operator_rbac.client") as mock_client,
        patch("feast.ui_operator_routes.config") as mock_routes_config,
        patch("feast.ui_operator_routes.client") as mock_routes_client,
    ):
        mock_config.load_incluster_config = MagicMock()
        mock_routes_config.load_incluster_config = MagicMock()

        mock_authz = MagicMock()
        mock_client.AuthorizationV1Api.return_value = mock_authz
        mock_client.CoreV1Api.return_value = MagicMock()

        sar_response = MagicMock()
        sar_response.status.allowed = True
        mock_authz.create_subject_access_review.return_value = sar_response

        mock_routes_client.Configuration.return_value = MagicMock()
        api_client_mock = MagicMock()
        mock_routes_client.ApiClient.return_value = api_client_mock
        mock_custom_api = MagicMock()
        mock_routes_client.CustomObjectsApi.return_value = mock_custom_api
        mock_core_api = MagicMock()
        mock_routes_client.CoreV1Api.return_value = mock_core_api

        yield {
            "authz": mock_authz,
            "custom_api": mock_custom_api,
            "core_api": mock_core_api,
            "sar_response": sar_response,
        }


@pytest.fixture
def app_with_routes(mock_k8s):
    """Create a FastAPI app with operator routes mounted."""
    app = FastAPI()
    from feast.ui_operator_routes import mount_operator_routes

    mount_operator_routes(app)
    return app


@pytest.fixture
def client(app_with_routes):
    return TestClient(app_with_routes)


class TestOperatorPermissions:
    def test_permissions_endpoint_no_user(self, client):
        response = client.get("/api/operator/permissions")
        assert response.status_code == 200
        data = response.json()
        assert data["canList"] is False
        assert data["canCreate"] is False

    def test_permissions_endpoint_with_user(self, client, mock_k8s):
        mock_k8s["core_api"].list_namespace.return_value = MagicMock(items=[])
        response = client.get(
            "/api/operator/permissions",
            headers={"X-Forwarded-User": "testuser", "X-Forwarded-Groups": "dev,admin"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["canList"] is True
        assert data["canCreate"] is True


class TestListFeatureStores:
    def test_list_all(self, client, mock_k8s):
        mock_k8s["custom_api"].list_cluster_custom_object.return_value = {
            "items": [
                {
                    "metadata": {"name": "test-fs", "namespace": "default"},
                    "spec": {},
                }
            ]
        }
        response = client.get(
            "/api/operator/featurestores",
            headers={"X-Forwarded-User": "admin"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 1

    def test_list_by_namespace(self, client, mock_k8s):
        mock_k8s["custom_api"].list_namespaced_custom_object.return_value = {
            "items": []
        }
        response = client.get(
            "/api/operator/featurestores?namespace=test-ns",
            headers={"X-Forwarded-User": "admin"},
        )
        assert response.status_code == 200

    def test_list_unauthorized(self, client, mock_k8s):
        mock_k8s["sar_response"].status.allowed = False
        response = client.get(
            "/api/operator/featurestores",
            headers={"X-Forwarded-User": "viewer"},
        )
        assert response.status_code == 403


class TestCreateFeatureStore:
    def test_create_success(self, client, mock_k8s):
        mock_k8s["custom_api"].create_namespaced_custom_object.return_value = {
            "metadata": {"name": "new-fs", "namespace": "test-ns"},
        }
        body = {
            "apiVersion": "feast.dev/v1",
            "kind": "FeatureStore",
            "metadata": {"name": "new-fs", "namespace": "test-ns"},
            "spec": {"feastProject": "myproject"},
        }
        response = client.post(
            "/api/operator/featurestores",
            json=body,
            headers={"X-Forwarded-User": "admin"},
        )
        assert response.status_code == 200

    def test_create_no_auth(self, client):
        response = client.post(
            "/api/operator/featurestores",
            json={"metadata": {"name": "x", "namespace": "y"}, "spec": {}},
        )
        assert response.status_code == 401


class TestDeleteFeatureStore:
    def test_delete_success(self, client, mock_k8s):
        mock_k8s["custom_api"].delete_namespaced_custom_object.return_value = {}
        response = client.delete(
            "/api/operator/featurestores/test-ns/test-fs",
            headers={"X-Forwarded-User": "admin"},
        )
        assert response.status_code == 200

    def test_delete_forbidden(self, client, mock_k8s):
        mock_k8s["sar_response"].status.allowed = False
        response = client.delete(
            "/api/operator/featurestores/test-ns/test-fs",
            headers={"X-Forwarded-User": "viewer"},
        )
        assert response.status_code == 403


class TestNamespaces:
    def test_list_namespaces(self, client, mock_k8s):
        ns1 = MagicMock()
        ns1.metadata.name = "my-namespace"
        ns2 = MagicMock()
        ns2.metadata.name = "kube-system"
        mock_k8s["core_api"].list_namespace.return_value = MagicMock(items=[ns1, ns2])
        response = client.get(
            "/api/operator/namespaces",
            headers={"X-Forwarded-User": "admin"},
        )
        assert response.status_code == 200
        data = response.json()
        names = [item["name"] for item in data["items"]]
        assert "my-namespace" in names
        assert "kube-system" not in names

    def test_list_namespaces_no_auth(self, client):
        response = client.get("/api/operator/namespaces")
        assert response.status_code == 401


class TestOperatorStatusEndpoint:
    def test_operator_status_disabled(self, monkeypatch):
        """When env var is not set, operator mode is disabled."""
        monkeypatch.delenv("FEAST_OPERATOR_MANAGED", raising=False)
        from feast.ui_server import _try_enable_operator_routes

        app = FastAPI()
        result = _try_enable_operator_routes(app)
        assert result is False
