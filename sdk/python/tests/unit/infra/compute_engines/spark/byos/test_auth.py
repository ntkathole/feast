import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from feast.errors import FeastSparkClusterError
from feast.infra.compute_engines.spark.byos.auth import get_k8s_api_client


@patch("feast.infra.compute_engines.spark.byos.auth.config")
@patch("feast.infra.compute_engines.spark.byos.auth.client")
def test_in_cluster_auth(mock_client, mock_config):
    mock_api_client = MagicMock()
    mock_client.ApiClient.return_value = mock_api_client

    result = get_k8s_api_client(kubeconfig_path=None)

    mock_config.load_incluster_config.assert_called_once()
    mock_config.load_kube_config.assert_not_called()
    assert result == mock_api_client


@patch("feast.infra.compute_engines.spark.byos.auth.config")
@patch("feast.infra.compute_engines.spark.byos.auth.client")
def test_kubeconfig_auth(mock_client, mock_config):
    mock_api_client = MagicMock()
    mock_client.ApiClient.return_value = mock_api_client

    with tempfile.NamedTemporaryFile(
        suffix=".kubeconfig", delete=False
    ) as f:
        f.write(b"apiVersion: v1\n")
        kubeconfig_path = f.name

    try:
        result = get_k8s_api_client(kubeconfig_path=kubeconfig_path)

        mock_config.load_kube_config.assert_called_once_with(
            config_file=kubeconfig_path
        )
        mock_config.load_incluster_config.assert_not_called()
        assert result == mock_api_client
    finally:
        os.unlink(kubeconfig_path)


def test_nonexistent_kubeconfig_raises():
    with pytest.raises(
        FeastSparkClusterError, match="kubeconfig file not found"
    ):
        get_k8s_api_client(kubeconfig_path="/nonexistent/kubeconfig")


@patch("feast.infra.compute_engines.spark.byos.auth.config")
def test_in_cluster_auth_failure_wraps_error(mock_config):
    mock_config.load_incluster_config.side_effect = Exception(
        "Service account not found"
    )

    with pytest.raises(
        FeastSparkClusterError,
        match="Failed to authenticate with Kubernetes",
    ):
        get_k8s_api_client(kubeconfig_path=None)


@patch("feast.infra.compute_engines.spark.byos.auth.config")
def test_kubeconfig_load_failure_wraps_error(mock_config):
    mock_config.load_kube_config.side_effect = Exception(
        "Invalid kubeconfig"
    )

    with tempfile.NamedTemporaryFile(
        suffix=".kubeconfig", delete=False
    ) as f:
        f.write(b"apiVersion: v1\n")
        kubeconfig_path = f.name

    try:
        with pytest.raises(
            FeastSparkClusterError,
            match="Failed to authenticate with Kubernetes",
        ):
            get_k8s_api_client(kubeconfig_path=kubeconfig_path)
    finally:
        os.unlink(kubeconfig_path)


@patch("feast.infra.compute_engines.spark.byos.auth.config")
@patch("feast.infra.compute_engines.spark.byos.auth.client")
def test_kubeconfig_tilde_expansion(mock_client, mock_config):
    mock_api_client = MagicMock()
    mock_client.ApiClient.return_value = mock_api_client

    with tempfile.NamedTemporaryFile(
        dir=os.path.expanduser("~"),
        suffix=".kubeconfig",
        delete=False,
    ) as f:
        f.write(b"apiVersion: v1\n")
        full_path = f.name
        relative_path = "~/" + os.path.basename(f.name)

    try:
        result = get_k8s_api_client(kubeconfig_path=relative_path)
        mock_config.load_kube_config.assert_called_once_with(
            config_file=full_path
        )
        assert result == mock_api_client
    finally:
        os.unlink(full_path)
