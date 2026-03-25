from pydantic import BaseModel, StrictStr


class SecretRef(BaseModel):
    """Reference to a Kubernetes secret to mount on Spark driver/executor pods."""

    name: StrictStr
    mount_path: StrictStr


class ConfigMapRef(BaseModel):
    """Reference to a Kubernetes ConfigMap to mount on Spark driver/executor pods."""

    name: StrictStr
    mount_path: StrictStr
