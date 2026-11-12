from fastapi.requests import Request
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_extractor import TokenExtractor


class RestTokenExtractor(TokenExtractor):
    def extract_access_token(self, **kwargs) -> str:
        """
        Token extractor for REST requests.

        Requires a keyword argument called `request` of type `Request`

        Returns:
            The extracted access token.
        """

        if "request" not in kwargs:
            raise ValueError("Missing keywork argument 'request'")
        if not isinstance(kwargs["request"], Request):
            raise ValueError(
                f"The keywork argument 'request' is not of the expected type {Request.__name__}"
            )

        request = kwargs["request"]
        headers = request.headers

        # Prefer the standard Authorization: Bearer header used by CLI/SDK
        # clients. Starlette headers lookups are case-insensitive.
        authorization = headers.get("authorization")
        if authorization:
            return self._extract_bearer_token(authorization)

        # Fall back to the token forwarded by an authenticating reverse proxy
        # (e.g. OpenShift oauth-proxy started with --pass-access-token), which
        # places the raw user access token in the X-Forwarded-Access-Token
        # header rather than the Authorization header. This lets a browser UI
        # sitting behind such a proxy authenticate to the Feast REST API.
        forwarded_token = headers.get("x-forwarded-access-token")
        if forwarded_token:
            return forwarded_token.strip()

        raise AuthenticationError(
            "Missing authorization header or X-Forwarded-Access-Token header"
        )
