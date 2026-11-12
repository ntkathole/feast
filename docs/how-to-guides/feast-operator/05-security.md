# Guide 5 — Security

The operator supports two authorization models via `spec.authz`, plus TLS for all servers.
Authorization is optional — omitting `authz` deploys Feast with no access control.

---

## Kubernetes RBAC authorization

Kubernetes RBAC authorization uses ServiceAccount tokens. The operator creates
`ClusterRole`s for each named role you declare and binds them to ServiceAccounts. Feast
servers enforce these roles on every API call.

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-rbac
spec:
  feastProject: feast_rbac
  authz:
    kubernetes:
      roles:
        - feast-writer     # created as a ClusterRole
        - feast-reader
  services:
    offlineStore:
      server: {}
    onlineStore:
      server: {}
    registry:
      local:
        server: {}
```

The operator creates `ClusterRole` resources named after each entry in `roles`. Bind them
to subjects using standard Kubernetes `ClusterRoleBinding` or `RoleBinding` resources.

> Kubernetes auth requires all services to be exposed as servers (the controller rejects
> partial configurations where some services are local while RBAC is enabled).

**SDK docs**: [Feast RBAC](../../getting-started/architecture/rbac.md)

### Feast UI with Kubernetes auth on OpenShift

Feast servers authenticate every API call with a bearer token (the caller's
ServiceAccount or user token, validated via `TokenReview`). A browser, however,
has no way to obtain such a token on its own. On **OpenShift**, where an OAuth
server is built in, the operator closes this gap automatically: whenever a **UI
server is deployed with `kubernetes` authorization**, it fronts the UI with an
[OpenShift `oauth-proxy`](https://github.com/openshift/oauth-proxy) sidecar.

The proxy performs the OpenShift login, keeps the session in an encrypted
server-side cookie (no token is exposed to the browser), and forwards the signed-in
user's token to the Feast REST API — which then applies the same Kubernetes RBAC
roles and `Permission` objects as any other client. There is nothing to enable:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-ui
spec:
  feastProject: feast_rbac
  authz:
    kubernetes: {}      # default; the proxy is wired in automatically on OpenShift
  services:
    registry:
      local:
        server: {}
    onlineStore:
      server: {}
    offlineStore:
      server: {}
    ui: {}              # exposed via a reencrypt Route in front of the proxy
```

Behind the scenes the operator, when running on OpenShift with `kubernetes` authz
and a UI server:

- adds the `oauth-proxy` sidecar (TLS-terminated on port 8443 with an OpenShift
  service serving certificate) and points the UI `Service`/`Route` at it;
- runs the UI container on plain HTTP on the loopback interface, reachable only
  through the proxy;
- generates a cookie-signing `Secret` and binds the Feast `ServiceAccount` to
  `system:auth-delegator` for the proxy's token/subject access reviews.

Optional overrides (image and resources) are available under `services.oauthProxy`;
the image also resolves from the operator's `RELATED_IMAGE_OAUTH_PROXY` environment
variable. This is OpenShift-only today — on vanilla Kubernetes you would instead
front the UI with your own login proxy (for example `oauth2-proxy` with an OIDC
provider) or use [OIDC authorization](#oidc-authorization).

> With `oidc` authorization the UI logs in directly through Keycloak and no proxy
> is added; this sidecar applies only to `kubernetes` authorization.

---

## OIDC authorization

OIDC authorization validates Bearer tokens against an OIDC provider (Keycloak, Dex, etc.).

### Secret format

Create a Secret with the OIDC client credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: oidc-secret
stringData:
  client_id: <your-client-id>
  auth_discovery_url: https://keycloak.example.com/realms/feast/.well-known/openid-configuration
  client_secret: <your-client-secret>
  username: <service-account-username>     # used for client-credentials flow
  password: <service-account-password>
  audience: <expected-aud-claim>           # optional: reject tokens whose aud claim differs
  issuer: <expected-iss-claim>             # optional: reject tokens whose iss claim differs
```

The optional `audience` and `issuer` keys enable audience and issuer claim verification on the standard OIDC/JWKS validation path; when omitted, the `aud` and `iss` claims are not checked. Set them to the values your IdP puts in the token itself, which are not always the ones in the discovery document (see [OIDC Authorization](../../getting-started/components/authz_manager.md#oidc-authorization)). The Secret key `issuer` is distinct from the CR's `issuerUrl`, which selects the discovery endpoint and plays no part in claim verification. Kubernetes ServiceAccount tokens (validated via TokenReview) and intra-server communication follow separate paths and are not subject to these checks.

{% hint style="warning" %}
Before enabling these, three operational caveats:

* **Existing Secret keys take effect on operator upgrade.** Keys named `audience` or `issuer` already present in the referenced Secret were previously ignored; after upgrading they are forwarded to every Feast pod.
* **Your IdP must mint matching tokens for Feast's own clients.** Feast's client-credentials flow requests no audience, so in multi-service topologies (e.g. a remote registry) and for the UI's browser tokens, the IdP must be configured to issue tokens carrying the expected claims (e.g. a Keycloak audience mapper), or inter-service calls will be rejected.
* **Secret edits are not watched.** Changes to these keys apply on the next reconcile or pod restart, not immediately.
{% endhint %}

Reference the Secret from the CR:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-oidc
spec:
  feastProject: my_project
  authz:
    oidc:
      secretRef:
        name: oidc-secret
```

### Advanced OIDC options

{% hint style="warning" %}
Every option in this section requires `apiVersion: feast.dev/v1`. Under the deprecated
`feast.dev/v1alpha1`, `authz.oidc` accepts only `secretRef`. The CRD has no conversion
webhook, so a resource submitted as v1alpha1 is validated against the v1alpha1 schema and
any other field is pruned without error rather than rejected. Applying the example below
as v1alpha1 therefore leaves OIDC configured by Secret alone, with none of these settings
taking effect and nothing in the output to say so. Use v1, which is the storage version.
{% endhint %}

```yaml
authz:
  oidc:
    secretRef:
      name: oidc-secret
    secretKeyName: client_id          # override the default Secret key name
    tokenEnvVar: FEAST_TOKEN          # env var from which servers read the Bearer token
    verifySSL: false                  # disable SSL verification (dev only)
    caCertConfigMap:                  # ConfigMap with CA cert for SSL verification
      name: oidc-ca-cert
    jwksCacheLifespanSeconds: 300     # how long servers reuse the fetched JWK set
    jwksRequestTimeoutSeconds: 10     # network timeout for the JWKS fetch
```

`jwksCacheLifespanSeconds` is not only a performance setting: it also bounds how long a key the provider has **revoked** continues to validate tokens. Lower it if your provider rotates or revokes aggressively, at the cost of proportionally more JWKS fetches. Key rotations that introduce a new key id are picked up immediately regardless, because an unknown key id forces a refetch. `jwksRequestTimeoutSeconds` bounds how long an unresponsive provider can block request serving. Both must be at least 1. When unset, neither key is written to the generated configuration and the feature server applies its own defaults (300 and 10 seconds respectively).

{% hint style="warning" %}
These two options require a feature server image that recognizes them. The operator deploys a matching image by default, so this only applies if you pin an older one explicitly, through a container `image` override or the operator's `RELATED_IMAGE_FEATURE_SERVER` setting. An image that predates these options rejects its configuration at startup, so leave them unset until the pinned image is updated.
{% endhint %}

**SDK docs**: [Feast OIDC Auth](../../getting-started/components/authz_manager.md#oidc-authorization)

---

## TLS for servers

Each server accepts a `tls` block pointing to a Kubernetes Secret that holds the TLS
certificate and key.

### Creating a TLS Secret

```sh
kubectl create secret tls feast-tls \
  --cert=path/to/tls.crt \
  --key=path/to/tls.key \
  -n <namespace>
```

### Applying TLS to servers

```yaml
services:
  onlineStore:
    server:
      tls:
        secretRef:
          name: feast-tls
  offlineStore:
    server:
      tls:
        secretRef:
          name: feast-tls
  registry:
    local:
      server:
        tls:
          secretRef:
            name: feast-tls
```

Each service can use different TLS Secrets.

### Custom certificate key names

By default the operator looks for keys `tls.crt` and `tls.key`. Override with:

```yaml
tls:
  secretRef:
    name: feast-tls
  certKeyName: server.crt     # default: tls.crt
```

### mTLS — providing a CA certificate

For mutual TLS (client certificate verification), supply a CA cert via a ConfigMap:

```yaml
tls:
  secretRef:
    name: feast-tls
  caCertConfigMapRef:
    name: client-ca-cert
  certKeyName: tls.crt
```

---

## OpenShift non-TLS mode

On OpenShift, services are typically accessed via Routes with TLS termination at the edge.
In this case it is common to run the Feast servers without internal TLS:

```yaml
# See config/samples/v1_featurestore_all_openshift_non_tls.yaml
services:
  onlineStore:
    server: {}    # no tls block
  offlineStore:
    server: {}
  registry:
    local:
      server: {}
```

---

## See also

- [API reference — `AuthzConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#authzconfig)
- [API reference — `TlsConfigs`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#tlsconfigs)
- [Sample: Kubernetes auth](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_kubernetes_auth.yaml)
- [Sample: OIDC auth](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_oidc_auth.yaml)
- [Sample: Postgres with TLS volumes](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_postgres_db_volumes_tls.yaml)
- [Sample: OpenShift non-TLS](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_all_openshift_non_tls.yaml)
- [Feast SDK — Auth Overview](../../reference/auth/)
