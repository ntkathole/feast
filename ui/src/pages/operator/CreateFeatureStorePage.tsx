import React, { useContext, useState } from "react";
import { useQuery, useMutation } from "react-query";
import { useNavigate } from "react-router-dom";
import {
  EuiPageHeader,
  EuiSpacer,
  EuiSteps,
  EuiButton,
  EuiButtonEmpty,
  EuiFlexGroup,
  EuiFlexItem,
  EuiForm,
  EuiFormRow,
  EuiFieldText,
  EuiSelect,
  EuiSwitch,
  EuiCallOut,
  EuiPanel,
  EuiText,
  EuiAccordion,
  EuiFieldNumber,
  EuiEmptyPrompt,
  EuiCodeBlock,
  EuiLoadingSpinner,
} from "@elastic/eui";

import OperatorContext from "../../contexts/OperatorContext";
import operatorApiClient from "../../queries/operatorApiClient";

interface FormState {
  name: string;
  namespace: string;
  feastProject: string;
  registryType: "local" | "remote";
  registryDbPath: string;
  registrySecretName: string;
  remoteHostname: string;
  remoteTlsEnabled: boolean;
  remoteTlsConfigMap: string;
  remoteTlsCertKey: string;
  onlineStoreEnabled: boolean;
  onlineStoreType: string;
  onlineStoreDbPath: string;
  offlineStoreEnabled: boolean;
  offlineStoreType: string;
  restApiEnabled: boolean;
  grpcEnabled: boolean;
}

const initialFormState: FormState = {
  name: "myfeaturestore",
  namespace: "",
  feastProject: "myproject",
  registryType: "local",
  registryDbPath: "/data/registry/registry.db",
  registrySecretName: "",
  remoteHostname: "",
  remoteTlsEnabled: false,
  remoteTlsConfigMap: "",
  remoteTlsCertKey: "service-ca.crt",
  onlineStoreEnabled: true,
  onlineStoreType: "sqlite",
  onlineStoreDbPath: "/data/online/online.db",
  offlineStoreEnabled: false,
  offlineStoreType: "dask",
  restApiEnabled: true,
  grpcEnabled: true,
};

const CreateFeatureStorePage: React.FC = () => {
  const operatorState = useContext(OperatorContext);
  const navigate = useNavigate();
  const [currentStep, setCurrentStep] = useState(0);
  const [form, setForm] = useState<FormState>(initialFormState);
  const [submitError, setSubmitError] = useState<string>("");

  const { data: namespaces, isLoading: nsLoading } = useQuery(
    "operator-namespaces",
    () => operatorApiClient.listNamespaces(),
    { enabled: operatorState.canCreate },
  );

  const { data: secrets } = useQuery(
    ["operator-secrets", form.namespace],
    () => operatorApiClient.listSecrets(form.namespace),
    { enabled: !!form.namespace },
  );

  const { data: configMaps } = useQuery(
    ["operator-configmaps", form.namespace],
    () => operatorApiClient.listConfigMaps(form.namespace),
    { enabled: !!form.namespace },
  );

  const createMutation = useMutation(
    (body: object) => operatorApiClient.createFeatureStore(body),
    {
      onSuccess: () => navigate("/manage"),
      onError: (err: Error) => setSubmitError(err.message),
    },
  );

  if (!operatorState.canCreate) {
    return (
      <EuiEmptyPrompt
        iconType="lock"
        title={<h2>Access Denied</h2>}
        body={
          <p>You do not have permission to create feature stores.</p>
        }
      />
    );
  }

  const updateForm = (partial: Partial<FormState>) => {
    setForm((prev) => ({ ...prev, ...partial }));
  };

  const buildSpec = (): object => {
    const spec: Record<string, any> = {
      feastProject: form.feastProject,
    };

    const services: Record<string, any> = {};

    // Registry
    if (form.registryType === "local") {
      const server: Record<string, any> = {
        restAPI: form.restApiEnabled,
        grpc: form.grpcEnabled,
      };
      if (form.registrySecretName) {
        server.envFrom = [
          { secretRef: { name: form.registrySecretName } },
        ];
      }
      services.registry = {
        local: {
          persistence: {
            file: { path: form.registryDbPath },
          },
          server,
        },
      };
    } else {
      services.registry = {
        remote: {
          hostname: form.remoteHostname,
        },
      };
      if (form.remoteTlsEnabled) {
        services.registry.remote.tls = {
          configMapRef: { name: form.remoteTlsConfigMap },
          certName: form.remoteTlsCertKey,
        };
      }
    }

    // Online store
    if (form.onlineStoreEnabled) {
      services.onlineStore = {
        persistence: {
          file: { path: form.onlineStoreDbPath },
        },
      };
    }

    // Offline store
    if (form.offlineStoreEnabled) {
      services.offlineStore = {
        persistence: {
          store: { type: form.offlineStoreType },
        },
      };
    }

    spec.services = services;

    return {
      apiVersion: "feast.dev/v1",
      kind: "FeatureStore",
      metadata: {
        name: form.name,
        namespace: form.namespace,
      },
      spec,
    };
  };

  const handleSubmit = () => {
    setSubmitError("");
    createMutation.mutate(buildSpec());
  };

  const namespaceOptions = (namespaces || []).map((ns) => ({
    value: ns,
    text: ns,
  }));

  const secretOptions = [
    { value: "", text: "— None —" },
    ...(secrets || []).map((s) => ({ value: s, text: s })),
  ];

  const configMapOptions = [
    { value: "", text: "— Select —" },
    ...(configMaps || []).map((cm) => ({ value: cm, text: cm })),
  ];

  const stepContent = [
    // Step 0: Basics
    <EuiPanel key="basics">
      <EuiForm>
        <EuiFormRow label="Feature Store Name" helpText="Kubernetes resource name (lowercase, no underscores)">
          <EuiFieldText
            value={form.name}
            onChange={(e) => updateForm({ name: e.target.value })}
          />
        </EuiFormRow>
        <EuiFormRow label="Namespace">
          {nsLoading ? (
            <EuiLoadingSpinner />
          ) : (
            <EuiSelect
              options={[{ value: "", text: "— Select namespace —" }, ...namespaceOptions]}
              value={form.namespace}
              onChange={(e) => updateForm({ namespace: e.target.value })}
            />
          )}
        </EuiFormRow>
        <EuiFormRow label="Feast Project Name" helpText="Logical project name within Feast">
          <EuiFieldText
            value={form.feastProject}
            onChange={(e) => updateForm({ feastProject: e.target.value })}
          />
        </EuiFormRow>
      </EuiForm>
    </EuiPanel>,

    // Step 1: Registry
    <EuiPanel key="registry">
      <EuiForm>
        <EuiFormRow label="Registry Type">
          <EuiSelect
            options={[
              { value: "local", text: "Local (file-based)" },
              { value: "remote", text: "Remote (hostname)" },
            ]}
            value={form.registryType}
            onChange={(e) =>
              updateForm({ registryType: e.target.value as "local" | "remote" })
            }
          />
        </EuiFormRow>

        {form.registryType === "local" && (
          <>
            <EuiFormRow label="Registry DB Path">
              <EuiFieldText
                value={form.registryDbPath}
                onChange={(e) => updateForm({ registryDbPath: e.target.value })}
              />
            </EuiFormRow>
            <EuiFormRow label="Credentials Secret" helpText="Optional: secret for S3/GCS registry">
              <EuiSelect
                options={secretOptions}
                value={form.registrySecretName}
                onChange={(e) => updateForm({ registrySecretName: e.target.value })}
              />
            </EuiFormRow>
            <EuiFormRow>
              <EuiSwitch
                label="REST API enabled (recommended)"
                checked={form.restApiEnabled}
                onChange={(e) => updateForm({ restApiEnabled: e.target.checked })}
              />
            </EuiFormRow>
            <EuiFormRow>
              <EuiSwitch
                label="gRPC enabled (recommended)"
                checked={form.grpcEnabled}
                onChange={(e) => updateForm({ grpcEnabled: e.target.checked })}
              />
            </EuiFormRow>
          </>
        )}

        {form.registryType === "remote" && (
          <>
            <EuiFormRow label="Registry Hostname" helpText="hostname:port of the remote registry">
              <EuiFieldText
                value={form.remoteHostname}
                onChange={(e) => updateForm({ remoteHostname: e.target.value })}
              />
            </EuiFormRow>
            <EuiFormRow>
              <EuiSwitch
                label="Enable TLS"
                checked={form.remoteTlsEnabled}
                onChange={(e) => updateForm({ remoteTlsEnabled: e.target.checked })}
              />
            </EuiFormRow>
            {form.remoteTlsEnabled && (
              <>
                <EuiFormRow label="TLS CA ConfigMap">
                  <EuiSelect
                    options={configMapOptions}
                    value={form.remoteTlsConfigMap}
                    onChange={(e) => updateForm({ remoteTlsConfigMap: e.target.value })}
                  />
                </EuiFormRow>
                <EuiFormRow label="Certificate Key Name">
                  <EuiFieldText
                    value={form.remoteTlsCertKey}
                    onChange={(e) => updateForm({ remoteTlsCertKey: e.target.value })}
                  />
                </EuiFormRow>
              </>
            )}
          </>
        )}
      </EuiForm>
    </EuiPanel>,

    // Step 2: Store Config
    <EuiPanel key="stores">
      <EuiForm>
        <EuiText>
          <h4>Online Store</h4>
        </EuiText>
        <EuiSpacer size="s" />
        <EuiFormRow>
          <EuiSwitch
            label="Enable online store"
            checked={form.onlineStoreEnabled}
            onChange={(e) => updateForm({ onlineStoreEnabled: e.target.checked })}
          />
        </EuiFormRow>
        {form.onlineStoreEnabled && (
          <>
            <EuiFormRow label="Store Type">
              <EuiSelect
                options={[
                  { value: "sqlite", text: "SQLite" },
                  { value: "snowflake.online", text: "Snowflake" },
                  { value: "redis", text: "Redis" },
                  { value: "postgres", text: "PostgreSQL" },
                ]}
                value={form.onlineStoreType}
                onChange={(e) => updateForm({ onlineStoreType: e.target.value })}
              />
            </EuiFormRow>
            <EuiFormRow label="DB Path">
              <EuiFieldText
                value={form.onlineStoreDbPath}
                onChange={(e) => updateForm({ onlineStoreDbPath: e.target.value })}
              />
            </EuiFormRow>
          </>
        )}

        <EuiSpacer size="l" />
        <EuiText>
          <h4>Offline Store</h4>
        </EuiText>
        <EuiSpacer size="s" />
        <EuiFormRow>
          <EuiSwitch
            label="Enable offline store server"
            checked={form.offlineStoreEnabled}
            onChange={(e) => updateForm({ offlineStoreEnabled: e.target.checked })}
          />
        </EuiFormRow>
        {form.offlineStoreEnabled && (
          <EuiFormRow label="Offline Store Type">
            <EuiSelect
              options={[
                { value: "dask", text: "Dask" },
                { value: "snowflake.offline", text: "Snowflake" },
                { value: "spark", text: "Spark" },
              ]}
              value={form.offlineStoreType}
              onChange={(e) => updateForm({ offlineStoreType: e.target.value })}
            />
          </EuiFormRow>
        )}
      </EuiForm>
    </EuiPanel>,

    // Step 3: Review
    <EuiPanel key="review">
      <EuiText>
        <h4>Review and Create</h4>
      </EuiText>
      <EuiSpacer size="m" />
      <EuiCodeBlock language="json" isCopyable>
        {JSON.stringify(buildSpec(), null, 2)}
      </EuiCodeBlock>
      {submitError && (
        <>
          <EuiSpacer />
          <EuiCallOut
            title="Failed to create feature store"
            color="danger"
            iconType="alert"
          >
            <p>{submitError}</p>
          </EuiCallOut>
        </>
      )}
    </EuiPanel>,
  ];

  const stepTitles = [
    "Feature Store Details",
    "Registry Configuration",
    "Store Configuration",
    "Review & Create",
  ];

  const isBasicsValid = form.name && form.namespace && form.feastProject;
  const isRegistryValid =
    form.registryType === "local"
      ? !!form.registryDbPath
      : !!form.remoteHostname;

  const canProceed = () => {
    switch (currentStep) {
      case 0:
        return isBasicsValid;
      case 1:
        return isRegistryValid;
      case 2:
        return true;
      default:
        return true;
    }
  };

  return (
    <div style={{ padding: "16px", maxWidth: "800px" }}>
      <EuiPageHeader
        pageTitle="Create Feature Store"
        description="Configure and deploy a new feature store instance."
      />
      <EuiSpacer size="l" />

      <EuiSteps
        steps={stepTitles.map((title, idx) => ({
          title,
          status:
            idx < currentStep
              ? "complete"
              : idx === currentStep
                ? "current"
                : "incomplete",
          children: idx === currentStep ? stepContent[idx] : null,
        }))}
      />

      <EuiSpacer size="l" />
      <EuiFlexGroup justifyContent="spaceBetween">
        <EuiFlexItem grow={false}>
          {currentStep > 0 && (
            <EuiButtonEmpty onClick={() => setCurrentStep((s) => s - 1)}>
              Back
            </EuiButtonEmpty>
          )}
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          {currentStep < stepTitles.length - 1 ? (
            <EuiButton
              fill
              onClick={() => setCurrentStep((s) => s + 1)}
              disabled={!canProceed()}
            >
              Next
            </EuiButton>
          ) : (
            <EuiButton
              fill
              color="success"
              onClick={handleSubmit}
              isLoading={createMutation.isLoading}
            >
              Create Feature Store
            </EuiButton>
          )}
        </EuiFlexItem>
      </EuiFlexGroup>
    </div>
  );
};

export default CreateFeatureStorePage;
