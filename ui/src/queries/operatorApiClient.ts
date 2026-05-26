/**
 * API client for Feast operator management endpoints.
 * These are only available when the backend runs in operator mode (FEAST_OPERATOR_MANAGED=true).
 */

export interface FeatureStoreResource {
  apiVersion: string;
  kind: string;
  metadata: {
    name: string;
    namespace: string;
    creationTimestamp?: string;
    labels?: Record<string, string>;
    uid?: string;
  };
  spec: Record<string, unknown>;
  status?: {
    phase?: string;
    conditions?: Array<{
      type: string;
      status: string;
      message?: string;
      lastTransitionTime?: string;
    }>;
  };
}

interface ListResponse {
  items: FeatureStoreResource[];
}

interface NamespaceItem {
  name: string;
}

interface NamespaceListResponse {
  items: NamespaceItem[];
}

interface SecretItem {
  name: string;
}

interface SecretListResponse {
  items: SecretItem[];
}

interface ConfigMapItem {
  name: string;
}

interface ConfigMapListResponse {
  items: ConfigMapItem[];
}

interface PodContainer {
  name: string;
  ready: boolean;
  state: string;
}

interface PodSummary {
  name: string;
  namespace: string;
  phase: string;
  creationTimestamp?: string;
  containers: PodContainer[];
  initContainers: PodContainer[];
}

interface PodListResponse {
  items: PodSummary[];
}

class OperatorApiClient {
  private baseUrl = "/api/operator";

  async listFeatureStores(
    namespace?: string,
  ): Promise<FeatureStoreResource[]> {
    const url = namespace
      ? `${this.baseUrl}/featurestores?namespace=${encodeURIComponent(namespace)}`
      : `${this.baseUrl}/featurestores`;
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`Failed to list feature stores: ${res.statusText}`);
    }
    const data: ListResponse = await res.json();
    return data.items || [];
  }

  async getFeatureStore(
    namespace: string,
    name: string,
  ): Promise<FeatureStoreResource> {
    const res = await fetch(
      `${this.baseUrl}/featurestores/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`,
    );
    if (!res.ok) {
      throw new Error(`Failed to get feature store: ${res.statusText}`);
    }
    return res.json();
  }

  async createFeatureStore(body: object): Promise<FeatureStoreResource> {
    const res = await fetch(`${this.baseUrl}/featurestores`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const detail = await res.text();
      throw new Error(`Failed to create feature store: ${detail}`);
    }
    return res.json();
  }

  async deleteFeatureStore(namespace: string, name: string): Promise<void> {
    const res = await fetch(
      `${this.baseUrl}/featurestores/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`,
      { method: "DELETE" },
    );
    if (!res.ok) {
      throw new Error(`Failed to delete feature store: ${res.statusText}`);
    }
  }

  async getFeatureStorePods(
    namespace: string,
    name: string,
  ): Promise<PodSummary[]> {
    const res = await fetch(
      `${this.baseUrl}/featurestores/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/pods`,
    );
    if (!res.ok) {
      throw new Error(`Failed to get pods: ${res.statusText}`);
    }
    const data: PodListResponse = await res.json();
    return data.items || [];
  }

  async listNamespaces(): Promise<string[]> {
    const res = await fetch(`${this.baseUrl}/namespaces`);
    if (!res.ok) {
      throw new Error(`Failed to list namespaces: ${res.statusText}`);
    }
    const data: NamespaceListResponse = await res.json();
    return data.items.map((ns) => ns.name);
  }

  async listSecrets(namespace: string): Promise<string[]> {
    const res = await fetch(
      `${this.baseUrl}/secrets/${encodeURIComponent(namespace)}`,
    );
    if (!res.ok) {
      throw new Error(`Failed to list secrets: ${res.statusText}`);
    }
    const data: SecretListResponse = await res.json();
    return data.items.map((s) => s.name);
  }

  async listConfigMaps(namespace: string): Promise<string[]> {
    const res = await fetch(
      `${this.baseUrl}/configmaps/${encodeURIComponent(namespace)}`,
    );
    if (!res.ok) {
      throw new Error(`Failed to list configmaps: ${res.statusText}`);
    }
    const data: ConfigMapListResponse = await res.json();
    return data.items.map((cm) => cm.name);
  }
}

const operatorApiClient = new OperatorApiClient();

export default operatorApiClient;
export type { PodSummary, PodContainer };
