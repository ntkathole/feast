/**
 * API client for federated discovery endpoints.
 * Queries all permitted registries and returns merged results.
 */

export interface DiscoveryItem {
  name?: string;
  spec?: Record<string, unknown>;
  meta?: Record<string, unknown>;
  tags?: Record<string, string>;
  description?: string;
  _source_project: string;
  _source_namespace: string;
  _source_featurestore: string;
  [key: string]: unknown;
}

export interface DiscoveryResponse {
  items: DiscoveryItem[];
  total: number;
  page: number;
  limit: number;
  projects: Array<{
    project: string;
    namespace: string;
    name: string;
  }>;
}

export interface SearchResult {
  type?: string;
  name?: string;
  description?: string;
  project?: string;
  featureView?: string;
  _source_project: string;
  _source_namespace: string;
  _source_featurestore: string;
}

export interface SearchResponse {
  query: string;
  results: SearchResult[];
  total: number;
  page: number;
  limit: number;
  projects_searched: string[];
}

export type ResourceType =
  | "entities"
  | "feature_views"
  | "features"
  | "data_sources"
  | "feature_services"
  | "saved_datasets";

class DiscoveryApiClient {
  private baseUrl = "/api/operator";

  async discoverResources(
    resourceType: ResourceType,
    options?: {
      tags?: string;
      sort_by?: string;
      sort_order?: "asc" | "desc";
      page?: number;
      limit?: number;
    },
  ): Promise<DiscoveryResponse> {
    const params = new URLSearchParams();
    if (options?.tags) params.set("tags", options.tags);
    if (options?.sort_by) params.set("sort_by", options.sort_by);
    if (options?.sort_order) params.set("sort_order", options.sort_order);
    if (options?.page) params.set("page", String(options.page));
    if (options?.limit) params.set("limit", String(options.limit));

    const queryStr = params.toString();
    const url = `${this.baseUrl}/discover/${resourceType}${queryStr ? `?${queryStr}` : ""}`;
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`Discovery failed: ${res.statusText}`);
    }
    return res.json();
  }

  async search(
    query: string,
    options?: { tags?: string; page?: number; limit?: number },
  ): Promise<SearchResponse> {
    const params = new URLSearchParams({ query });
    if (options?.tags) params.set("tags", options.tags);
    if (options?.page) params.set("page", String(options.page));
    if (options?.limit) params.set("limit", String(options.limit));

    const res = await fetch(`${this.baseUrl}/search?${params.toString()}`);
    if (!res.ok) {
      throw new Error(`Search failed: ${res.statusText}`);
    }
    return res.json();
  }
}

const discoveryApiClient = new DiscoveryApiClient();
export default discoveryApiClient;
