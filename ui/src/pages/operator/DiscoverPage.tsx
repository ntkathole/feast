import React, { useState, useCallback } from "react";
import { useQuery } from "react-query";
import {
  EuiPageHeader,
  EuiBasicTable,
  EuiFieldSearch,
  EuiSpacer,
  EuiTabs,
  EuiTab,
  EuiFlexGroup,
  EuiFlexItem,
  EuiBadge,
  EuiText,
  EuiLoadingSpinner,
  EuiCallOut,
  EuiBasicTableColumn,
  EuiTableSortingType,
  CriteriaWithPagination,
  EuiFilterGroup,
  EuiFilterButton,
  EuiPopover,
  EuiSelectable,
  EuiSelectableOption,
} from "@elastic/eui";
import discoveryApiClient, {
  DiscoveryItem,
  DiscoveryResponse,
  ResourceType,
  SearchResponse,
} from "../../queries/discoveryApiClient";

type TabId = ResourceType;

interface TabConfig {
  id: TabId;
  label: string;
  nameField: string;
}

const TABS: TabConfig[] = [
  { id: "features", label: "Features", nameField: "name" },
  { id: "feature_views", label: "Feature Views", nameField: "name" },
  { id: "entities", label: "Entities", nameField: "name" },
  { id: "data_sources", label: "Data Sources", nameField: "name" },
  { id: "feature_services", label: "Feature Services", nameField: "name" },
];

const DiscoverPage: React.FC = () => {
  const [activeTab, setActiveTab] = useState<TabId>("features");
  const [searchQuery, setSearchQuery] = useState("");
  const [pageIndex, setPageIndex] = useState(0);
  const [pageSize, setPageSize] = useState(25);
  const [sortField, setSortField] = useState<string>("name");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [projectFilter, setProjectFilter] = useState<string[]>([]);
  const [namespaceFilter, setNamespaceFilter] = useState<string[]>([]);
  const [isProjectPopoverOpen, setProjectPopoverOpen] = useState(false);
  const [isNamespacePopoverOpen, setNamespacePopoverOpen] = useState(false);

  const isSearchMode = searchQuery.length >= 2;

  const {
    data: discoveryData,
    isLoading: isDiscoveryLoading,
    error: discoveryError,
  } = useQuery<DiscoveryResponse>(
    ["discover", activeTab, pageIndex, pageSize, sortField, sortDirection],
    () =>
      discoveryApiClient.discoverResources(activeTab, {
        page: pageIndex + 1,
        limit: pageSize,
        sort_by: sortField,
        sort_order: sortDirection,
      }),
    { enabled: !isSearchMode, keepPreviousData: true, refetchInterval: 30000 },
  );

  const {
    data: searchData,
    isLoading: isSearchLoading,
    error: searchError,
  } = useQuery<SearchResponse>(
    ["federated-search", searchQuery, pageIndex, pageSize],
    () =>
      discoveryApiClient.search(searchQuery, {
        page: pageIndex + 1,
        limit: pageSize,
      }),
    { enabled: isSearchMode, keepPreviousData: true },
  );

  const isLoading = isSearchMode ? isSearchLoading : isDiscoveryLoading;
  const error = isSearchMode ? searchError : discoveryError;

  const allProjects = discoveryData?.projects?.map((p) => p.project) || [];
  const allNamespaces = Array.from(
    new Set(discoveryData?.projects?.map((p) => p.namespace) || []),
  );

  const getFilteredItems = useCallback((): DiscoveryItem[] => {
    if (isSearchMode) {
      const results = searchData?.results || [];
      return results.map((r) => ({
        name: r.name,
        type: r.type,
        description: r.description,
        _source_project: r._source_project,
        _source_namespace: r._source_namespace,
        _source_featurestore: r._source_featurestore,
      })) as DiscoveryItem[];
    }

    let items = discoveryData?.items || [];
    if (projectFilter.length > 0) {
      items = items.filter((i) => projectFilter.includes(i._source_project));
    }
    if (namespaceFilter.length > 0) {
      items = items.filter((i) =>
        namespaceFilter.includes(i._source_namespace),
      );
    }
    return items;
  }, [
    isSearchMode,
    searchData,
    discoveryData,
    projectFilter,
    namespaceFilter,
  ]);

  const items = getFilteredItems();
  const totalItems = isSearchMode
    ? searchData?.total || 0
    : discoveryData?.total || 0;

  const onTableChange = ({ page, sort }: CriteriaWithPagination<DiscoveryItem>) => {
    if (page) {
      setPageIndex(page.index);
      setPageSize(page.size);
    }
    if (sort) {
      setSortField(sort.field as string);
      setSortDirection(sort.direction);
    }
  };

  const onSearchChange = (value: string) => {
    setSearchQuery(value);
    setPageIndex(0);
  };

  const getItemName = (item: DiscoveryItem): string => {
    if (item.name) return item.name;
    if (item.spec && typeof item.spec === "object") {
      const spec = item.spec as Record<string, unknown>;
      if (spec.name) return String(spec.name);
    }
    if (item.meta && typeof item.meta === "object") {
      const meta = item.meta as Record<string, unknown>;
      if (meta.name) return String(meta.name);
    }
    return "Unknown";
  };

  const getItemTags = (item: DiscoveryItem): Record<string, string> => {
    if (item.tags) return item.tags;
    if (item.spec && typeof item.spec === "object") {
      const spec = item.spec as Record<string, unknown>;
      if (spec.tags && typeof spec.tags === "object")
        return spec.tags as Record<string, string>;
    }
    return {};
  };

  const columns: EuiBasicTableColumn<DiscoveryItem>[] = [
    {
      field: "name",
      name: "Name",
      sortable: true,
      render: (_: unknown, item: DiscoveryItem) => (
        <strong>{getItemName(item)}</strong>
      ),
    },
    ...(isSearchMode
      ? [
          {
            field: "type",
            name: "Type",
            sortable: true,
            render: (type: string) => (
              <EuiBadge color="hollow">{type || "unknown"}</EuiBadge>
            ),
          } as EuiBasicTableColumn<DiscoveryItem>,
        ]
      : []),
    {
      field: "_source_project",
      name: "Project",
      sortable: true,
      render: (project: string) => <EuiBadge>{project}</EuiBadge>,
    },
    {
      field: "_source_namespace",
      name: "Namespace",
      sortable: true,
      render: (ns: string) => (
        <EuiText size="s" color="subdued">
          {ns}
        </EuiText>
      ),
    },
    {
      field: "tags",
      name: "Tags",
      render: (_: unknown, item: DiscoveryItem) => {
        const tags = getItemTags(item);
        const entries = Object.entries(tags).slice(0, 3);
        if (entries.length === 0) return <EuiText size="s">—</EuiText>;
        return (
          <EuiFlexGroup gutterSize="xs" wrap responsive={false}>
            {entries.map(([k, v]) => (
              <EuiFlexItem grow={false} key={k}>
                <EuiBadge color="hollow">
                  {k}={v}
                </EuiBadge>
              </EuiFlexItem>
            ))}
          </EuiFlexGroup>
        );
      },
    },
  ];

  const sorting: EuiTableSortingType<DiscoveryItem> = {
    sort: { field: sortField as keyof DiscoveryItem, direction: sortDirection },
  };

  const pagination = {
    pageIndex,
    pageSize,
    totalItemCount: totalItems,
    pageSizeOptions: [10, 25, 50, 100],
  };

  const projectOptions: EuiSelectableOption[] = allProjects.map((p) => ({
    label: p,
    checked: projectFilter.includes(p) ? "on" : undefined,
  }));

  const namespaceOptions: EuiSelectableOption[] = allNamespaces.map((ns) => ({
    label: ns,
    checked: namespaceFilter.includes(ns) ? "on" : undefined,
  }));

  return (
    <>
      <EuiPageHeader
        pageTitle="Discover Features"
        description="Search and browse features across all accessible projects"
      />
      <EuiSpacer size="m" />

      <EuiFlexGroup>
        <EuiFlexItem>
          <EuiFieldSearch
            placeholder="Search features, entities, data sources..."
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            isClearable
            fullWidth
          />
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {!isSearchMode && (
        <>
          <EuiTabs>
            {TABS.map((tab) => (
              <EuiTab
                key={tab.id}
                isSelected={activeTab === tab.id}
                onClick={() => {
                  setActiveTab(tab.id);
                  setPageIndex(0);
                }}
              >
                {tab.label}
              </EuiTab>
            ))}
          </EuiTabs>
          <EuiSpacer size="m" />
        </>
      )}

      <EuiFlexGroup gutterSize="s">
        <EuiFlexItem grow={false}>
          <EuiFilterGroup>
            <EuiPopover
              button={
                <EuiFilterButton
                  iconType="arrowDown"
                  onClick={() => setProjectPopoverOpen(!isProjectPopoverOpen)}
                  isSelected={isProjectPopoverOpen}
                  numFilters={allProjects.length}
                  hasActiveFilters={projectFilter.length > 0}
                  numActiveFilters={projectFilter.length}
                >
                  Project
                </EuiFilterButton>
              }
              isOpen={isProjectPopoverOpen}
              closePopover={() => setProjectPopoverOpen(false)}
              panelPaddingSize="none"
            >
              <EuiSelectable
                options={projectOptions}
                onChange={(opts) => {
                  setProjectFilter(
                    opts
                      .filter((o) => o.checked === "on")
                      .map((o) => o.label),
                  );
                }}
              >
                {(list) => <div style={{ width: 200 }}>{list}</div>}
              </EuiSelectable>
            </EuiPopover>

            <EuiPopover
              button={
                <EuiFilterButton
                  iconType="arrowDown"
                  onClick={() =>
                    setNamespacePopoverOpen(!isNamespacePopoverOpen)
                  }
                  isSelected={isNamespacePopoverOpen}
                  numFilters={allNamespaces.length}
                  hasActiveFilters={namespaceFilter.length > 0}
                  numActiveFilters={namespaceFilter.length}
                >
                  Namespace
                </EuiFilterButton>
              }
              isOpen={isNamespacePopoverOpen}
              closePopover={() => setNamespacePopoverOpen(false)}
              panelPaddingSize="none"
            >
              <EuiSelectable
                options={namespaceOptions}
                onChange={(opts) => {
                  setNamespaceFilter(
                    opts
                      .filter((o) => o.checked === "on")
                      .map((o) => o.label),
                  );
                }}
              >
                {(list) => <div style={{ width: 200 }}>{list}</div>}
              </EuiSelectable>
            </EuiPopover>
          </EuiFilterGroup>
        </EuiFlexItem>
        <EuiFlexItem grow={false}>
          <EuiText size="s" color="subdued">
            {totalItems} result{totalItems !== 1 ? "s" : ""} across{" "}
            {discoveryData?.projects?.length || 0} project
            {(discoveryData?.projects?.length || 0) !== 1 ? "s" : ""}
          </EuiText>
        </EuiFlexItem>
      </EuiFlexGroup>

      <EuiSpacer size="m" />

      {error && (
        <>
          <EuiCallOut
            title="Failed to load data"
            color="danger"
            iconType="alert"
          >
            <p>{String(error)}</p>
          </EuiCallOut>
          <EuiSpacer size="m" />
        </>
      )}

      {isLoading && !items.length ? (
        <EuiLoadingSpinner size="xl" />
      ) : (
        <EuiBasicTable
          items={items}
          columns={columns}
          sorting={sorting}
          pagination={pagination}
          onChange={onTableChange}
          loading={isLoading}
          noItemsMessage={
            isSearchMode
              ? "No results found for your search"
              : "No resources found. Create a FeatureStore to get started."
          }
        />
      )}
    </>
  );
};

export default DiscoverPage;
