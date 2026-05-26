import React, { useContext, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "react-query";
import { Link } from "react-router-dom";
import {
  EuiPageHeader,
  EuiBasicTable,
  EuiButton,
  EuiHealth,
  EuiSpacer,
  EuiConfirmModal,
  EuiText,
  EuiCallOut,
  EuiFlexGroup,
  EuiFlexItem,
  EuiDescriptionList,
  EuiLoadingSpinner,
  EuiEmptyPrompt,
  EuiBasicTableColumn,
} from "@elastic/eui";

import OperatorContext from "../../contexts/OperatorContext";
import operatorApiClient, {
  FeatureStoreResource,
} from "../../queries/operatorApiClient";

const statusToColor = (phase?: string) => {
  switch (phase?.toLowerCase()) {
    case "ready":
      return "success";
    case "failed":
      return "danger";
    case "pending":
    case "progressing":
      return "warning";
    default:
      return "subdued";
  }
};

const ManageFeatureStoresPage: React.FC = () => {
  const operatorState = useContext(OperatorContext);
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<FeatureStoreResource | null>(
    null,
  );
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});

  const {
    data: featureStores,
    isLoading,
    error,
  } = useQuery<FeatureStoreResource[]>(
    "operator-featurestores",
    () => operatorApiClient.listFeatureStores(),
    { refetchInterval: 10000 },
  );

  const deleteMutation = useMutation(
    (fs: FeatureStoreResource) =>
      operatorApiClient.deleteFeatureStore(
        fs.metadata.namespace,
        fs.metadata.name,
      ),
    {
      onSuccess: () => {
        queryClient.invalidateQueries("operator-featurestores");
        setDeleteTarget(null);
      },
    },
  );

  if (!operatorState.canList) {
    return (
      <EuiEmptyPrompt
        iconType="lock"
        title={<h2>Access Denied</h2>}
        body={
          <p>You do not have permission to manage feature stores on this cluster.</p>
        }
      />
    );
  }

  const toggleRowDetails = (uid: string) => {
    setExpandedRows((prev) => ({ ...prev, [uid]: !prev[uid] }));
  };

  const columns: Array<EuiBasicTableColumn<FeatureStoreResource>> = [
    {
      field: "metadata.name",
      name: "Name",
      sortable: true,
    },
    {
      field: "metadata.namespace",
      name: "Namespace",
      sortable: true,
    },
    {
      field: "status.phase",
      name: "Status",
      render: (phase: string) => (
        <EuiHealth color={statusToColor(phase)}>
          {phase || "Unknown"}
        </EuiHealth>
      ),
    },
    {
      field: "metadata.creationTimestamp",
      name: "Created",
      render: (ts: string) =>
        ts ? new Date(ts).toLocaleString() : "—",
    },
    ...(operatorState.canDelete
      ? [
          {
            name: "Actions",
            actions: [
              {
                name: "Delete",
                description: "Delete this feature store",
                icon: "trash",
                color: "danger" as const,
                type: "icon" as const,
                onClick: (item: FeatureStoreResource) => setDeleteTarget(item),
              },
            ],
          } as EuiBasicTableColumn<FeatureStoreResource>,
        ]
      : []),
  ];

  const itemIdToExpandedRowMap: Record<string, React.ReactNode> = {};
  (featureStores || []).forEach((fs) => {
    const uid =
      fs.metadata.uid || `${fs.metadata.namespace}/${fs.metadata.name}`;
    if (expandedRows[uid]) {
      const spec = fs.spec || {};
      const conditions = fs.status?.conditions || [];

      itemIdToExpandedRowMap[uid] = (
        <div style={{ padding: "12px 24px" }}>
          <EuiFlexGroup direction="column" gutterSize="m">
            <EuiFlexItem>
              <EuiText size="s">
                <strong>Project:</strong> {(spec as any).feastProject || "—"}
              </EuiText>
            </EuiFlexItem>

            {conditions.length > 0 && (
              <EuiFlexItem>
                <EuiText size="s"><strong>Component Status:</strong></EuiText>
                <EuiSpacer size="xs" />
                <EuiBasicTable
                  items={conditions}
                  columns={[
                    {
                      field: "type",
                      name: "Component",
                      width: "200px",
                    },
                    {
                      field: "status",
                      name: "Status",
                      width: "100px",
                      render: (val: string) => (
                        <EuiHealth color={val === "True" ? "success" : val === "False" ? "danger" : "warning"}>
                          {val}
                        </EuiHealth>
                      ),
                    },
                    {
                      field: "message",
                      name: "Message",
                      render: (val: string) => val || "—",
                    },
                    {
                      field: "lastTransitionTime",
                      name: "Last Transition",
                      width: "180px",
                      render: (val: string) => val ? new Date(val).toLocaleString() : "—",
                    },
                  ] as Array<EuiBasicTableColumn<any>>}
                  tableLayout="auto"
                  compressed
                />
              </EuiFlexItem>
            )}

            {(spec as any).services && (
              <EuiFlexItem>
                <EuiText size="s"><strong>Services Configuration:</strong></EuiText>
                <EuiSpacer size="xs" />
                <EuiDescriptionList
                  listItems={Object.entries((spec as any).services || {}).map(([key, value]) => ({
                    title: key.charAt(0).toUpperCase() + key.slice(1),
                    description: typeof value === "object"
                      ? Object.keys(value as object).join(", ")
                      : String(value),
                  }))}
                  type="column"
                  compressed
                />
              </EuiFlexItem>
            )}
          </EuiFlexGroup>
        </div>
      );
    }
  });

  return (
    <div style={{ padding: "16px" }}>
      <EuiPageHeader
        pageTitle="Manage Feature Stores"
        rightSideItems={
          operatorState.canCreate
            ? [
                <Link to="/create" key="create">
                  <EuiButton fill iconType="plusInCircle">
                    Create Feature Store
                  </EuiButton>
                </Link>,
              ]
            : []
        }
      />
      <EuiSpacer size="l" />

      {error ? (
        <>
          <EuiCallOut
            title="Failed to load feature stores"
            color="danger"
            iconType="alert"
          >
            <p>{error instanceof Error ? error.message : String(error)}</p>
          </EuiCallOut>
          <EuiSpacer />
        </>
      ) : null}

      {isLoading ? (
        <EuiFlexGroup justifyContent="center">
          <EuiFlexItem grow={false}>
            <EuiLoadingSpinner size="xl" />
          </EuiFlexItem>
        </EuiFlexGroup>
      ) : featureStores && featureStores.length === 0 ? (
        <EuiEmptyPrompt
          iconType="database"
          title={<h3>No feature stores found</h3>}
          body={<p>Create your first feature store to get started.</p>}
          actions={
            operatorState.canCreate ? (
              <Link to="/create">
                <EuiButton fill>Create Feature Store</EuiButton>
              </Link>
            ) : undefined
          }
        />
      ) : (
        <EuiBasicTable
          items={featureStores || []}
          columns={columns}
          itemId={(item) =>
            item.metadata.uid ||
            `${item.metadata.namespace}/${item.metadata.name}`
          }
          itemIdToExpandedRowMap={itemIdToExpandedRowMap}
          rowProps={(item) => ({
            onClick: () =>
              toggleRowDetails(
                item.metadata.uid ||
                  `${item.metadata.namespace}/${item.metadata.name}`,
              ),
          })}
        />
      )}

      {deleteTarget && (
        <EuiConfirmModal
          title={`Delete ${deleteTarget.metadata.name}?`}
          onCancel={() => setDeleteTarget(null)}
          onConfirm={() => deleteMutation.mutate(deleteTarget)}
          cancelButtonText="Cancel"
          confirmButtonText="Delete"
          buttonColor="danger"
          isLoading={deleteMutation.isLoading}
        >
          <EuiText>
            <p>
              This will permanently delete the feature store{" "}
              <strong>{deleteTarget.metadata.name}</strong> in namespace{" "}
              <strong>{deleteTarget.metadata.namespace}</strong> and all its
              associated resources.
            </p>
          </EuiText>
        </EuiConfirmModal>
      )}
    </div>
  );
};

export default ManageFeatureStoresPage;
