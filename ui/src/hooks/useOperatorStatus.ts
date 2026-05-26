import { useQuery } from "react-query";
import type { OperatorPermissions } from "../contexts/OperatorContext";
import { defaultPermissions } from "../contexts/OperatorContext";

interface OperatorStatusResponse {
  enabled: boolean;
}

interface OperatorPermissionsResponse {
  canList: boolean;
  canCreate: boolean;
  canDelete: boolean;
  canUpdate: boolean;
  accessibleNamespaces: string[];
}

const fetchOperatorStatus = async (): Promise<OperatorStatusResponse> => {
  try {
    const res = await fetch("/api/operator/status");
    if (!res.ok) return { enabled: false };
    return res.json();
  } catch {
    return { enabled: false };
  }
};

const fetchOperatorPermissions =
  async (): Promise<OperatorPermissionsResponse> => {
    const res = await fetch("/api/operator/permissions");
    if (!res.ok) {
      return {
        canList: false,
        canCreate: false,
        canDelete: false,
        canUpdate: false,
        accessibleNamespaces: [],
      };
    }
    return res.json();
  };

const useOperatorStatus = (): OperatorPermissions => {
  const { data: status, isSuccess: statusLoaded } =
    useQuery<OperatorStatusResponse>("operator-status", fetchOperatorStatus, {
      staleTime: Infinity,
      retry: false,
    });

  const { data: permissions } = useQuery<OperatorPermissionsResponse>(
    "operator-permissions",
    fetchOperatorPermissions,
    {
      enabled: status?.enabled === true,
      staleTime: 60000,
      retry: false,
    },
  );

  if (!statusLoaded) {
    return defaultPermissions;
  }

  if (!status?.enabled) {
    return { ...defaultPermissions, loaded: true };
  }

  return {
    enabled: true,
    loaded: true,
    canList: permissions?.canList ?? false,
    canCreate: permissions?.canCreate ?? false,
    canDelete: permissions?.canDelete ?? false,
    canUpdate: permissions?.canUpdate ?? false,
    accessibleNamespaces: permissions?.accessibleNamespaces ?? [],
  };
};

export default useOperatorStatus;
