import React from "react";

interface OperatorPermissions {
  enabled: boolean;
  loaded: boolean;
  canList: boolean;
  canCreate: boolean;
  canDelete: boolean;
  canUpdate: boolean;
  accessibleNamespaces: string[];
}

const defaultPermissions: OperatorPermissions = {
  enabled: false,
  loaded: false,
  canList: false,
  canCreate: false,
  canDelete: false,
  canUpdate: false,
  accessibleNamespaces: [],
};

const OperatorContext = React.createContext<OperatorPermissions>(defaultPermissions);

export default OperatorContext;
export { defaultPermissions };
export type { OperatorPermissions };
