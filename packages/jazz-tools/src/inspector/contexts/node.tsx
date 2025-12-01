import {
  createContext,
  type PropsWithChildren,
  useContext,
  useEffect,
  useState,
} from "react";
import type { CoID, LocalNode, RawAccount } from "cojson";

type NodeContextType = {
  accountID: CoID<RawAccount> | null;
  localNode: LocalNode | null;
  setLocalNode: (localNode: LocalNode | null) => void;
};

export const NodeContext = createContext<NodeContextType>({
  accountID: null,
  localNode: null,
  setLocalNode: () => {},
});

type NodeProviderProps = PropsWithChildren<{
  localNode: LocalNode | null;
  accountID: CoID<RawAccount> | null;
}>;

export function NodeProvider(props: NodeProviderProps) {
  const [accountID, setAccountID] = useState<CoID<RawAccount> | null>(
    props.accountID,
  );
  const [localNode, setLocalNode] = useState<LocalNode | null>(props.localNode);

  useEffect(() => {
    setLocalNode(props.localNode);
    setAccountID(props.accountID);
  }, [props.localNode, props.accountID]);

  return (
    <NodeContext.Provider value={{ accountID, localNode, setLocalNode }}>
      {props.children}
    </NodeContext.Provider>
  );
}

export function useNode() {
  const context = useContext(NodeContext);
  if (!context) {
    throw new Error("useNode must be used within a NodeProvider");
  }
  return context;
}
