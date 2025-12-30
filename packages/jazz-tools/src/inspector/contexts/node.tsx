import {
  createContext,
  type PropsWithChildren,
  useContext,
  useEffect,
  useState,
} from "react";
import { CoID, LocalNode, RawAccount } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { createWebSocketPeer } from "cojson-transport-ws";

type NodeContextType = {
  accountID: CoID<RawAccount> | null;
  localNode: LocalNode | null;
  server: string;
  createLocalNode: (
    accountID: CoID<RawAccount>,
    clientSecret: string,
    server: string,
  ) => Promise<void>;
  reset: () => void;
};

export const NodeContext = createContext<NodeContextType>({
  accountID: null,
  localNode: null,
  server: "wss://cloud.jazz.tools/",
  createLocalNode: async () => {
    throw new Error("createLocalNode not implemented");
  },
  reset: () => {
    throw new Error("reset not implemented");
  },
});

type NodeProviderProps = PropsWithChildren<{
  localNode?: LocalNode | null;
  accountID?: CoID<RawAccount> | null;
  server?: string;
}>;

let crypto: WasmCrypto | null = null;

async function getCrypto() {
  if (crypto) return crypto;
  crypto = await WasmCrypto.create();
  return crypto;
}

export function NodeProvider(props: NodeProviderProps) {
  const [accountID, setAccountID] = useState<CoID<RawAccount> | null>(
    props?.accountID ?? null,
  );
  const [localNode, setLocalNode] = useState<LocalNode | null>(
    props?.localNode ?? null,
  );
  const [server, setServer] = useState<string>(
    props?.server ?? "wss://cloud.jazz.tools/",
  );

  useEffect(() => {
    if (props.localNode !== undefined) setLocalNode(props.localNode);

    if (props.accountID !== undefined) setAccountID(props.accountID);

    if (props.server !== undefined) setServer(props.server);
  }, [props.localNode, props.accountID, props.server]);

  async function createLocalNode(
    accountID: CoID<RawAccount>,
    clientSecret: string,
    server: string,
  ) {
    if (localNode) {
      localNode.gracefulShutdown();
    }

    setLocalNode(null);

    const wsPeer = createWebSocketPeer({
      id: "cloud",
      websocket: new WebSocket(server),
      role: "server",
    });

    const crypto = await getCrypto();

    const node = await LocalNode.withLoadedAccount({
      accountID: accountID,
      accountSecret: clientSecret as any,
      sessionID: crypto.newRandomSessionID(accountID),
      peers: [wsPeer],
      crypto,
      migration: async () => {
        console.log("Not running any migration in inspector");
      },
    });

    setLocalNode(node);
    setAccountID(accountID);
    setServer(server);
  }

  function reset() {
    if (localNode) {
      localNode.gracefulShutdown();
    }
    setLocalNode(null);
    setAccountID(null);
    setServer("wss://cloud.jazz.tools/");
  }

  return (
    <NodeContext.Provider
      value={{ accountID, localNode, server, createLocalNode, reset }}
    >
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
