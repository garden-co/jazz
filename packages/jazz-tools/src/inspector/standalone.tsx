import { AgentSecret, CoID, LocalNode, RawAccount, RawAccountID } from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";

import React, { useState, useEffect } from "react";
import { HashRouterProvider, useRouter } from "./router";
import { setup, styled } from "goober";
import { NodeProvider } from "./contexts/node";
import { Header } from "./viewer/header";
import { GlobalStyles } from "./ui/global-styles";
import { PageStack } from "./viewer/page-stack";
import { Button, Icon, Input, Select } from "./ui";
import { AccountOrGroupText } from "./viewer/account-or-group-text";

interface Account {
  id: CoID<RawAccount>;
  secret: AgentSecret;
}

interface JazzLoggedInSecret {
  accountID: string;
  accountSecret: string;
  secretSeed?: number[];
  provider?: string;
}

type InspectorAppProps = {
  defaultSyncServer?: string;
};

setup(React.createElement);

export default function InspectorStandalone(props: InspectorAppProps) {
  return (
    <HashRouterProvider>
      <CoJsonViewerApp {...props} />
    </HashRouterProvider>
  );
}

function CoJsonViewerApp(props: InspectorAppProps) {
  const [errors, setErrors] = useState<string | null>(null);
  const [accounts, setAccounts] = useState<Account[]>(() => {
    const storedAccounts = localStorage.getItem("inspectorAccounts");
    return storedAccounts ? JSON.parse(storedAccounts) : [];
  });
  const [currentAccount, setCurrentAccount] = useState<Account | null>(() => {
    const lastSelectedId = localStorage.getItem("lastSelectedAccountId");
    if (lastSelectedId) {
      const lastAccount = accounts.find(
        (account) => account.id === lastSelectedId,
      );
      return lastAccount || null;
    }
    return null;
  });
  const [localNode, setLocalNode] = useState<LocalNode | null>(null);
  const { path, goToIndex } = useRouter();

  useEffect(() => {
    localStorage.setItem("inspectorAccounts", JSON.stringify(accounts));
  }, [accounts]);

  useEffect(() => {
    if (currentAccount) {
      localStorage.setItem("lastSelectedAccountId", currentAccount.id);
    } else {
      localStorage.removeItem("lastSelectedAccountId");
    }
  }, [currentAccount]);

  useEffect(() => {
    if (!currentAccount && path.length > 0) {
      setLocalNode(null);
      goToIndex(-1);
      return;
    }

    if (!currentAccount) return;

    WasmCrypto.create().then(async (crypto) => {
      const wsPeer = createWebSocketPeer({
        id: "cloud",
        websocket: new WebSocket(
          props.defaultSyncServer || "wss://cloud.jazz.tools/",
        ),
        role: "server",
      });
      let node;
      try {
        node = await LocalNode.withLoadedAccount({
          accountID: currentAccount.id,
          accountSecret: currentAccount.secret,
          sessionID: crypto.newRandomSessionID(currentAccount.id),
          peers: [wsPeer],
          crypto,
          migration: async () => {
            console.log("Not running any migration in inspector");
          },
        });
      } catch (err: any) {
        if (err.toString().includes("invalid id")) {
          setAccounts(accounts.filter((acc) => acc.id !== currentAccount.id));
          //remove from localStorage
          localStorage.removeItem("lastSelectedAccountId");
          localStorage.setItem(
            "inspectorAccounts",
            JSON.parse(localStorage.inspectorAccounts).filter(
              (acc: Account) => acc.id != currentAccount.id,
            ),
          );
          setCurrentAccount(null);
          setErrors("Trying to load covalue with invalid id");
        } else {
          setErrors("The account could not be loaded");
        }
        setLocalNode(null);
        goToIndex(-1);
        return;
      }
      setLocalNode(node);
    });
  }, [currentAccount, accounts, goToIndex, path]);

  const addAccount = (id: RawAccountID, secret: AgentSecret) => {
    const newAccount = { id, secret };
    const accountExists = accounts.some((account) => account.id === id);
    //todo: ideally there would be some validation here so we don't have to manually remove a non existent account from localStorage
    if (!accountExists) {
      setAccounts([...accounts, newAccount]);
    }
    setCurrentAccount(newAccount);
  };

  const deleteCurrentAccount = () => {
    if (currentAccount) {
      const updatedAccounts = accounts.filter(
        (account) => account.id !== currentAccount.id,
      );
      setAccounts(updatedAccounts);
      setCurrentAccount(
        updatedAccounts.length > 0 ? updatedAccounts[0]! : null,
      );
    }
  };

  return (
    <HashRouterProvider>
      <NodeProvider
        localNode={localNode}
        accountID={currentAccount?.id ?? null}
      >
        <InspectorContainer as={GlobalStyles}>
          <Header>
            <AccountSwitcher
              accounts={accounts}
              currentAccount={currentAccount}
              setCurrentAccount={setCurrentAccount}
              deleteCurrentAccount={deleteCurrentAccount}
              localNode={localNode}
            />
          </Header>

          <PageStack
            homePage={
              currentAccount ? null : (
                <AddAccountForm addAccount={addAccount} errors={errors} />
              )
            }
          />
        </InspectorContainer>
      </NodeProvider>
    </HashRouterProvider>
  );
}

function AccountSwitcher({
  accounts,
  currentAccount,
  setCurrentAccount,
  deleteCurrentAccount,
  localNode,
}: {
  accounts: Account[];
  currentAccount: Account | null;
  setCurrentAccount: (account: Account | null) => void;
  deleteCurrentAccount: () => void;
  localNode: LocalNode | null;
}) {
  return (
    <AccountSwitcherContainer>
      <Select
        label="Account to inspect"
        hideLabel
        className="label:sr-only max-w-96"
        value={currentAccount?.id || "add-account"}
        onChange={(e) => {
          if (e.target.value === "add-account") {
            setCurrentAccount(null);
          } else {
            const account = accounts.find((a) => a.id === e.target.value);
            setCurrentAccount(account || null);
          }
        }}
      >
        {accounts.map((account) => (
          <option key={account.id} value={account.id}>
            {localNode ? (
              <AccountOrGroupText coId={account.id} showId node={localNode} />
            ) : (
              account.id
            )}
          </option>
        ))}
        <option value="add-account">Add account</option>
      </Select>
      {currentAccount && (
        <Button
          variant="secondary"
          onClick={deleteCurrentAccount}
          className="rounded-md p-2 ml-1"
          aria-label="Remove account"
        >
          <Icon name="delete" className="text-gray-500" />
        </Button>
      )}
    </AccountSwitcherContainer>
  );
}

function AddAccountForm({
  addAccount,
  errors,
}: {
  addAccount: (id: RawAccountID, secret: AgentSecret) => void;
  errors: string | null;
}) {
  const [id, setId] = useState("");
  const [secret, setSecret] = useState("");

  const handleIdChange = (e: React.ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value;
    setId(value);

    // Try to parse as JSON if it looks like a JSON object
    if (value.trim().startsWith("{") && value.trim().endsWith("}")) {
      try {
        const parsed: JazzLoggedInSecret = JSON.parse(value);
        if (parsed.accountID && parsed.accountSecret) {
          setId(parsed.accountID);
          setSecret(parsed.accountSecret);
        }
      } catch (error) {
        // If parsing fails, just keep the raw value in the id field
        console.log("Failed to parse JSON:", error);
      }
    }
  };

  const handleSubmit = (e: React.FormEvent): void => {
    e.preventDefault();
    addAccount(id as RawAccountID, secret as AgentSecret);
    setId("");
    setSecret("");
  };

  return (
    <AddAccountFormContainer
      onSubmit={handleSubmit}
      fullHeight={errors == null}
    >
      {errors != null && (
        <ErrorContainer>
          <h3>Error</h3>
          <ErrorPre>{errors}</ErrorPre>
        </ErrorContainer>
      )}

      <FormHeading>Add an account to inspect</FormHeading>
      <FormDescription>
        Use the <CodeInline>jazz-logged-in-secret</CodeInline> local storage key
        from within your Jazz app for your account credentials. You can paste
        the full JSON object or enter the ID and secret separately.
      </FormDescription>
      <Input
        label="Account ID"
        value={id}
        placeholder="co_z1234567890abcdef123456789 or paste full JSON"
        onChange={handleIdChange}
      />
      <Input
        label="Account secret"
        type="password"
        value={secret}
        onChange={(e) => setSecret(e.target.value)}
        placeholder="sealerSecret_ziz7NA12340abcdef123789..."
      />
      <Button className="mt-3" type="submit">
        Add account
      </Button>
    </AddAccountFormContainer>
  );
}

const InspectorContainer = styled("div")`
  height: 100vh;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  color: #44403c; /* text-stone-700 */
  background-color: #fff; /* bg-white */

  @media (prefers-color-scheme: dark) {
    color: #d6d3d1; /* text-stone-300 */
    background-color: #0c0a09; /* bg-stone-950 */
  }
`;

const AccountSwitcherContainer = styled("div")`
  position: relative;
  display: flex;
  align-items: stretch;
  gap: 0.25rem;
`;

const AddAccountFormContainer = styled("form")<{ fullHeight: boolean }>`
  display: flex;
  flex-direction: column;
  max-width: 30rem;
  margin-left: auto;
  margin-right: auto;
  justify-content: center;
  ${(props) => (props.fullHeight ? "height: 100%;" : "")}
`;

const ErrorContainer = styled("div")`
  background-color: #fee2e2;
  border: 1px solid #f87171;
  color: #b91c1c;
  padding: 0.75rem 1rem;
  border-radius: 0.25rem;
  margin-top: 1rem;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  white-space: pre-wrap;
  word-break: break-words;
  margin-bottom: 2rem;
`;

const ErrorPre = styled("pre")`
  white-space: pre-wrap;
  word-break: break-words;
  overflow: hidden;
`;

const FormHeading = styled("h2")`
  font-size: 1.5rem;
  font-weight: 500;
  color: #111827;

  @media (prefers-color-scheme: dark) {
    color: #fff;
  }
`;

const FormDescription = styled("p")`
  line-height: 1.625;
  margin-bottom: 1.25rem;
`;

const CodeInline = styled("code")`
  white-space: nowrap;
  color: #0c0a09;
  font-weight: 600;

  @media (prefers-color-scheme: dark) {
    color: #fff;
  }
`;
