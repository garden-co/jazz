import clsx from "clsx";
import {
  AgentSecret,
  CoID,
  LocalNode,
  RawAccount,
  RawAccountID,
  RawCoValue,
} from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  Breadcrumbs,
  Button,
  GlobalStyles,
  Icon,
  Input,
  PageStack,
  Select,
} from "jazz-inspector";
import { AccountOrGroupText } from "jazz-inspector";
import React, { useState, useEffect } from "react";
import { usePagePath } from "./use-page-path";

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

export default function CoJsonViewerApp() {
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
  const [coValueId, setCoValueId] = useState<CoID<RawCoValue> | "">("");
  const { path, addPages, goToIndex, goBack, setPage } = usePagePath();

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
        websocket: new WebSocket("wss://cloud.jazz.tools"),
        role: "server",
      });
      let node;
      try {
        node = await LocalNode.withLoadedAccount({
          accountID: currentAccount.id,
          accountSecret: currentAccount.secret,
          sessionID: crypto.newRandomSessionID(currentAccount.id),
          peersToLoadFrom: [wsPeer],
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
      setCurrentAccount(updatedAccounts.length > 0 ? updatedAccounts[0] : null);
    }
  };

  const handleCoValueIdSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (coValueId) {
      setPage(coValueId);
    }
    setCoValueId("");
  };

  if (
    path?.[0]?.coId.toString() === "import" &&
    path?.[1]?.coId !== undefined &&
    path?.[2]?.coId !== undefined
  ) {
    addAccount(
      path?.[1]?.coId as RawAccountID,
      atob(path?.[2]?.coId as string) as AgentSecret,
    );
    goToIndex(-1);
  }

  return (
    <GlobalStyles
      className={clsx(
        "h-screen overflow-hidden flex flex-col",
        " text-stone-700 bg-white",
        "dark:text-stone-300 dark:bg-stone-950",
      )}
    >
      <header className="flex items-center gap-4 p-3">
        <Breadcrumbs path={path} onBreadcrumbClick={goToIndex} />
        <div className="flex-1">
          <form onSubmit={handleCoValueIdSubmit}>
            {path.length !== 0 && (
              <Input
                className="min-w-84 font-mono"
                placeholder="co_z1234567890abcdef123456789"
                label="CoValue ID"
                hideLabel
                value={coValueId}
                onChange={(e) =>
                  setCoValueId(e.target.value as CoID<RawCoValue>)
                }
              />
            )}
          </form>
        </div>
        <AccountSwitcher
          accounts={accounts}
          currentAccount={currentAccount}
          setCurrentAccount={setCurrentAccount}
          deleteCurrentAccount={deleteCurrentAccount}
          localNode={localNode}
        />
      </header>

      <PageStack
        path={path}
        node={localNode}
        goBack={goBack}
        addPages={addPages}
      >
        {!currentAccount && (
          <AddAccountForm addAccount={addAccount} errors={errors} />
        )}

        {currentAccount && path.length <= 0 && (
          <form
            onSubmit={handleCoValueIdSubmit}
            aria-hidden={path.length !== 0}
            className="flex flex-col relative -top-6 justify-center gap-2 h-full w-full max-w-sm mx-auto"
          >
            <h1 className="text-lg text-center font-medium mb-4 text-stone-900 dark:text-white">
              Jazz CoValue Inspector
            </h1>
            <Input
              label="CoValue ID"
              className="font-mono"
              hideLabel
              placeholder="co_z1234567890abcdef123456789"
              value={coValueId}
              onChange={(e) => setCoValueId(e.target.value as CoID<RawCoValue>)}
            />
            <Button type="submit" variant="primary">
              Inspect CoValue
            </Button>

            <p className="text-center">or</p>

            <Button
              variant="secondary"
              onClick={() => {
                setPage(currentAccount.id);
              }}
            >
              Inspect my account
            </Button>
          </form>
        )}
      </PageStack>
    </GlobalStyles>
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
    <div className="relative flex items-stretch gap-1">
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
    </div>
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
    <form
      onSubmit={handleSubmit}
      className={`flex flex-col max-w-120 mx-auto justify-center ${errors == null ? "h-full" : ""}`}
    >
      {errors != null && (
        <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mt-4 font-mono whitespace-pre-wrap break-words mb-8">
          <h3>Error</h3>
          <pre className="whitespace-pre-wrap break-words overflow-hidden">
            {errors}
          </pre>
        </div>
      )}

      <h2 className="text-2xl font-medium text-gray-900 dark:text-white">
        Add an account to inspect
      </h2>
      <p className="leading-relaxed mb-5">
        Use the{" "}
        <code className="whitespace-nowrap text-stone-900 dark:text-white font-semibold">
          jazz-logged-in-secret
        </code>{" "}
        local storage key from within your Jazz app for your account
        credentials. You can paste the full JSON object or enter the ID and
        secret separately.
      </p>
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
    </form>
  );
}
