import type { AgentSecret, RawAccountID } from "cojson";
import React, { useCallback, useEffect, useState } from "react";
import { styled } from "goober";
import { useNode } from "./contexts/node";
import { Button, Icon, Input, Modal } from "./ui";
import { AccountOrGroupText } from "./viewer/account-or-group-text";

interface Account {
  id: RawAccountID;
  secret: AgentSecret;
  syncServer?: string;
}

interface JazzLoggedInSecret {
  accountID: string;
  accountSecret: string;
  secretSeed?: number[];
  provider?: string;
}

export function AccountSwitcher({
  defaultSyncServer,
}: {
  defaultSyncServer?: string;
}) {
  const {
    accountID: currentAccountId,
    localNode,
    createLocalNode,
    reset,
  } = useNode();
  const [accounts, setAccounts] = useState<Account[]>(() => {
    const storedAccounts = localStorage.getItem("inspectorAccounts");
    return storedAccounts ? JSON.parse(storedAccounts) : [];
  });
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedAccountId, setSelectedAccountId] =
    useState<RawAccountID | null>(() => {
      const lastSelectedAccountId = localStorage.getItem(
        "lastSelectedAccountId",
      );
      return lastSelectedAccountId
        ? (lastSelectedAccountId as RawAccountID)
        : currentAccountId;
    });
  const [newAccountId, setNewAccountId] = useState("");
  const [newAccountSecret, setNewAccountSecret] = useState("");
  const [newAccountSyncServer, setNewAccountSyncServer] = useState(
    "wss://cloud.jazz.tools",
  );
  const [addAccountError, setAddAccountError] = useState<string | null>(null);

  const addAccount = (
    id: RawAccountID,
    secret: AgentSecret,
    syncServer: string,
  ) => {
    const newAccount: Account = {
      id,
      secret,
      syncServer,
    };
    const accountExists = accounts.some((account) => account.id === id);
    if (!accountExists) {
      const updatedAccounts = [...accounts, newAccount];
      setAccounts(updatedAccounts);
      localStorage.setItem(
        "inspectorAccounts",
        JSON.stringify(updatedAccounts),
      );
      setSelectedAccountId(id);
    } else {
      setSelectedAccountId(id);
    }

    setNewAccountId("");
    setNewAccountSecret("");
    setNewAccountSyncServer(syncServer);
    setAddAccountError(null);
  };

  const deleteAccount = (accountId: RawAccountID) => {
    const updatedAccounts = accounts.filter(
      (account) => account.id !== accountId,
    );
    setAccounts(updatedAccounts);
    localStorage.setItem("inspectorAccounts", JSON.stringify(updatedAccounts));
    if (updatedAccounts.length > 0) {
      setCurrentAccount(updatedAccounts[0]!.id);
    } else {
      setSelectedAccountId(null);
      localStorage.removeItem("lastSelectedAccountId");
      reset();
    }
  };

  const deleteCurrentAccount = () => {
    if (currentAccountId) {
      deleteAccount(currentAccountId);
    }
  };

  const setCurrentAccount = useCallback(
    async (accountId: RawAccountID | null) => {
      if (accountId === null) {
        localStorage.removeItem("lastSelectedAccountId");
        reset();
        setSelectedAccountId(null);

        return;
      }

      const account = accounts.find((a) => a.id === accountId);
      if (!account) {
        throw new Error(`Account ${accountId} not found in accounts list`);
      }
      const syncServer =
        account.syncServer || defaultSyncServer || "wss://cloud.jazz.tools";

      await createLocalNode(accountId, account.secret, syncServer);

      setSelectedAccountId(accountId);
      localStorage.setItem("lastSelectedAccountId", accountId);
    },
    [createLocalNode, accounts, defaultSyncServer],
  );

  const handleModalConfirm = async () => {
    if (selectedAccountId) {
      await setCurrentAccount(selectedAccountId);
      setIsModalOpen(false);
    }
  };

  const handleModalCancel = () => {
    setSelectedAccountId(currentAccountId);
    setNewAccountId("");
    setNewAccountSecret("");
    setNewAccountSyncServer("wss://cloud.jazz.tools/");
    setAddAccountError(null);
    setIsModalOpen(false);
  };

  const handleNewAccountIdChange = (
    e: React.ChangeEvent<HTMLInputElement>,
  ): void => {
    const value = e.target.value;
    setNewAccountId(value);

    if (value.trim().startsWith("{") && value.trim().endsWith("}")) {
      try {
        const parsed: JazzLoggedInSecret = JSON.parse(value);
        if (parsed.accountID && parsed.accountSecret) {
          setNewAccountId(parsed.accountID);
          setNewAccountSecret(parsed.accountSecret);
        }
      } catch (error) {
        console.log("Failed to parse JSON:", error);
      }
    }
  };

  const handleAddAccountSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newAccountId || !newAccountSecret) {
      setAddAccountError("Account ID and secret are required");
      return;
    }
    try {
      // first: try to use the credentials
      // if successful, add the account to the list
      addAccount(
        newAccountId as RawAccountID,
        newAccountSecret as AgentSecret,
        newAccountSyncServer,
      );

      // await setCurrentAccount(newAccountId as RawAccountID);
      setSelectedAccountId(newAccountId as RawAccountID);

      setIsModalOpen(false);
    } catch (error) {
      setAddAccountError(
        error instanceof Error ? error.message : "Failed to add account",
      );
      deleteAccount(newAccountId as RawAccountID);
    }
  };

  useEffect(() => {
    if (selectedAccountId) {
      setCurrentAccount(selectedAccountId);
    }
  }, [selectedAccountId]);

  return (
    <>
      <AccountSwitcherContainer>
        <Button
          variant="secondary"
          onClick={() => {
            setSelectedAccountId(currentAccountId);
            setIsModalOpen(true);
          }}
          className="max-w-96"
        >
          {currentAccountId ? (
            localNode ? (
              <AccountOrGroupText
                coId={currentAccountId}
                showId
                node={localNode}
              />
            ) : (
              currentAccountId
            )
          ) : (
            "Select account"
          )}
        </Button>
        {currentAccountId && (
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

      <Modal
        isOpen={isModalOpen}
        onClose={handleModalCancel}
        heading="Select Account"
        showButtons={false}
        wide={true}
      >
        <ModalContentGrid>
          <div>
            <AccountSelectionFieldset>
              <legend>Accounts</legend>
              {accounts.length === 0 ? (
                <p style={{ color: "var(--j-text-color)", margin: "0.5rem 0" }}>
                  No accounts available. Add one below.
                </p>
              ) : (
                accounts.map((account) => (
                  <RadioOption key={account.id}>
                    <input
                      type="radio"
                      id={`account-${account.id}`}
                      name="account-selection"
                      value={account.id}
                      checked={selectedAccountId === account.id}
                      onChange={(e) =>
                        setSelectedAccountId(e.target.value as RawAccountID)
                      }
                    />
                    <label htmlFor={`account-${account.id}`}>
                      <AccountLabelContent>
                        {localNode ? (
                          <AccountOrGroupText
                            coId={account.id}
                            showId
                            node={localNode}
                          />
                        ) : (
                          account.id
                        )}
                        <SyncServerText>
                          {account.syncServer ?? "cloud.jazz.tools"}
                        </SyncServerText>
                      </AccountLabelContent>
                    </label>
                  </RadioOption>
                ))
              )}
            </AccountSelectionFieldset>
            {accounts.length > 0 && (
              <ConfirmButtonContainer>
                <Button
                  variant="primary"
                  onClick={handleModalConfirm}
                  disabled={!selectedAccountId}
                >
                  Confirm
                </Button>
              </ConfirmButtonContainer>
            )}
          </div>

          <ModalAddAccountForm onSubmit={handleAddAccountSubmit}>
            <FormHeading>Add an account to inspect</FormHeading>
            <FormDescription>
              Use the <CodeInline>jazz-logged-in-secret</CodeInline> local
              storage key from within your Jazz app for your account
              credentials. You can paste the full JSON object or enter the ID
              and secret separately.
            </FormDescription>
            {addAccountError && <ErrorText>{addAccountError}</ErrorText>}
            <Input
              label="Account ID"
              value={newAccountId}
              required
              placeholder="co_z1234567890abcdef123456789 or paste full JSON"
              onChange={handleNewAccountIdChange}
            />
            <Input
              label="Account secret"
              type="password"
              required
              value={newAccountSecret}
              onChange={(e) => setNewAccountSecret(e.target.value)}
              placeholder="sealerSecret_ziz7NA12340abcdef123789..."
            />
            <Input
              label="Sync server"
              required
              value={newAccountSyncServer}
              onChange={(e) => setNewAccountSyncServer(e.target.value)}
              placeholder="wss://cloud.jazz.tools/"
            />
            <Button className="mt-3" type="submit">
              Add account
            </Button>
          </ModalAddAccountForm>
        </ModalContentGrid>
      </Modal>
    </>
  );
}

const AccountSwitcherContainer = styled("div")`
  position: relative;
  display: flex;
  align-items: stretch;
  gap: 0.25rem;
`;

const ModalContentGrid = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 1.5rem;

  @media (min-width: 768px) {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1.5rem;
  }
`;

const AccountSelectionFieldset = styled("fieldset")`
  border: 1px solid var(--j-border-color);
  border-radius: var(--j-radius-md);
  padding: 1rem;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 0.75rem;

  legend {
    padding: 0 0.5rem;
    font-weight: 500;
    color: var(--j-text-color);
  }
`;

const ConfirmButtonContainer = styled("div")`
  margin-top: 0.5rem;
  display: flex;
  justify-content: flex-end;
`;

const RadioOption = styled("div")`
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;

  input[type="radio"] {
    margin: 0;
    cursor: pointer;
    margin-top: 0.25rem;
  }

  label {
    cursor: pointer;
    color: var(--j-text-color);
    flex: 1;
  }
`;

const AccountLabelContent = styled("div")`
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
`;

const SyncServerText = styled("span")`
  font-style: italic;
  font-size: 0.875rem;
  color: var(--j-text-color);
  opacity: 0.7;
`;

const ModalAddAccountForm = styled("form")`
  display: flex;
  flex-direction: column;
  gap: 1rem;

  @media (max-width: 767px) {
    padding-top: 1rem;
    border-top: 1px solid var(--j-border-color);
  }
`;

const ErrorText = styled("p")`
  color: #b91c1c;
  font-size: 0.875rem;
  margin: 0;
  padding: 0.5rem;
  background-color: #fee2e2;
  border-radius: var(--j-radius-sm);
  border: 1px solid #f87171;

  @media (prefers-color-scheme: dark) {
    color: #fca5a5;
    background-color: #7f1d1d;
    border-color: #991b1b;
  }
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
  margin-bottom: 1.25rem;
  font-size: 0.875rem;
  color: var(--j-text-color);
`;

const CodeInline = styled("code")`
  white-space: nowrap;
  color: #0c0a09;
  font-weight: 600;

  @media (prefers-color-scheme: dark) {
    color: #fff;
  }
`;
