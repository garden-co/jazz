import { Button } from "@/components/Button";
import { AccountsType, useAuth } from "jazz-react-auth-betterauth";

export const AccountProviders = ({
  setLoading,
  setError,
  accounts,
}: {
  setLoading: React.Dispatch<React.SetStateAction<boolean>>;
  setError: React.Dispatch<React.SetStateAction<Error | undefined>>;
  accounts: AccountsType | undefined;
}) => {
  const auth = useAuth();
  return (
    <table className="w-full text-sm border-full border-collapse">
      <thead className="text-xs">
        <tr>
          <th scope="col" className="px-6 py-3">
            Provider
          </th>
          <th scope="col" className="px-6 py-3">
            Created
          </th>
          <th scope="col" className="px-6 py-3">
            Updated
          </th>
          <th scope="col" className="px-6 py-3">
            Scopes
          </th>
        </tr>
      </thead>
      <tbody>
        {!accounts?.data?.length && "No authentication providers found"}
        {accounts?.data &&
          accounts.data.map((account) => (
            <tr key={account.id} className="border-b">
              <th
                scope="row"
                className="px-6 py-4 font-medium whitespace-nowrap"
              >
                {account.provider}
              </th>
              <td className="px-6 py-4">
                {account.createdAt.toLocaleString()}
              </td>
              <td className="px-6 py-4">
                {account.updatedAt.toLocaleString()}
              </td>
              <td className="px-6 py-4">{account.scopes.join(", ")}</td>
              <td className="px-6 py-4">
                <Button
                  variant="secondary"
                  className="relative"
                  onClick={async (e) => {
                    e.preventDefault();
                    setLoading(true);
                    const { error } = await auth.authClient.unlinkAccount({
                      providerId: account.provider,
                      accountId: account.id,
                    });
                    const errorMessage = error?.message ?? error?.statusText;
                    setError(
                      error
                        ? {
                            ...error,
                            name: error.statusText,
                            message:
                              errorMessage && errorMessage.length > 0
                                ? errorMessage
                                : "An error occurred",
                          }
                        : undefined,
                    );
                    setLoading(false);
                  }}
                >
                  Unlink
                </Button>
              </td>
            </tr>
          ))}
      </tbody>
    </table>
  );
};
