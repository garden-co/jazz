/** Use the example's public SDK instance so opaque handles stay in its realm. */
export async function prepareTestAccount<Account>(
  createManager: (config: { appId: string; serverUrl: string }) => Promise<{
    createLocalFirst(): Account;
    restoreLocalFirst(secret: string): Account;
  }>,
  appId: string,
  serverUrl: string,
  recoverySecret?: string,
): Promise<Account> {
  const manager = await createManager({ appId, serverUrl });
  return recoverySecret === undefined
    ? manager.createLocalFirst()
    : manager.restoreLocalFirst(recoverySecret);
}
