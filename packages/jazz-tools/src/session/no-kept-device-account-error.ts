/** No device account's key is kept on this device: none signed in here, or `clearOnSignOut` forgot it. */
export class NoKeptDeviceAccountError extends Error {
  override readonly name = "NoKeptDeviceAccountError";
  constructor(operation: string) {
    super(
      `${operation}: this device keeps no device account. Signing out keeps a device account's key, except with clearOnSignOut; create one with createDeviceAccount(), or restore one with restoreDeviceAccount(key).`,
    );
  }
}
