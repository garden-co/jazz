import type { CryptoProvider } from "cojson";

/**
 * Options for the CrossDeviceAccountTransfer class.
 */
export interface CrossDeviceAccountTransferOptions {
  /**
   * Function to generate a confirmation code.
   * @param crypto - The crypto provider to use for random number generation.
   * @returns The generated confirmation code.
   */
  confirmationCodeFn: (crypto: CryptoProvider) => string | Promise<string>;
  handlerPath: string;
}

/**
 * Options for CrossDeviceAccountTransfer consumer classes.
 */
export interface CrossDeviceAccountTransferAsTargetOptions {
  /**
   * The timeout for the consumer handler.
   */
  handlerTimeout?: number;
  /**
   * The function to call when the consumer is logged in.
   */
  onLoggedIn?: () => void;
}

/**
 * Options for CrossDeviceAccountTransfer provider classes.
 */
export interface CrossDeviceAccountTransferAsSourceOptions {
  /**
   * The expiration time for the provider.
   */
  expireInMs?: number;
}
