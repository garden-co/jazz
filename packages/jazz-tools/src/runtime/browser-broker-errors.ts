export const INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE =
  "incompatible-browser-broker-configuration";

export type BrowserBrokerUnsupportedCode = typeof INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE;

export class IncompatibleBrowserBrokerConfigurationError extends Error {
  readonly code = INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE;

  constructor(message = "incompatible persistent browser configuration") {
    super(message);
    this.name = "IncompatibleBrowserBrokerConfigurationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export type IncompatibleBrowserBrokerConfigurationHandler = (
  error: IncompatibleBrowserBrokerConfigurationError,
) => void;

export function createBrowserBrokerUnsupportedError(reason: string, code?: string): Error {
  if (code === INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE) {
    return new IncompatibleBrowserBrokerConfigurationError(reason);
  }
  return new Error(reason);
}

export function isIncompatibleBrowserBrokerConfigurationError(
  error: unknown,
): error is IncompatibleBrowserBrokerConfigurationError {
  return (
    error instanceof IncompatibleBrowserBrokerConfigurationError ||
    (!!error &&
      typeof error === "object" &&
      (error as { code?: unknown }).code === INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE)
  );
}
