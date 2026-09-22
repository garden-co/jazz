export class SecretLoadError extends Error {
  override readonly cause: unknown;

  constructor(cause: unknown) {
    super("Failed to load the authentication secret");
    this.name = "SecretLoadError";
    this.cause = cause;
  }
}

export async function loadSecret<T>(load: () => Promise<T>): Promise<T> {
  try {
    return await load();
  } catch (cause) {
    throw new SecretLoadError(cause);
  }
}

export interface ResettablePromise<T> {
  get(): Promise<T>;
  reset(): void;
}

export function createResettablePromise<T>(load: () => Promise<T>): ResettablePromise<T> {
  let promise: Promise<T> | undefined;

  return {
    get() {
      promise ??= load();
      return promise;
    },
    reset() {
      promise = undefined;
    },
  };
}
