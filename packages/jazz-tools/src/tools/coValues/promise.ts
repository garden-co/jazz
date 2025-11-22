export class CoValuePromise<T> extends Promise<T> {
  status: "pending" | "fulfilled" | "rejected" = "pending";
  value: T | undefined;
  reason: unknown | undefined;

  static getRejected<T = never>(reason?: unknown): CoValuePromise<T> {
    return new CoValuePromise<T>((resolve, reject) => {
      reject(reason);
    });
  }

  static getFulfilled<T>(value: T): CoValuePromise<T> {
    return new CoValuePromise<T>((resolve) => {
      resolve(value);
    });
  }

  constructor(
    executor: (
      resolve: (value: T) => void,
      reject: (reason?: unknown) => void,
    ) => void,
  ) {
    super((resolve, reject) => {
      const _resolve = (value: T) => {
        resolve(value);
      };
      const _reject = (reason?: unknown) => {
        reject(reason);
      };
      executor(_resolve, _reject);
    });
  }
}
