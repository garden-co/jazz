export class CoValuePromise<out T> extends Promise<T> {
  status: "pending" | "fulfilled" | "rejected" = "pending";
  value: T | undefined;
  reason: unknown | undefined;
}

export function resolvedPromise<T>(value: T): CoValuePromise<T> {
  const promise = CoValuePromise.resolve(value) as CoValuePromise<T>;
  promise.status = "fulfilled";
  promise.value = value;
  return promise;
}

export function rejectedPromise<T>(reason: unknown): CoValuePromise<T> {
  const promise = CoValuePromise.reject(reason) as CoValuePromise<T>;
  promise.status = "rejected";
  promise.reason = reason;
  return promise;
}
