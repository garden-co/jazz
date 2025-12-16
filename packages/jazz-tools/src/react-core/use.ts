import React from "react";

// shim from https://github.com/pmndrs/jotai/blob/f287c5d665a807e676bc731e83174c62c1fe1fc9/src/react/useAtomValue.ts#L13C1-L56C1
const attachPromiseStatus = <T>(
  promise: PromiseLike<T> & {
    status?: "pending" | "fulfilled" | "rejected";
    value?: T;
    reason?: unknown;
  },
) => {
  if (!promise.status) {
    promise.status = "pending";
    promise.then(
      (v) => {
        promise.status = "fulfilled";
        promise.value = v;
      },
      (e) => {
        promise.status = "rejected";
        promise.reason = e;
      },
    );
  }
};

export const use =
  React.use ||
  // A shim for older React versions
  (<T>(
    promise: PromiseLike<T> & {
      status?: "pending" | "fulfilled" | "rejected";
      value?: T;
      reason?: unknown;
    },
  ): T => {
    if (promise.status === "pending") {
      throw promise;
    } else if (promise.status === "fulfilled") {
      return promise.value as T;
    } else if (promise.status === "rejected") {
      throw promise.reason;
    } else {
      attachPromiseStatus(promise);
      throw promise;
    }
  });
