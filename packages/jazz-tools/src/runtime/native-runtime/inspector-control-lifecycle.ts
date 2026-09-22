export function inspectorControlAbortError(): Error {
  return new Error("Inspector control opening was cancelled");
}

export function closeInspectorControlPort(port: MessagePort): void {
  try {
    port.postMessage({ type: "close" });
  } catch {
    // The remote endpoint may already be unavailable or not yet adopted.
  }
  port.close();
}

export function waitForInspectorOpening<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(inspectorControlAbortError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      signal.removeEventListener("abort", onAbort);
      reject(inspectorControlAbortError());
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}
