export function waitFor(
  callback: () => boolean | void,
  getError?: () => Error,
) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = () => {
      try {
        return { ok: callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(() => {
      const { ok, error } = checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 20) {
        clearInterval(interval);
        reject(getError ? getError() : error);
      }
    }, 100);
  });
}
