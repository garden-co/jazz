export const debounce = <TArgs extends unknown[]>(
  callback: (...args: TArgs) => void,
  wait: number,
): ((...args: TArgs) => void) => {
  let timeoutId: number | null = null;
  return (...args: TArgs) => {
    if (timeoutId) {
      window.clearTimeout(timeoutId);
    }
    timeoutId = window.setTimeout(() => {
      callback(...args);
    }, wait);
  };
};
