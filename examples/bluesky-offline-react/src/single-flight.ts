export function singleFlight(task: () => Promise<void>) {
  let inFlight: Promise<void> | undefined;
  return () => {
    if (inFlight) return inFlight;
    const request = task().finally(() => {
      if (inFlight === request) inFlight = undefined;
    });
    inFlight = request;
    return request;
  };
}
