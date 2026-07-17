export function shouldStartTimelineHydration(
  localQueryReady: boolean,
  browserOnline: boolean,
) {
  return localQueryReady && browserOnline;
}
