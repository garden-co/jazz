const STANDALONE_INSPECTOR_URL = "https://jazz2-inspector.vercel.app/";

function encodeFragmentValue(value: string): string {
  return encodeURIComponent(value);
}

export function buildInspectorLink(serverUrl: string, appId: string): string {
  return (
    `${STANDALONE_INSPECTOR_URL}#serverUrl=${encodeFragmentValue(serverUrl)}` +
    `&appId=${encodeFragmentValue(appId)}`
  );
}
