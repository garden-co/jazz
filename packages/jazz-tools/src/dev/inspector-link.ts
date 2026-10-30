const STANDALONE_INSPECTOR_URL = "https://jazz2-inspector.vercel.app/";

export function logInspectorLink({ serverUrl, appId }: { serverUrl: string; appId: string }): void {
  const link = `${STANDALONE_INSPECTOR_URL}#serverUrl=${encodeURIComponent(serverUrl)}&appId=${encodeURIComponent(appId)}`;
  console.log(`[jazz] Open the inspector: ${link}`);
}
