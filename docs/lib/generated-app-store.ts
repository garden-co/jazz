export interface GeneratedApp {
  appId: string;
  adminSecret: string;
  backendSecret: string;
}

const STORAGE_KEY = "jazz-quickstart-generated-app";
const EVENT_NAME = "jazz-app-generated";

export function storeGeneratedApp(app: GeneratedApp): void {
  try {
    sessionStorage.setItem(STORAGE_KEY, JSON.stringify(app));
  } catch {
    // sessionStorage may be unavailable in some environments
  }
  substituteInPage(app);
  window.dispatchEvent(new CustomEvent(EVENT_NAME, { detail: app }));
}

function substituteInPage(app: GeneratedApp): void {
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  let node: Text | null;
  while ((node = walker.nextNode() as Text | null)) {
    if (!node.nodeValue) continue;
    if (
      node.nodeValue.includes("<your-app-id>") ||
      node.nodeValue.includes("<your-admin-secret>")
    ) {
      node.nodeValue = node.nodeValue
        .replaceAll("<your-app-id>", app.appId)
        .replaceAll("<your-admin-secret>", app.adminSecret);
    }
  }
}

export function restoreFromSession(): void {
  const app = getStoredApp();
  if (app) substituteInPage(app);
}

export function getStoredApp(): GeneratedApp | null {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as GeneratedApp) : null;
  } catch {
    return null;
  }
}

export function onAppGenerated(cb: (app: GeneratedApp) => void): () => void {
  const handler = (e: Event) => cb((e as CustomEvent<GeneratedApp>).detail);
  window.addEventListener(EVENT_NAME, handler);
  return () => window.removeEventListener(EVENT_NAME, handler);
}
