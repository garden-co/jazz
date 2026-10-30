import { createJazzSession } from "jazz-tools/client";
import { mountApp } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
function buildConfig() {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return { appId: APP_ID, serverUrl: SERVER_URL };
}

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");
  const config = buildConfig();
  let disposed = false;
  let session: Awaited<ReturnType<typeof createJazzSession>> | undefined;
  let unsubscribe = () => {};
  let unmount = () => {};
  const onPageHide = (event: PageTransitionEvent) => {
    if (event.persisted) return;
    disposed = true;
    window.removeEventListener("pagehide", onPageHide);
    unsubscribe();
    unmount();
    void session?.close().catch(console.error);
  };
  window.addEventListener("pagehide", onPageHide);
  try {
    session = await createJazzSession({ ...config, initial: "local-first" });
    if (disposed) return await session.close();
    const activeSession = session;
    const render = () => {
      unmount();
      unmount = () => {};
      const { status, client, account, error } = activeSession.getSnapshot();
      if (status === "ready" && client && account) {
        unmount = mountApp(root, client.db, account, activeSession.restoreLocalFirst);
        if (error) {
          const alert = document.createElement("p");
          alert.setAttribute("role", "alert");
          alert.textContent = error.message;
          root.prepend(alert);
        }
      } else {
        root.textContent = error?.message ?? "Loading...";
        if (error) {
          const retry = document.createElement("button");
          retry.textContent = "Retry";
          retry.onclick = () => void activeSession.retry().catch(() => {});
          root.append(retry);
        }
      }
    };
    unsubscribe = session.subscribe(render);
    render();
  } catch (error) {
    window.removeEventListener("pagehide", onPageHide);
    throw error;
  }
}

boot().catch((error: unknown) => {
  const root = document.getElementById("root");
  if (root) root.textContent = error instanceof Error ? error.message : String(error);
});
