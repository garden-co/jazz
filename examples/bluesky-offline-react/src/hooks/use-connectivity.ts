import { useEffect, useState } from "react";

const pollInterval = 5_000;

export type ConnectivityStatus = "checking" | "online" | "offline";

export function connectivityStatus(
  browserOnline: boolean,
  apiReachable: boolean | undefined,
): ConnectivityStatus {
  if (!browserOnline || apiReachable === false) return "offline";
  return apiReachable === true ? "online" : "checking";
}

export async function checkApiReachable(request: typeof fetch = fetch) {
  try {
    return (await request("/api/health", { cache: "no-store" })).ok;
  } catch {
    return false;
  }
}

export function useConnectivity() {
  const [browserOnline, setBrowserOnline] = useState(navigator.onLine);
  const [apiReachable, setApiReachable] = useState<boolean | undefined>();

  useEffect(() => {
    let active = true;
    const update = () => {
      const online = navigator.onLine;
      setBrowserOnline(online);
      if (!online) {
        setApiReachable(false);
        return;
      }
      checkApiReachable().then((reachable) => {
        if (active) {
          setApiReachable(reachable);
        }
      });
    };
    update();
    const timer = window.setInterval(update, pollInterval);
    addEventListener("online", update);
    addEventListener("offline", update);
    return () => {
      active = false;
      window.clearInterval(timer);
      removeEventListener("online", update);
      removeEventListener("offline", update);
    };
  }, []);

  return {
    browserOnline,
    status: connectivityStatus(browserOnline, apiReachable),
    reportApiReachable: setApiReachable,
  };
}
