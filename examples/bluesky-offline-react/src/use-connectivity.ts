import { useEffect, useState } from "react";

export function useConnectivity() {
  const [browserOnline, setBrowserOnline] = useState(navigator.onLine);
  const [apiReachable, setApiReachable] = useState<boolean | undefined>();

  useEffect(() => {
    const update = () => setBrowserOnline(navigator.onLine);
    addEventListener("online", update);
    addEventListener("offline", update);
    return () => {
      removeEventListener("online", update);
      removeEventListener("offline", update);
    };
  }, []);

  return {
    browserOnline,
    apiReachable,
    online: browserOnline && apiReachable !== false,
    reportApiReachable: setApiReachable,
  };
}
