import { useEffect } from "react";
import { startInspectorOnce } from "../dev-tools/auto-attach.js";
import { useJazzClient as useCoreJazzClient } from "../react-core/provider.js";
import type { JazzClient } from "./create-jazz-client.js";

interface JazzClientContextValue {
  db: JazzClient["db"];
}

export function DevToolsAutoAttach() {
  const { db } = useCoreJazzClient() as unknown as JazzClientContextValue;

  useEffect(() => {
    startInspectorOnce(db);
  }, [db]);

  return null;
}
