import React, { useEffect, useState } from "react";
import { setup } from "goober";
import { useJazzContext } from "jazz-tools/react-core";
import { Account } from "jazz-tools";
import { InspectorInApp } from "./in-app.js";
import { Position } from "./viewer/inspector-button.js";

export { recordMetrics, jazzMetricReader } from "./utils/instrumentation";

export function JazzInspector({ position = "right" }: { position?: Position }) {
  const context = useJazzContext<Account>();
  const localNode = context.node;
  const me = "me" in context ? context.me : undefined;

  const [isCSR, setIsCSR] = useState(false);
  useEffect(() => {
    setIsCSR(true);
  }, []);

  if (!isCSR) {
    return null;
  }

  return (
    <InspectorInApp
      position={position}
      localNode={localNode}
      accountId={me?.$jazz.raw.id}
    />
  );
}

setup(React.createElement);
