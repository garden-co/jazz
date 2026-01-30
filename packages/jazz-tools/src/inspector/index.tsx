import React, { useEffect, useState } from "react";
import { setup } from "goober";
import { useJazzContextValue } from "jazz-tools/react-core";
import { Account, SubscriptionScope } from "jazz-tools";
import { InspectorInApp } from "./in-app.js";
import { Position } from "./viewer/inspector-button.js";

export function enableProfiling() {
  SubscriptionScope.enableProfiling();
}

export function JazzInspector({ position = "right" }: { position?: Position }) {
  const context = useJazzContextValue<Account>();
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
      showDeleteLocalData
    />
  );
}

setup(React.createElement);
