import React from "react";

export { JazzInspectorInternal } from "./viewer/new-app.js";
export { PageStack } from "./viewer/page-stack.js";
export { Breadcrumbs } from "./viewer/breadcrumbs.js";
export { AccountOrGroupText } from "./viewer/account-or-group-text.js";

export { Button } from "./ui/button.js";
export { Input } from "./ui/input.js";
export { Select } from "./ui/select.js";
export { Icon } from "./ui/icon.js";
export { GlobalStyles } from "./ui/global-styles.js";

export {
  resolveCoValue,
  useResolvedCoValue,
} from "./viewer/use-resolve-covalue.js";

export type { PageInfo } from "./viewer/types.js";

import { setup } from "goober";
import { useJazzContext } from "jazz-tools/react-core";
import { Account } from "jazz-tools";

import { JazzInspectorInternal } from "./viewer/new-app.js";
import { Position } from "./viewer/inspector-button.js";

export function JazzInspector({ position = "right" }: { position?: Position }) {
  const context = useJazzContext<Account>();
  const localNode = context.node;
  const me = "me" in context ? context.me : undefined;

  if (process.env.NODE_ENV !== "development") return null;

  return (
    <JazzInspectorInternal
      position={position}
      localNode={localNode}
      accountId={me?.$jazz.raw.id}
    />
  );
}

setup(React.createElement);
