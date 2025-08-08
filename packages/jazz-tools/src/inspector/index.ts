import React from "react";

export { Button } from "./ui/button.js";
export { GlobalStyles } from "./ui/global-styles.js";
export { Icon } from "./ui/icon.js";
export { Input } from "./ui/input.js";
export { Select } from "./ui/select.js";
export { AccountOrGroupText } from "./viewer/account-or-group-text.js";
export { Breadcrumbs } from "./viewer/breadcrumbs.js";
export { JazzInspector, JazzInspectorInternal } from "./viewer/new-app.js";
export { PageStack } from "./viewer/page-stack.js";
export type { PageInfo } from "./viewer/types.js";
export {
  resolveCoValue,
  useResolvedCoValue,
} from "./viewer/use-resolve-covalue.js";

import { setup } from "goober";

setup(React.createElement);
