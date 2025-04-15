import React from "react";

export { JazzInspector } from "./viewer/new-app.js";
export { PageStack } from "./viewer/page-stack.js";
export { Breadcrumbs } from "./viewer/breadcrumbs.js";
export { AccountNameDisplay } from "./viewer/account-name-display.js";

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

setup(React.createElement);
