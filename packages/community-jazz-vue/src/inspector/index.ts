// Vue Inspector exports - Canonical Vue Implementation

// Types
export type { PageInfo } from "./composables/useJazzInspector.js";
export type { ResolvedCoValue } from "./composables/useCoValueResolver.js";

// Composables (Canonical Vue)
export { useJazzInspector } from "./composables/useJazzInspector.js";
export { useCoValueResolver } from "./composables/useCoValueResolver.js";

// Main Inspector Components (Canonical Vue)
export { default as JazzInspector } from "./JazzInspector.vue";
export { default as VueJazzInspector } from "./components/VueJazzInspector.vue";
export { default as InspectorPanel } from "./components/InspectorPanel.vue";
export { InspectorButton } from "./viewer/inspector-button.js"; // Use JSX version that matches React
export { default as CoValueViewer } from "./components/CoValueViewer.vue";
export { default as NavigationBreadcrumbs } from "./components/NavigationBreadcrumbs.vue";

// Legacy JSX Components (deprecated - use Vue components above)
export {
  JazzInspector as JazzInspectorJSX,
  JazzInspectorInternal,
} from "./viewer/new-app.js";
export { PageStack } from "./viewer/page-stack.js";
export { Breadcrumbs } from "./viewer/breadcrumbs.js";
export { AccountOrGroupText } from "./viewer/account-or-group-text.js";
// Note: InspectorButton is now the main export (JSX version that matches React)

// UI Components
export { Button } from "./ui/button.js";
export { Input } from "./ui/input.js";
export { Select } from "./ui/select.js";
export { Icon } from "./ui/icon.js";
export { GlobalStyles } from "./ui/global-styles.js";

export { Badge } from "./ui/badge.js";
export { Card, CardBody, CardHeader } from "./ui/card.js";
export { Grid } from "./ui/grid.js";
export { Heading } from "./ui/heading.js";
export { Text } from "./ui/text.js";
export {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table.js";
