import {
  CodeExampleTabs as CodeExampleTabsClient,
  CodeExampleTabsProps,
} from "@/components/CodeExampleTabs";

import { CodeGroup as CodeGroupClient } from "gcmp-design-system/src/app/components/molecules/CodeGroup";

export function CodeExampleTabs(props: CodeExampleTabsProps) {
  return <CodeExampleTabsClient {...props} />;
}

export function CodeGroup(props: { children: React.ReactNode }) {
  return <CodeGroupClient {...props}></CodeGroupClient>;
}
