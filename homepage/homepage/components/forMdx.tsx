import {
  CodeExampleTabs as CodeExampleTabsClient,
  CodeExampleTabsProps,
} from "@/components/examples/CodeExampleTabs";

import {
  ContentByFramework as ContentByFrameworkClient,
  ContentByFrameworkProps,
} from "@/components/docs/ContentByFramework";
import { CodeGroup as CodeGroupClient } from "gcmp-design-system/src/app/components/molecules/CodeGroup";
import { ComingSoon as ComingSoonClient } from "./docs/ComingSoon";
import { IssueTrackerPreview as IssueTrackerPreviewClient } from "./docs/IssueTrackerPreview";
import {
  PackageUpgrade as PackageUpgradeClient,
  PackageUpgradeProps,
} from "./docs/PackageUpgrade";

export function CodeExampleTabs(props: CodeExampleTabsProps) {
  return <CodeExampleTabsClient {...props} />;
}

export function CodeGroup(props: { children: React.ReactNode }) {
  return <CodeGroupClient {...props}></CodeGroupClient>;
}

export function ComingSoon() {
  return <ComingSoonClient />;
}

export function ContentByFramework(props: ContentByFrameworkProps) {
  return <ContentByFrameworkClient {...props}></ContentByFrameworkClient>;
}

export function IssueTrackerPreview() {
  return <IssueTrackerPreviewClient />;
}

export function PackageUpgrade(props: PackageUpgradeProps) {
  return <PackageUpgradeClient {...props} />;
}
