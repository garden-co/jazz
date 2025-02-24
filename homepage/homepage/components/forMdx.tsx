import {
  CodeExampleTabs as CodeExampleTabsClient,
  CodeExampleTabsProps,
} from "@/components/examples/CodeExampleTabs";

import {
  ContentByFramework as ContentByFrameworkClient,
  ContentByFrameworkProps,
} from "@/components/docs/ContentByFramework";
import { JazzLogo as JazzLogoClient } from "gcmp-design-system/src/app/components/atoms/logos/JazzLogo";
import { CodeGroup as CodeGroupClient } from "gcmp-design-system/src/app/components/molecules/CodeGroup";
import { AnchorHTMLAttributes, DetailedHTMLProps } from "react";
import { FileDownloadLink as FileDownloadLinkClient } from "./FileDownloadLink";
import { ComingSoon as ComingSoonClient } from "./docs/ComingSoon";
import { IssueTrackerPreview as IssueTrackerPreviewClient } from "./docs/IssueTrackerPreview";

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

export function JazzLogo(props: { className?: string }) {
  return <JazzLogoClient {...props} />;
}

export function FileDownloadLink(
  props: DetailedHTMLProps<
    AnchorHTMLAttributes<HTMLAnchorElement>,
    HTMLAnchorElement
  >,
) {
  return <FileDownloadLinkClient {...props} />;
}
