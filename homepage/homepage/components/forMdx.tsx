import {
  ContentByFramework as ContentByFrameworkClient,
  ContentByFrameworkProps,
} from "@/components/docs/ContentByFramework";
import { JazzLogo as JazzLogoClient } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import { CodeGroup as CodeGroupClient } from "@garden-co/design-system/src/components/molecules/CodeGroup";
import { TabbedCodeGroup as TabbedCodeGroupClient, TabbedCodeGroupItem as TabbedCodeGroupItemClient } from "@garden-co/design-system/src/components/molecules/TabbedCodeGroup";
import { AnchorHTMLAttributes, DetailedHTMLProps } from "react";
import { FileDownloadLink as FileDownloadLinkClient } from "./FileDownloadLink";
import { Framework as FrameworkClient } from "./docs/Framework";
import { IssueTrackerPreview as IssueTrackerPreviewClient } from "./docs/IssueTrackerPreview";
import { FileName as FileNameClient } from "./docs/FileName";


export function CodeGroup(props: { children: React.ReactNode }) {
  return <CodeGroupClient {...props}></CodeGroupClient>;
}

export function TabbedCodeGroup(props: { children: React.ReactNode; default?: string }) {
  return <TabbedCodeGroupClient {...props}></TabbedCodeGroupClient>;
}

export function TabbedCodeGroupItem(props: { children: React.ReactNode; label: string }) {
  return <TabbedCodeGroupItemClient {...props}></TabbedCodeGroupItemClient>;
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

export function Framework() {
  return <FrameworkClient />;
}

export function FileName(
  props: { children: React.ReactNode },
) {
  return <FileNameClient {...props} />;
}