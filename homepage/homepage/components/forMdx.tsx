import { Alert as AlertClient, AlertProps } from '@garden-co/design-system/src/components/atoms/Alert';
import {
  ContentByFramework as ContentByFrameworkClient,
  ContentByFrameworkProps,
} from "@/components/docs/ContentByFramework";
import { JazzLogo as JazzLogoClient } from "@garden-co/design-system/src/components/atoms/logos/JazzLogo";
import { ReactLogo as ReactLogoClient } from "./icons/ReactLogo";
import { SvelteLogo as SvelteLogoClient } from "./icons/SvelteLogo";
import { JavascriptLogo as VanillaLogoClient } from "./icons/JavascriptLogo";
import { ReactNativeLogo as RNLogoClient } from "./icons/ReactNativeLogo";
import { ExpoLogo as ExpoLogoClient } from "./icons/ExpoLogo";
import { CodeGroup as CodeGroupClient } from "@garden-co/design-system/src/components/molecules/CodeGroup";
import { TabbedCodeGroup as TabbedCodeGroupClient, TabbedCodeGroupItem as TabbedCodeGroupItemClient } from "@garden-co/design-system/src/components/molecules/TabbedCodeGroup";
import { AnchorHTMLAttributes, DetailedHTMLProps } from "react";
import { FileDownloadLink as FileDownloadLinkClient } from "./FileDownloadLink";
import { Framework as FrameworkClient } from "./docs/Framework";
import { IssueTrackerPreview as IssueTrackerPreviewClient } from "./docs/IssueTrackerPreview";
import { FileName as FileNameClient } from "./docs/FileName";
import { JazzIcon as JazzIconClient } from "@garden-co/design-system/src/components/atoms/logos/JazzIcon"


export function Alert(props: AlertProps) {
  return <AlertClient {...props}></AlertClient>;
}

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

export function ReactLogo(props: { className?: string }) {
  return <ReactLogoClient {...props} />;
}
export function SvelteLogo(props: { className?: string }) {
  return <SvelteLogoClient {...props} />;
}
export function VanillaLogo(props: { className?: string }) {
  return <VanillaLogoClient {...props} />;
}
export function RNLogo(props: { className?: string }) {
  return <RNLogoClient {...props} />;
}
export function ExpoLogo(props: { className?: string }) {
  return <ExpoLogoClient {...props} />;
}
export function JazzIcon(props: { className?: string }) {
  return <JazzIconClient {...props} />;
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