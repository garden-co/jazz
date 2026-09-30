import { SiteShell } from "@/components/site/site-shell";

export default function Layout({ children }: LayoutProps<"/">) {
  return <SiteShell>{children}</SiteShell>;
}
