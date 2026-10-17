import { SiteShell } from "@/components/site/site-shell";
import { ExamplesSideNav } from "@/components/showcase/examples-side-nav";

export default function Layout({ children }: LayoutProps<"/examples">) {
  return <SiteShell sideNav={<ExamplesSideNav />}>{children}</SiteShell>;
}
