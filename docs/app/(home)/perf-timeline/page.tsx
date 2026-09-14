import type { Metadata } from "next";
import "./style.css";
import { Dashboard } from "./timeline";

export const metadata: Metadata = {
  title: "Jazz · Performance timeline",
  description:
    "Public wallclock benchmark history across Jazz releases, main commits, and open performance experiments.",
  robots: { index: false, follow: false },
};

export default function Page() {
  return <Dashboard />;
}
