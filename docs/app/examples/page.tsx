import type { Metadata } from "next";
import { Showcase } from "@/components/showcase/showcase";

export const metadata: Metadata = {
  title: "Jazz · Examples & benchmarks",
  description:
    "Example apps built with Jazz, what they do, and how fast they are: benchmark results from every release.",
};

export default function Page() {
  return <Showcase />;
}
