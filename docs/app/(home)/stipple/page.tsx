import type { Metadata } from "next";
import { StipplePlayground } from "@/components/brand/stipple-playground";

export const metadata: Metadata = {
  title: "Stipple patterns",
  robots: { index: false, follow: false },
};

export default function StipplePage() {
  return <StipplePlayground />;
}
