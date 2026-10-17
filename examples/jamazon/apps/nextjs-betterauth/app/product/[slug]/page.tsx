"use client";

import { use } from "react";
import { ProductDetail } from "@/src/components/ProductDetail";

export default function ProductPage({ params }: { params: Promise<{ slug: string }> }) {
  return <ProductDetail slug={use(params).slug} />;
}
