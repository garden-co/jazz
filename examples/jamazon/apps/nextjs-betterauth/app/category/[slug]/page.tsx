"use client";

import { use } from "react";
import { Catalogue } from "@/src/components/Catalogue";

export default function CategoryPage({ params }: { params: Promise<{ slug: string }> }) {
  return <Catalogue categorySlug={use(params).slug} />;
}
