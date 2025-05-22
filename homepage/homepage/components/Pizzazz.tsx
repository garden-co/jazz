"use client";

import { addPizzazz } from "@unicorn-poo/pizzazz";
import { useSearchParams } from "next/navigation";
import { useEffect } from "react";

export function Pizzazz() {
  const searchParams = useSearchParams();

  const utmSource = searchParams.get("utm_source");
  useEffect(() => {
    if (utmSource === "zod") {
      addPizzazz(document?.body, {
        effectType: "fire",
        // character: '💎',
        count: 12,
        sizeRange: [10, 40],
        duration: 1500,
      });
    }
  }, []);

  return null;
}
