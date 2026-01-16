"use client";

import { useState } from "react";
import { track } from "@vercel/analytics";
import { Button } from "@garden-co/design-system/src/components/atoms/Button";
import { usePathname } from "next/navigation";

export function FeedbackAffordances() {
  const [option, setOption] = useState<"helpful" | "not_helpful" | null>(null);
  const pathname = usePathname();

  const handleClick = (type: "helpful" | "not_helpful") => {
    // If the user is changing their feedback, let them, otherwise block them from voting again
    if (option === type) return;
    setOption(type);
    track("Docs feedback", { feedback: type, page: pathname });
  };

  return (
    <div className="flex flex-row justify-start flex-wrap items-center gap-2 py-2">
      <strong>Was this page helpful?</strong>
      <div>
        <Button
          size="sm"
          icon="check"
          variant={option === "helpful" ? "inverted" : "default"}
          onClick={() => handleClick("helpful")}
          className="rounded-r-none"
        >
          Yes
        </Button>
        <Button
          size="sm"
          icon="close"
          variant={option === "not_helpful" ? "inverted" : "default"}
          onClick={() => handleClick("not_helpful")}
          className="rounded-l-none"
        >
          No
        </Button>
      </div>
    </div>
  );
}
