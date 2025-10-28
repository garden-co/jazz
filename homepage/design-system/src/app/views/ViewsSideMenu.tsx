"use client";

import clsx from "clsx";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { Button } from "../../components/atoms/Button";

const designSystemTopics = [
  "Colors",
  "Typography",
  "Buttons",
  "Components",
  "Inputs",
  "Icons",
];

export function ViewsSideMenu() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const router = useRouter();
  return (
    <div className={clsx("sticky top-0", mobileMenuOpen ? "w-32" : "w-7")}>
      <Button
        intent="default"
        variant="link"
        icon={mobileMenuOpen ? "close" : "chevronRight"}
        onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
        size="sm"
      />
      {mobileMenuOpen && (
        <div className="flex flex-col gap-2">
          {designSystemTopics.map((topic) => (
            <div key={topic}>
              <Button
                intent="default"
                variant="link"
                onClick={() => router.push(`/views/${topic.toLowerCase()}`)}
              >
                {topic}
              </Button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
