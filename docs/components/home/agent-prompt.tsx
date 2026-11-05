"use client";

import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";

const instruction = "Read https://jazz.tools/quickstart.md and follow it to build with Jazz.";
const placeholder = "[describe your project here or drop into ongoing chat]";

/** The prompt to paste into a coding agent, with a copy button. */
export function AgentPrompt() {
  const [copied, setCopied] = useState(false);

  return (
    <div className="home-agent-prompt">
      <p>
        {instruction}
        <br />
        <br />
        <span className="home-agent-prompt-placeholder">{placeholder}</span>
      </p>
      <IconButton
        label={copied ? "Copied" : "Copy prompt"}
        variant="ghost"
        size="sm"
        icon={<Icon icon={copied ? Check : Copy} size="sm" />}
        onClick={() => {
          void navigator.clipboard.writeText(`${instruction}\n\n${placeholder}`).then(() => {
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
          });
        }}
      />
    </div>
  );
}
