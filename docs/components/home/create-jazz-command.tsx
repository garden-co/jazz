"use client";

import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";

const command = "npm create jazz";

/** The scaffold command with a copy button. */
export function CreateJazzCommand() {
  const [copied, setCopied] = useState(false);

  return (
    <div className="home-command">
      <code>
        <span aria-hidden className="home-command-prompt">
          ${" "}
        </span>
        {command}
      </code>
      <IconButton
        label={copied ? "Copied" : "Copy command"}
        variant="ghost"
        size="sm"
        icon={<Icon icon={copied ? Check : Copy} size="sm" />}
        onClick={() => {
          void navigator.clipboard.writeText(command).then(() => {
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
          });
        }}
      />
    </div>
  );
}
