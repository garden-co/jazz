"use client";

import { useState } from "react";
import { Check, Copy } from "lucide-react";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";

const placeholder = "[describe your project first, or drop into an ongoing chat]";

/**
 * The agent quickstart, shown in full so people see what they hand their
 * agent, with a button that copies it.
 */
export function AgentPrompt({ quickstart, display }: { quickstart: string; display: string }) {
  const [copied, setCopied] = useState(false);

  return (
    <div className="home-agent-prompt-wrap">
      <p className="home-agent-prompt-placeholder">{placeholder}</p>
      <div className="home-agent-prompt">
        <pre tabIndex={0} aria-label="Agent quickstart">
          {display}
        </pre>
        <div className="home-agent-prompt-copy">
          <IconButton
            label={copied ? "Copied" : "Copy quickstart"}
            variant="ghost"
            size="sm"
            icon={<Icon icon={copied ? Check : Copy} size="sm" />}
            onClick={() => {
              void navigator.clipboard.writeText(quickstart).then(() => {
                setCopied(true);
                window.setTimeout(() => setCopied(false), 1500);
              });
            }}
          />
        </div>
      </div>
    </div>
  );
}
