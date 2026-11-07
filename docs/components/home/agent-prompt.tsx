"use client";

import { Fragment, type ReactNode, useState } from "react";
import { track } from "@vercel/analytics";
import { Check, Copy } from "lucide-react";
import { Icon } from "@astryxdesign/core/Icon";
import { Button } from "@astryxdesign/core/Button";
import { copyEvent } from "@/lib/copy-analytics";

const placeholder = "[describe your project first, or drop into an ongoing chat]";

/** Inline Markdown: `code`, **bold**, [links](url) and /docs paths. */
const INLINE =
  /(`[^`\n]+`)|(\*\*[^*\n]+\*\*)|(\[[^\]\n]+\]\([^)\n]+\))|(\/docs\/[\w\-/#.]*[\w\-/#])/g;

function inline(text: string, key: string): ReactNode[] {
  const out: ReactNode[] = [];
  let last = 0;
  for (const match of text.matchAll(INLINE)) {
    const index = match.index ?? 0;
    if (index > last) out.push(text.slice(last, index));
    const [token] = match;
    const k = `${key}-${index}`;
    if (match[1]) {
      out.push(
        <span key={k} className="md-code">
          {token}
        </span>,
      );
    } else if (match[2]) {
      out.push(
        <span key={k} className="md-strong">
          {token}
        </span>,
      );
    } else if (match[4]) {
      out.push(
        <span key={k} className="md-url">
          {token}
        </span>,
      );
    } else {
      const split = token.indexOf("](");
      out.push(
        <span key={k}>
          <span className="md-link">{token.slice(0, split + 1)}</span>
          <span className="md-url">{token.slice(split + 1)}</span>
        </span>,
      );
    }
    last = index + token.length;
  }
  if (last < text.length) out.push(text.slice(last));
  return out;
}

/** Light Markdown highlighting: headings, list markers and inline marks. */
function highlightMarkdown(source: string): ReactNode {
  return source.split("\n").map((line, i) => {
    const key = String(i);
    const nl = i > 0 ? "\n" : "";
    if (/^#{1,6} /.test(line)) {
      return (
        <Fragment key={key}>
          {nl}
          <span className="md-heading">{line}</span>
        </Fragment>
      );
    }
    const list = /^(\s*)(\d+\.|[-*]) /.exec(line);
    if (list) {
      const [marker, indent, bullet] = list;
      return (
        <Fragment key={key}>
          {nl}
          {indent}
          <span className="md-marker">{bullet}</span> {inline(line.slice(marker.length), key)}
        </Fragment>
      );
    }
    return (
      <Fragment key={key}>
        {nl}
        {inline(line, key)}
      </Fragment>
    );
  });
}

/**
 * The agent quickstart, shown in full so people see what they hand their
 * agent, with a button that copies it.
 */
export function AgentPrompt({ quickstart, display }: { quickstart: string; display: string }) {
  const [copied, setCopied] = useState(false);

  return (
    <div className="home-agent-prompt-wrap">
      <div className="home-agent-prompt-bar">
        <p className="home-agent-prompt-placeholder">{placeholder}</p>
        <div className="home-agent-prompt-copy">
          <Button
            label={copied ? "Copied" : "Copy"}
            aria-label={copied ? "Copied" : "Copy quickstart"}
            variant="primary"
            size="sm"
            icon={<Icon icon={copied ? Check : Copy} size="sm" />}
            onClick={() => {
              void navigator.clipboard.writeText(quickstart).then(() => {
                const event = copyEvent("quickstart", "button");
                track(event.name, event.properties);
                setCopied(true);
                window.setTimeout(() => setCopied(false), 1500);
              });
            }}
          />
        </div>
      </div>
      <div className="home-agent-prompt">
        <pre tabIndex={0} aria-label="Agent quickstart" data-agent-quickstart="">
          {highlightMarkdown(display)}
        </pre>
      </div>
    </div>
  );
}
