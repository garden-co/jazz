"use client";

import { useState } from "react";
import { HStack } from "@astryxdesign/core/HStack";
import { Icon } from "@astryxdesign/core/Icon";
import { IconButton } from "@astryxdesign/core/IconButton";
import { Text } from "@astryxdesign/core/Text";
import { VStack } from "@astryxdesign/core/VStack";

/**
 * Shows the signed-in Jazz account ID, which a session creator pastes into
 * the members dialog to add this person.
 */
export function AccountId({ accountId }: { accountId: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    await navigator.clipboard.writeText(accountId);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  return (
    <VStack gap={1}>
      <Text type="label">Your account ID</Text>
      <HStack gap={1} align="center">
        <Text type="code" data-testid="member-id" wordBreak="break-all">
          {accountId}
        </Text>
        <IconButton
          variant="ghost"
          size="sm"
          label={copied ? "Copied" : "Copy account ID"}
          tooltip={copied ? "Copied" : "Copy account ID"}
          icon={<Icon icon={copied ? "check" : "copy"} size="sm" />}
          clickAction={copy}
        />
      </HStack>
      <Text type="supporting">Send it to a session creator so they can add you as a bandmate.</Text>
    </VStack>
  );
}
