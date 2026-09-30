import type { ReactNode } from "react";
import { HStack, VStack } from "@astryxdesign/core/Stack";
import { Heading, Text } from "@astryxdesign/core/Text";

type PageProps = {
  title: string;
  description?: ReactNode;
  actions?: ReactNode;
  children: ReactNode;
};

/** A page body: title row with actions, then the content. */
export function Page({ title, description, actions, children }: PageProps) {
  return (
    <VStack gap={6} paddingBlock={6} paddingInline={4} className="page">
      <HStack gap={3} vAlign="center" justify="between" wrap="wrap">
        <VStack gap={1}>
          <Heading level={1}>{title}</Heading>
          {description && <Text color="secondary">{description}</Text>}
        </VStack>
        {actions}
      </HStack>
      {children}
    </VStack>
  );
}
