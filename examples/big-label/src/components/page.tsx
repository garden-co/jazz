"use client";

import type { ReactNode } from "react";
import {
  BreadcrumbItem,
  Breadcrumbs,
  Card,
  HStack,
  Heading,
  Text,
  VStack,
} from "@astryxdesign/core";

/** Title row shared by every page: optional breadcrumbs, heading, actions. */
export function PageHeader({
  title,
  description,
  parent,
  actions,
}: {
  title: string;
  description?: ReactNode;
  parent?: { label: string; href: string };
  actions?: ReactNode;
}) {
  return (
    <VStack gap={2}>
      {parent && (
        <Breadcrumbs>
          <BreadcrumbItem href={parent.href}>{parent.label}</BreadcrumbItem>
          <BreadcrumbItem isCurrent>{title}</BreadcrumbItem>
        </Breadcrumbs>
      )}
      <HStack gap={3} vAlign="center" wrap="wrap" justify="between">
        <Heading level={1}>{title}</Heading>
        {actions && (
          <HStack gap={2} wrap="wrap">
            {actions}
          </HStack>
        )}
      </HStack>
      {description && <Text color="secondary">{description}</Text>}
    </VStack>
  );
}

/** A big-number metric for the overview. */
export function Stat({ label, value, note }: { label: string; value: ReactNode; note?: string }) {
  return (
    <Card padding={4}>
      <VStack gap={1}>
        <Text type="supporting" color="secondary">
          {label}
        </Text>
        <Heading level={2} type="display-3">
          {value}
        </Heading>
        {note && (
          <Text type="supporting" color="secondary">
            {note}
          </Text>
        )}
      </VStack>
    </Card>
  );
}

/** A titled page section; spacing comes from the parent stack. */
export function PageSection({
  title,
  actions,
  children,
}: {
  title: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <VStack gap={3} as="section">
      <HStack gap={3} vAlign="center" wrap="wrap" justify="between">
        <Heading level={2}>{title}</Heading>
        {actions}
      </HStack>
      {children}
    </VStack>
  );
}
