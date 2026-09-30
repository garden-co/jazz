"use client";

import { useState } from "react";
import { useDb, useOne } from "jazz-tools/react";
import { Grid, HStack, Selector, Skeleton, TextInput, Token, VStack } from "@astryxdesign/core";
import { app, type IssuePriority, type IssueStatus } from "@/schema";
import {
  ISSUE_PRIORITIES,
  ISSUE_STATUSES,
  PRIORITY_LABELS,
  STATUS_LABELS,
} from "@/src/lib/issue-labels";
import { useWorkspace } from "./workspace-context";

const UNASSIGNED = "unassigned";

/** The database columns of an issue, shown above its page body. */
export function IssueProperties({ pageId, editable }: { pageId: string; editable: boolean }) {
  const db = useDb();
  const { members } = useWorkspace();
  const { data: issue } = useOne(app.issues.where({ pageId }));
  const [newLabel, setNewLabel] = useState("");
  if (issue === undefined) return <Skeleton height={64} width="100%" />;
  if (!issue) return null;

  const addLabel = () => {
    const label = newLabel.trim();
    if (!label || issue.labels.includes(label)) return;
    db.update(app.issues, issue.id, { labels: [...issue.labels, label] });
    setNewLabel("");
  };

  return (
    <VStack gap={3}>
      <Grid columns={{ minWidth: 180 }} gap={3}>
        <Selector
          label="Status"
          size="sm"
          value={issue.status}
          isReadOnly={!editable}
          options={ISSUE_STATUSES.map((status) => ({
            value: status,
            label: STATUS_LABELS[status],
          }))}
          onChange={(status) => db.update(app.issues, issue.id, { status: status as IssueStatus })}
        />
        <Selector
          label="Priority"
          size="sm"
          value={issue.priority}
          isReadOnly={!editable}
          options={ISSUE_PRIORITIES.map((priority) => ({
            value: priority,
            label: PRIORITY_LABELS[priority],
          }))}
          onChange={(priority) =>
            db.update(app.issues, issue.id, { priority: priority as IssuePriority })
          }
        />
        <Selector
          label="Assignee"
          size="sm"
          value={issue.assignee ?? UNASSIGNED}
          isReadOnly={!editable}
          options={[
            { value: UNASSIGNED, label: "Unassigned" },
            ...members.map((member) => ({ value: member.account, label: member.displayName })),
          ]}
          onChange={(assignee) =>
            db.update(app.issues, issue.id, { assignee: assignee === UNASSIGNED ? null : assignee })
          }
        />
      </Grid>
      <HStack gap={2} align="end" wrap="wrap">
        {issue.labels.map((label) => (
          <Token
            key={label}
            label={label}
            onRemove={
              editable
                ? () =>
                    db.update(app.issues, issue.id, {
                      labels: issue.labels.filter((l) => l !== label),
                    })
                : undefined
            }
          />
        ))}
        {editable && (
          <TextInput
            label="Add label"
            isLabelHidden
            size="sm"
            placeholder="Add label"
            value={newLabel}
            onChange={setNewLabel}
            onEnter={addLabel}
            width={160}
          />
        )}
      </HStack>
    </VStack>
  );
}
