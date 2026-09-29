"use client";

import { useState } from "react";
import { useAll, useDb } from "jazz-tools/react";
import {
  Avatar,
  Badge,
  Button,
  ClickableCard,
  EmptyState,
  Grid,
  HStack,
  Link,
  MoreMenu,
  Skeleton,
  Tab,
  Table,
  TabList,
  Text,
  Token,
  Toolbar,
  VStack,
  type TableColumn,
} from "@astryxdesign/core";
import { Plus } from "lucide-react";
import { app, type Issue, type IssueStatus } from "@/schema";
import { createIssue } from "@/src/lib/page-actions";
import {
  ISSUE_STATUSES,
  PRIORITY_LABELS,
  STATUS_BADGES,
  STATUS_LABELS,
} from "@/src/lib/issue-labels";
import { memberName, useWorkspace } from "./workspace-context";

type IssueRow = Issue & { title: string; $createdAt?: Date | number | null };

/**
 * A database page: every issue is a row here and a page of its own. The table
 * and the board are two views over the same live query.
 */
export function IssuesDatabase({
  databaseId,
  editable,
}: {
  databaseId: string;
  editable: boolean;
}) {
  const db = useDb();
  const { workspace, tree, members, me, openPage } = useWorkspace();
  const [view, setView] = useState<"table" | "board">("table");
  // Newest first. Titles come from the issue pages the workspace already holds.
  const { data } = useAll(
    app.issues.where({ databaseId }).select("*", "$createdAt").orderBy("$createdAt", "desc"),
  );

  const rows: IssueRow[] = (data ?? []).flatMap((issue) => {
    const page = tree.byId.get(issue.pageId);
    return page ? [{ ...issue, title: page.title }] : [];
  });

  const addIssue = (status: IssueStatus) => {
    const page = createIssue(db, {
      workspaceId: workspace.id,
      databaseId,
      title: "",
      status,
      assignee: members.some((member) => member.account === me) ? me : null,
    });
    openPage(page.id);
  };

  const setStatus = (issue: IssueRow, status: IssueStatus) =>
    db.update(app.issues, issue.id, { status });

  const statusMenu = (issue: IssueRow) =>
    ISSUE_STATUSES.filter((status) => status !== issue.status).map((status) => ({
      label: `Move to ${STATUS_LABELS[status].toLowerCase()}`,
      onClick: () => setStatus(issue, status),
    }));

  const columns: TableColumn<IssueRow>[] = [
    {
      key: "title",
      header: "Title",
      renderCell: (issue) => (
        <Link onClick={() => openPage(issue.pageId)} hasUnderline={false} weight="medium">
          {issue.title || "Untitled"}
        </Link>
      ),
    },
    {
      key: "status",
      header: "Status",
      renderCell: (issue) => (
        <Badge variant={STATUS_BADGES[issue.status]} label={STATUS_LABELS[issue.status]} />
      ),
    },
    {
      key: "assignee",
      header: "Assignee",
      renderCell: (issue) => <Assignee account={issue.assignee} />,
    },
    { key: "priority", header: "Priority", renderCell: (issue) => PRIORITY_LABELS[issue.priority] },
    {
      key: "labels",
      header: "Labels",
      renderCell: (issue) => (
        <HStack gap={1} wrap="wrap">
          {issue.labels.map((label) => (
            <Token key={label} label={label} size="sm" />
          ))}
        </HStack>
      ),
    },
  ];

  return (
    <VStack gap={4}>
      <Toolbar
        label="Issue views"
        startContent={
          <TabList value={view} onChange={(value) => setView(value as "table" | "board")} size="sm">
            <Tab value="table" label="Table" />
            <Tab value="board" label="Board" />
          </TabList>
        }
        endContent={
          editable ? (
            <Button
              label="New issue"
              size="sm"
              variant="primary"
              icon={<Plus aria-hidden size="1em" />}
              onClick={() => addIssue("todo")}
            />
          ) : undefined
        }
      />
      {!data ? (
        <VStack gap={2}>
          <Skeleton height={32} width="100%" />
          <Skeleton height={32} width="100%" />
        </VStack>
      ) : rows.length === 0 ? (
        <EmptyState
          title="No issues"
          description="Track gear, logistics and songwriting to-dos here."
          actions={
            editable ? <Button label="New issue" onClick={() => addIssue("todo")} /> : undefined
          }
        />
      ) : view === "table" ? (
        <Table data={rows} columns={columns} idKey="id" density="compact" hasHover />
      ) : (
        <Grid columns={{ minWidth: 220 }} gap={4} align="start">
          {ISSUE_STATUSES.map((status) => {
            const inColumn = rows.filter((issue) => issue.status === status);
            return (
              <VStack key={status} gap={2} as="section" aria-label={STATUS_LABELS[status]}>
                <HStack gap={2} align="center" justify="between">
                  <HStack gap={2} align="center">
                    <Text type="label">{STATUS_LABELS[status]}</Text>
                    <Text type="supporting">{inColumn.length}</Text>
                  </HStack>
                  {editable && (
                    <Button
                      label={`New ${STATUS_LABELS[status].toLowerCase()} issue`}
                      isIconOnly
                      size="sm"
                      variant="ghost"
                      icon={<Plus aria-hidden size="1em" />}
                      onClick={() => addIssue(status)}
                    />
                  )}
                </HStack>
                {inColumn.map((issue) => (
                  <ClickableCard
                    key={issue.id}
                    label={issue.title || "Untitled"}
                    onClick={() => openPage(issue.pageId)}
                    padding={3}
                  >
                    <VStack gap={2}>
                      <HStack gap={1} justify="between" align="start">
                        <Text weight="medium">{issue.title || "Untitled"}</Text>
                        {editable && (
                          <MoreMenu
                            label="Change status"
                            size="sm"
                            alignment="end"
                            items={statusMenu(issue)}
                          />
                        )}
                      </HStack>
                      <HStack gap={1} align="center" wrap="wrap">
                        {issue.priority !== "none" && (
                          <Badge label={PRIORITY_LABELS[issue.priority]} variant="neutral" />
                        )}
                        {issue.labels.map((label) => (
                          <Token key={label} label={label} size="sm" />
                        ))}
                      </HStack>
                      <Assignee account={issue.assignee} />
                    </VStack>
                  </ClickableCard>
                ))}
              </VStack>
            );
          })}
        </Grid>
      )}
    </VStack>
  );
}

function Assignee({ account }: { account: string | null }) {
  const { members } = useWorkspace();
  const name = memberName(members, account);
  if (!account) return <Text type="supporting">Unassigned</Text>;
  return (
    <HStack gap={2} align="center">
      <Avatar name={name} size="xsm" tooltip={false} />
      <Text type="supporting">{name}</Text>
    </HStack>
  );
}
