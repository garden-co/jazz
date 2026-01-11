import { app } from "@/generated/client";
import type { ObjectId } from "@/generated/types";
import type { WasmDatabaseLike } from "@jazz/react";
import {
  ISSUE_TITLES,
  LABEL_DATA,
  PRIORITIES,
  PROJECT_DATA,
  STATUSES,
  USER_COLORS,
  USER_NAMES,
} from "./constants";

export async function generateFakeData(
  db: WasmDatabaseLike,
  issueCount: number,
): Promise<ObjectId> {
  // 1. Create Users
  const userIds: ObjectId[] = [];
  for (let i = 0; i < USER_NAMES.length; i++) {
    const name = USER_NAMES[i];
    const id = app.users.create(db, {
      name,
      email: `${name.toLowerCase().replace(" ", ".")}@example.com`,
      avatarColor: USER_COLORS[i],
    });
    userIds.push(id);
  }

  // 2. Create Projects
  const projectIds: ObjectId[] = [];
  for (const proj of PROJECT_DATA) {
    const id = app.projects.create(db, proj);
    projectIds.push(id);
  }

  // 3. Create Labels
  const labelIds: ObjectId[] = [];
  for (const label of LABEL_DATA) {
    const id = app.labels.create(db, label);
    labelIds.push(id);
  }

  // 4. Create Issues
  const issueIds: ObjectId[] = [];
  for (let i = 0; i < issueCount; i++) {
    const now = BigInt(
      Date.now() - Math.floor(Math.random() * 7 * 24 * 60 * 60 * 1000),
    );
    const id = app.issues.create(db, {
      title:
        ISSUE_TITLES[i % ISSUE_TITLES.length] +
        (i >= ISSUE_TITLES.length ? ` (#${i + 1})` : ""),
      description: `Description for issue ${i + 1}. This is a sample issue created for testing purposes.`,
      status: STATUSES[Math.floor(Math.random() * STATUSES.length)],
      priority: PRIORITIES[Math.floor(Math.random() * PRIORITIES.length)],
      project: projectIds[Math.floor(Math.random() * projectIds.length)],
      createdAt: now,
      updatedAt: now,
    });
    issueIds.push(id);
  }

  // 5. Create IssueLabels (1-3 random labels per issue)
  for (const issueId of issueIds) {
    const labelCount = 1 + Math.floor(Math.random() * 3);
    const shuffledLabels = [...labelIds].sort(() => Math.random() - 0.5);

    for (let i = 0; i < labelCount && i < shuffledLabels.length; i++) {
      app.issuelabels.create(db, {
        issue: issueId,
        label: shuffledLabels[i],
      });
    }
  }

  // 6. Create IssueAssignees (0-2 random assignees per issue)
  for (const issueId of issueIds) {
    const assigneeCount = Math.floor(Math.random() * 3); // 0, 1, or 2
    const shuffledUsers = [...userIds].sort(() => Math.random() - 0.5);

    for (let i = 0; i < assigneeCount && i < shuffledUsers.length; i++) {
      app.issueassignees.create(db, {
        issue: issueId,
        user: shuffledUsers[i],
      });
    }
  }

  // Return a random user as the "current user"
  return userIds[Math.floor(Math.random() * userIds.length)];
}
