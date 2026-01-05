// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import type { SchemaMeta } from "@jazz/schema/runtime";

export const schemaMeta: SchemaMeta = {
  tables: {
    Users: {
      name: "Users",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "email", type: {"kind":"string"}, nullable: false },
        { name: "avatarColor", type: {"kind":"string"}, nullable: false },
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "IssueAssignees", sourceTable: "IssueAssignees", sourceColumn: "user" },
      ],
    },
    Projects: {
      name: "Projects",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "color", type: {"kind":"string"}, nullable: false },
        { name: "description", type: {"kind":"string"}, nullable: true },
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "Issues", sourceTable: "Issues", sourceColumn: "project" },
      ],
    },
    Issues: {
      name: "Issues",
      columns: [
        { name: "title", type: {"kind":"string"}, nullable: false },
        { name: "description", type: {"kind":"string"}, nullable: true },
        { name: "status", type: {"kind":"string"}, nullable: false },
        { name: "priority", type: {"kind":"string"}, nullable: false },
        { name: "project", type: {"kind":"ref","table":"Projects"}, nullable: false },
        { name: "createdAt", type: {"kind":"i64"}, nullable: false },
        { name: "updatedAt", type: {"kind":"i64"}, nullable: false },
      ],
      refs: [
        { column: "project", targetTable: "Projects", nullable: false },
      ],
      reverseRefs: [
        { name: "IssueLabels", sourceTable: "IssueLabels", sourceColumn: "issue" },
        { name: "IssueAssignees", sourceTable: "IssueAssignees", sourceColumn: "issue" },
      ],
    },
    Labels: {
      name: "Labels",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "color", type: {"kind":"string"}, nullable: false },
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "IssueLabels", sourceTable: "IssueLabels", sourceColumn: "label" },
      ],
    },
    IssueLabels: {
      name: "IssueLabels",
      columns: [
        { name: "issue", type: {"kind":"ref","table":"Issues"}, nullable: false },
        { name: "label", type: {"kind":"ref","table":"Labels"}, nullable: false },
      ],
      refs: [
        { column: "issue", targetTable: "Issues", nullable: false },
        { column: "label", targetTable: "Labels", nullable: false },
      ],
      reverseRefs: [
      ],
    },
    IssueAssignees: {
      name: "IssueAssignees",
      columns: [
        { name: "issue", type: {"kind":"ref","table":"Issues"}, nullable: false },
        { name: "user", type: {"kind":"ref","table":"Users"}, nullable: false },
      ],
      refs: [
        { column: "issue", targetTable: "Issues", nullable: false },
        { column: "user", targetTable: "Users", nullable: false },
      ],
      reverseRefs: [
      ],
    },
  },
};

// Individual table metadata exports
export const userMeta = schemaMeta.tables.Users;
export const projectMeta = schemaMeta.tables.Projects;
export const issueMeta = schemaMeta.tables.Issues;
export const labelMeta = schemaMeta.tables.Labels;
export const issuelabelMeta = schemaMeta.tables.IssueLabels;
export const issueassigneeMeta = schemaMeta.tables.IssueAssignees;
