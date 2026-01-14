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
        { name: "avatar", type: {"kind":"string"}, nullable: true },
        { name: "age", type: {"kind":"i64"}, nullable: false },
        { name: "score", type: {"kind":"f64"}, nullable: false },
        { name: "isAdmin", type: {"kind":"bool"}, nullable: false },
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "Projects", sourceTable: "Projects", sourceColumn: "owner" },
        { name: "Tasks", sourceTable: "Tasks", sourceColumn: "assignee" },
        { name: "Comments", sourceTable: "Comments", sourceColumn: "author" },
      ],
    },
    Projects: {
      name: "Projects",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "description", type: {"kind":"string"}, nullable: true },
        { name: "owner", type: {"kind":"ref","table":"Users"}, nullable: false },
        { name: "color", type: {"kind":"string"}, nullable: false },
      ],
      refs: [
        { column: "owner", targetTable: "Users", nullable: false },
      ],
      reverseRefs: [
        { name: "Tasks", sourceTable: "Tasks", sourceColumn: "project" },
      ],
    },
    Tasks: {
      name: "Tasks",
      columns: [
        { name: "title", type: {"kind":"string"}, nullable: false },
        { name: "description", type: {"kind":"string"}, nullable: true },
        { name: "status", type: {"kind":"string"}, nullable: false },
        { name: "priority", type: {"kind":"string"}, nullable: false },
        { name: "project", type: {"kind":"ref","table":"Projects"}, nullable: false },
        { name: "assignee", type: {"kind":"ref","table":"Users"}, nullable: true },
        { name: "createdAt", type: {"kind":"i64"}, nullable: false },
        { name: "updatedAt", type: {"kind":"i64"}, nullable: false },
        { name: "isCompleted", type: {"kind":"bool"}, nullable: false },
      ],
      refs: [
        { column: "project", targetTable: "Projects", nullable: false },
        { column: "assignee", targetTable: "Users", nullable: true },
      ],
      reverseRefs: [
        { name: "TaskTags", sourceTable: "TaskTags", sourceColumn: "task" },
        { name: "Comments", sourceTable: "Comments", sourceColumn: "task" },
      ],
    },
    Tags: {
      name: "Tags",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "color", type: {"kind":"string"}, nullable: false },
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "TaskTags", sourceTable: "TaskTags", sourceColumn: "tag" },
      ],
    },
    TaskTags: {
      name: "TaskTags",
      columns: [
        { name: "task", type: {"kind":"ref","table":"Tasks"}, nullable: false },
        { name: "tag", type: {"kind":"ref","table":"Tags"}, nullable: false },
      ],
      refs: [
        { column: "task", targetTable: "Tasks", nullable: false },
        { column: "tag", targetTable: "Tags", nullable: false },
      ],
      reverseRefs: [
      ],
    },
    Categories: {
      name: "Categories",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "parent", type: {"kind":"ref","table":"Categories"}, nullable: true },
      ],
      refs: [
        { column: "parent", targetTable: "Categories", nullable: true },
      ],
      reverseRefs: [
        { name: "Categories", sourceTable: "Categories", sourceColumn: "parent" },
      ],
    },
    Comments: {
      name: "Comments",
      columns: [
        { name: "content", type: {"kind":"string"}, nullable: false },
        { name: "author", type: {"kind":"ref","table":"Users"}, nullable: false },
        { name: "task", type: {"kind":"ref","table":"Tasks"}, nullable: true },
        { name: "parentComment", type: {"kind":"ref","table":"Comments"}, nullable: true },
        { name: "createdAt", type: {"kind":"i64"}, nullable: false },
      ],
      refs: [
        { column: "author", targetTable: "Users", nullable: false },
        { column: "task", targetTable: "Tasks", nullable: true },
        { column: "parentComment", targetTable: "Comments", nullable: true },
      ],
      reverseRefs: [
        { name: "Comments", sourceTable: "Comments", sourceColumn: "parentComment" },
      ],
    },
  },
};

// Individual table metadata exports
export const userMeta = schemaMeta.tables.Users;
export const projectMeta = schemaMeta.tables.Projects;
export const taskMeta = schemaMeta.tables.Tasks;
export const tagMeta = schemaMeta.tables.Tags;
export const tasktagMeta = schemaMeta.tables.TaskTags;
export const categoryMeta = schemaMeta.tables.Categories;
export const commentMeta = schemaMeta.tables.Comments;
