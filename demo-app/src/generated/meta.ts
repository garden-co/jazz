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
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "Folders", sourceTable: "Folders", sourceColumn: "owner" },
        { name: "Notes", sourceTable: "Notes", sourceColumn: "author" },
      ],
    },
    Folders: {
      name: "Folders",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "owner", type: {"kind":"ref","table":"Users"}, nullable: false },
        { name: "parent", type: {"kind":"ref","table":"Folders"}, nullable: true },
      ],
      refs: [
        { column: "owner", targetTable: "Users", nullable: false },
        { column: "parent", targetTable: "Folders", nullable: true },
      ],
      reverseRefs: [
        { name: "Folders", sourceTable: "Folders", sourceColumn: "parent" },
        { name: "Notes", sourceTable: "Notes", sourceColumn: "folder" },
      ],
    },
    Notes: {
      name: "Notes",
      columns: [
        { name: "title", type: {"kind":"string"}, nullable: false },
        { name: "content", type: {"kind":"string"}, nullable: false },
        { name: "author", type: {"kind":"ref","table":"Users"}, nullable: false },
        { name: "folder", type: {"kind":"ref","table":"Folders"}, nullable: true },
        { name: "createdAt", type: {"kind":"i64"}, nullable: false },
        { name: "updatedAt", type: {"kind":"i64"}, nullable: false },
      ],
      refs: [
        { column: "author", targetTable: "Users", nullable: false },
        { column: "folder", targetTable: "Folders", nullable: true },
      ],
      reverseRefs: [
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
      ],
    },
  },
};

// Individual table metadata exports
export const userMeta = schemaMeta.tables.Users;
export const folderMeta = schemaMeta.tables.Folders;
export const noteMeta = schemaMeta.tables.Notes;
export const tagMeta = schemaMeta.tables.Tags;
