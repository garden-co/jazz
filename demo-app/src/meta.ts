// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import type { SchemaMeta } from "@jazz/schema/runtime";

export const schemaMeta: SchemaMeta = {
  tables: {
    User: {
      name: "User",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "email", type: {"kind":"string"}, nullable: false },
        { name: "avatar", type: {"kind":"string"}, nullable: true },
      ],
      refs: [
      ],
      reverseRefs: [
        { name: "Folder", sourceTable: "Folder", sourceColumn: "owner" },
        { name: "Note", sourceTable: "Note", sourceColumn: "author" },
      ],
    },
    Folder: {
      name: "Folder",
      columns: [
        { name: "name", type: {"kind":"string"}, nullable: false },
        { name: "owner", type: {"kind":"ref","table":"User"}, nullable: false },
        { name: "parent", type: {"kind":"ref","table":"Folder"}, nullable: true },
      ],
      refs: [
        { column: "owner", targetTable: "User", nullable: false },
        { column: "parent", targetTable: "Folder", nullable: true },
      ],
      reverseRefs: [
        { name: "Folder", sourceTable: "Folder", sourceColumn: "parent" },
        { name: "Note", sourceTable: "Note", sourceColumn: "folder" },
      ],
    },
    Note: {
      name: "Note",
      columns: [
        { name: "title", type: {"kind":"string"}, nullable: false },
        { name: "content", type: {"kind":"string"}, nullable: false },
        { name: "author", type: {"kind":"ref","table":"User"}, nullable: false },
        { name: "folder", type: {"kind":"ref","table":"Folder"}, nullable: true },
        { name: "createdAt", type: {"kind":"i64"}, nullable: false },
        { name: "updatedAt", type: {"kind":"i64"}, nullable: false },
      ],
      refs: [
        { column: "author", targetTable: "User", nullable: false },
        { column: "folder", targetTable: "Folder", nullable: true },
      ],
      reverseRefs: [
      ],
    },
    Tag: {
      name: "Tag",
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
export const userMeta = schemaMeta.tables.User;
export const folderMeta = schemaMeta.tables.Folder;
export const noteMeta = schemaMeta.tables.Note;
export const tagMeta = schemaMeta.tables.Tag;
