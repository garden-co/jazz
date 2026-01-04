// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

import type { StringFilter, BigIntFilter, NumberFilter, BoolFilter } from "@jazz/schema/runtime";

/** ObjectId is a 128-bit unique identifier (UUIDv7) represented as a Base32 string */
export type ObjectId = string;

/** Base interface for all Groove rows */
export interface GrooveRow {
  id: ObjectId;
}

// === Includes types (specify which refs to load) ===

export type UserIncludes = {
  Folder?: true | FolderIncludes;
  Note?: true | NoteIncludes;
};

export type FolderIncludes = {
  owner?: true | UserIncludes;
  parent?: true | FolderIncludes;
  Folder?: true | FolderIncludes;
  Note?: true | NoteIncludes;
};

export type NoteIncludes = {
  author?: true | UserIncludes;
  folder?: true | FolderIncludes;
};

export type TagIncludes = {};

// === Filter types (Prisma-style filters) ===

export interface UserFilter {
  AND?: UserFilter | UserFilter[];
  OR?: UserFilter[];
  NOT?: UserFilter | UserFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  email?: string | StringFilter;
  avatar?: string | StringFilter | null;
}

export interface FolderFilter {
  AND?: FolderFilter | FolderFilter[];
  OR?: FolderFilter[];
  NOT?: FolderFilter | FolderFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  owner?: string | StringFilter;
  parent?: string | StringFilter | null;
}

export interface NoteFilter {
  AND?: NoteFilter | NoteFilter[];
  OR?: NoteFilter[];
  NOT?: NoteFilter | NoteFilter[];
  id?: string | StringFilter;
  title?: string | StringFilter;
  content?: string | StringFilter;
  author?: string | StringFilter;
  folder?: string | StringFilter | null;
  createdAt?: bigint | BigIntFilter;
  updatedAt?: bigint | BigIntFilter;
}

export interface TagFilter {
  AND?: TagFilter | TagFilter[];
  OR?: TagFilter[];
  NOT?: TagFilter | TagFilter[];
  id?: string | StringFilter;
  name?: string | StringFilter;
  color?: string | StringFilter;
}

// === Row types ===

/** User row from the User table */
export interface User extends GrooveRow {
  name: string;
  email: string;
  avatar: string | null;
}

/** Data for inserting a new User */
export interface UserInsert {
  name: string;
  email: string;
  avatar?: string | null;
}

/** User with refs/reverse refs resolved based on includes parameter I */
export type UserLoaded<I extends UserIncludes = {}> = {
  id: ObjectId;
  name: string;
  email: string;
  avatar: string | null;
}
  & ('Folder' extends keyof I
    ? I['Folder'] extends true
      ? { Folder: Folder[] }
      : I['Folder'] extends object
        ? { Folder: FolderLoaded<I['Folder'] & FolderIncludes>[] }
        : {}
    : {})
  & ('Note' extends keyof I
    ? I['Note'] extends true
      ? { Note: Note[] }
      : I['Note'] extends object
        ? { Note: NoteLoaded<I['Note'] & NoteIncludes>[] }
        : {}
    : {})
;

/** Folder row from the Folder table */
export interface Folder extends GrooveRow {
  name: string;
  owner: ObjectId;
  parent: ObjectId | null;
}

/** Data for inserting a new Folder */
export interface FolderInsert {
  name: string;
  owner: ObjectId | User;
  parent?: ObjectId | Folder | null;
}

/** Folder with refs/reverse refs resolved based on includes parameter I */
export type FolderLoaded<I extends FolderIncludes = {}> = {
  id: ObjectId;
  name: string;
  owner: 'owner' extends keyof I
    ? I['owner'] extends true
      ? User
      : I['owner'] extends object
        ? UserLoaded<I['owner'] & UserIncludes>
        : ObjectId
    : ObjectId;
  parent: 'parent' extends keyof I
    ? I['parent'] extends true
      ? Folder | null
      : I['parent'] extends object
        ? FolderLoaded<I['parent'] & FolderIncludes> | null
        : ObjectId | null
    : ObjectId | null;
}
  & ('Folder' extends keyof I
    ? I['Folder'] extends true
      ? { Folder: Folder[] }
      : I['Folder'] extends object
        ? { Folder: FolderLoaded<I['Folder'] & FolderIncludes>[] }
        : {}
    : {})
  & ('Note' extends keyof I
    ? I['Note'] extends true
      ? { Note: Note[] }
      : I['Note'] extends object
        ? { Note: NoteLoaded<I['Note'] & NoteIncludes>[] }
        : {}
    : {})
;

/** Note row from the Note table */
export interface Note extends GrooveRow {
  title: string;
  content: string;
  author: ObjectId;
  folder: ObjectId | null;
  createdAt: bigint;
  updatedAt: bigint;
}

/** Data for inserting a new Note */
export interface NoteInsert {
  title: string;
  content: string;
  author: ObjectId | User;
  folder?: ObjectId | Folder | null;
  createdAt: bigint;
  updatedAt: bigint;
}

/** Note with refs/reverse refs resolved based on includes parameter I */
export type NoteLoaded<I extends NoteIncludes = {}> = {
  id: ObjectId;
  title: string;
  content: string;
  author: 'author' extends keyof I
    ? I['author'] extends true
      ? User
      : I['author'] extends object
        ? UserLoaded<I['author'] & UserIncludes>
        : ObjectId
    : ObjectId;
  folder: 'folder' extends keyof I
    ? I['folder'] extends true
      ? Folder | null
      : I['folder'] extends object
        ? FolderLoaded<I['folder'] & FolderIncludes> | null
        : ObjectId | null
    : ObjectId | null;
  createdAt: bigint;
  updatedAt: bigint;
}
;

/** Tag row from the Tag table */
export interface Tag extends GrooveRow {
  name: string;
  color: string;
}

/** Data for inserting a new Tag */
export interface TagInsert {
  name: string;
  color: string;
}

/** Tag has no refs, so Loaded is the same as base type */
export type TagLoaded<I extends TagIncludes = {}> = Tag;
