import { z } from "zod";
import { table, generateSchema } from "@jazz/schema";

// A simple note-taking app schema

const User = table({
  name: z.string(),
  email: z.string(),
  avatar: z.optional(z.string()),
});

const Folder = table({
  name: z.string(),
  owner: User,
  get parent() {
    return z.optional(Folder);
  },
});

const Note = table({
  title: z.string(),
  content: z.string(),
  author: User,
  folder: z.optional(Folder),
  createdAt: z.date(),
  updatedAt: z.date(),
});

const Tag = table({
  name: z.string(),
  color: z.string(),
});

generateSchema({ User, Folder, Note, Tag });
