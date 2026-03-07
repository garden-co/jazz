// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";

export interface Todo {
  id: string;
  title: string;
  done: boolean;
}

export interface TodoInit {
  title: string;
  done: boolean;
}

export interface TodoWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  title?: string | { eq?: string; ne?: string; contains?: string };
  done?: boolean;
}

export const wasmSchema: WasmSchema = {
  todos: {
    columns: [
      {
        name: "title",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "done",
        column_type: {
          type: "Boolean",
        },
        nullable: false,
      },
    ],
  },
};

export const app = {
  wasmSchema,
};
