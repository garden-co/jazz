// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue = string | number | boolean | null | { [key: string]: JsonValue } | JsonValue[];

export type PermissionIntrospectionColumn = "$canRead" | "$canEdit" | "$canDelete";
export interface PermissionIntrospectionColumns {
  $canRead: boolean | null;
  $canEdit: boolean | null;
  $canDelete: boolean | null;
}

export interface Message {
  id: string;
  author_id: string;
  author_name: string;
  chat_id: string;
  text: string;
  sent_at: Date;
}

export interface MessageInit {
  author_id: string;
  author_name: string;
  chat_id: string;
  text: string;
  sent_at: Date;
}

export interface MessageWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  author_id?: string | { eq?: string; ne?: string; contains?: string };
  author_name?: string | { eq?: string; ne?: string; contains?: string };
  chat_id?: string | { eq?: string; ne?: string; contains?: string };
  text?: string | { eq?: string; ne?: string; contains?: string };
  sent_at?: Date | number | { eq?: Date | number; gt?: Date | number; gte?: Date | number; lt?: Date | number; lte?: Date | number };
  $canRead?: boolean;
  $canEdit?: boolean;
  $canDelete?: boolean;
}

export type MessageSelectableColumn = keyof Message | PermissionIntrospectionColumn | "*";
export type MessageOrderableColumn = keyof Message | PermissionIntrospectionColumn;

export type MessageSelected<S extends MessageSelectableColumn = keyof Message> = ("*" extends S ? Message : Pick<Message, Extract<S | "id", keyof Message>>) & Pick<PermissionIntrospectionColumns, Extract<S, PermissionIntrospectionColumn>>;

export const wasmSchema: WasmSchema = {
  "messages": {
    "columns": [
      {
        "name": "author_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "author_name",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "chat_id",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "text",
        "column_type": {
          "type": "Text"
        },
        "nullable": false
      },
      {
        "name": "sent_at",
        "column_type": {
          "type": "Timestamp"
        },
        "nullable": false
      }
    ],
    "policies": {
      "select": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "Cmp",
              "column": "chat_id",
              "op": "Eq",
              "value": {
                "type": "Literal",
                "value": {
                  "type": "Text",
                  "value": "announcements"
                }
              }
            },
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "chat-01"
                    }
                  }
                },
                {
                  "type": "SessionInList",
                  "path": [
                    "claims",
                    "role"
                  ],
                  "values": [
                    {
                      "type": "Text",
                      "value": "admin"
                    },
                    {
                      "type": "Text",
                      "value": "member"
                    }
                  ]
                }
              ]
            }
          ]
        }
      },
      "insert": {
        "with_check": {
          "type": "Or",
          "exprs": [
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "announcements"
                    }
                  }
                },
                {
                  "type": "SessionCmp",
                  "path": [
                    "claims",
                    "role"
                  ],
                  "op": "Eq",
                  "value": {
                    "type": "Text",
                    "value": "admin"
                  }
                }
              ]
            },
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "chat-01"
                    }
                  }
                },
                {
                  "type": "Or",
                  "exprs": [
                    {
                      "type": "Cmp",
                      "column": "author_id",
                      "op": "Eq",
                      "value": {
                        "type": "SessionRef",
                        "path": [
                          "user_id"
                        ]
                      }
                    },
                    {
                      "type": "SessionCmp",
                      "path": [
                        "claims",
                        "role"
                      ],
                      "op": "Eq",
                      "value": {
                        "type": "Text",
                        "value": "admin"
                      }
                    }
                  ]
                }
              ]
            }
          ]
        }
      },
      "update": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "announcements"
                    }
                  }
                },
                {
                  "type": "SessionCmp",
                  "path": [
                    "claims",
                    "role"
                  ],
                  "op": "Eq",
                  "value": {
                    "type": "Text",
                    "value": "admin"
                  }
                }
              ]
            },
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "chat-01"
                    }
                  }
                },
                {
                  "type": "Or",
                  "exprs": [
                    {
                      "type": "Cmp",
                      "column": "author_id",
                      "op": "Eq",
                      "value": {
                        "type": "SessionRef",
                        "path": [
                          "user_id"
                        ]
                      }
                    },
                    {
                      "type": "SessionCmp",
                      "path": [
                        "claims",
                        "role"
                      ],
                      "op": "Eq",
                      "value": {
                        "type": "Text",
                        "value": "admin"
                      }
                    }
                  ]
                }
              ]
            }
          ]
        },
        "with_check": {
          "type": "Or",
          "exprs": [
            {
              "type": "Cmp",
              "column": "chat_id",
              "op": "Eq",
              "value": {
                "type": "Literal",
                "value": {
                  "type": "Text",
                  "value": "announcements"
                }
              }
            },
            {
              "type": "Cmp",
              "column": "chat_id",
              "op": "Eq",
              "value": {
                "type": "Literal",
                "value": {
                  "type": "Text",
                  "value": "chat-01"
                }
              }
            }
          ]
        }
      },
      "delete": {
        "using": {
          "type": "Or",
          "exprs": [
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "announcements"
                    }
                  }
                },
                {
                  "type": "SessionCmp",
                  "path": [
                    "claims",
                    "role"
                  ],
                  "op": "Eq",
                  "value": {
                    "type": "Text",
                    "value": "admin"
                  }
                }
              ]
            },
            {
              "type": "And",
              "exprs": [
                {
                  "type": "Cmp",
                  "column": "chat_id",
                  "op": "Eq",
                  "value": {
                    "type": "Literal",
                    "value": {
                      "type": "Text",
                      "value": "chat-01"
                    }
                  }
                },
                {
                  "type": "Or",
                  "exprs": [
                    {
                      "type": "Cmp",
                      "column": "author_id",
                      "op": "Eq",
                      "value": {
                        "type": "SessionRef",
                        "path": [
                          "user_id"
                        ]
                      }
                    },
                    {
                      "type": "SessionCmp",
                      "path": [
                        "claims",
                        "role"
                      ],
                      "op": "Eq",
                      "value": {
                        "type": "Text",
                        "value": "admin"
                      }
                    }
                  ]
                }
              ]
            }
          ]
        }
      }
    }
  }
};

export class MessageQueryBuilder<I extends Record<string, never> = {}, S extends MessageSelectableColumn = keyof Message, R extends boolean = false> implements QueryBuilder<MessageSelected<S>> {
  readonly _table = "messages";
  readonly _schema: WasmSchema = wasmSchema;
  readonly _rowType!: MessageSelected<S>;
  readonly _initType!: MessageInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<Record<string, never>> = {};
  private _requireIncludes = false;
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: MessageWhereInput): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends MessageSelectableColumn>(...columns: [NewS, ...NewS[]]): MessageQueryBuilder<I, NewS, R> {
    const clone = this._clone<I, NewS, R>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  orderBy(column: MessageOrderableColumn, direction: "asc" | "desc" = "asc"): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): MessageQueryBuilder<I, S, R> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  gather(options: {
    start: MessageWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): MessageQueryBuilder<I, S, R> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (!stepOutput || typeof stepOutput !== "object" || typeof (stepOutput as { _build?: unknown })._build !== "function") {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(
      stepOutput._build(),
    ) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }

    const currentCondition = currentConditions[0];
    if (currentCondition === undefined) {
      throw new Error("gather(...) step must include exactly one where condition bound to current.");
    }
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      __jazz_requireIncludes: this._requireIncludes || undefined,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<CloneI extends Record<string, never> = I, CloneS extends MessageSelectableColumn = S, CloneR extends boolean = R>(): MessageQueryBuilder<CloneI, CloneS, CloneR> {
    const clone = new MessageQueryBuilder<CloneI, CloneS, CloneR>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._requireIncludes = this._requireIncludes;
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  messages: MessageQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  messages: new MessageQueryBuilder(),
  wasmSchema,
};
