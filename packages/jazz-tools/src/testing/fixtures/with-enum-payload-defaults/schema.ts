export const app = {
  wasmSchema: {
    records: {
      columns: [
        {
          name: "state",
          column_type: {
            type: "EnumPayload",
            cases: [
              {
                name: "ready",
                fields: [
                  {
                    name: "count",
                    column_type: { type: "Integer" },
                    nullable: false,
                    default: { type: "Integer", value: 3 },
                  },
                  {
                    name: "label",
                    column_type: { type: "Text" },
                    nullable: false,
                    default: { type: "Text", value: "queued" },
                  },
                  {
                    name: "note",
                    column_type: { type: "Text" },
                    nullable: true,
                    default: { type: "Null" },
                  },
                ],
              },
            ],
          },
          nullable: false,
          default: {
            type: "Enum",
            value: {
              case: "ready",
              values: [
                { type: "Integer", value: 7 },
                { type: "Text", value: "live" },
                { type: "Null" },
              ],
            },
          },
        },
      ],
    },
  },
};
