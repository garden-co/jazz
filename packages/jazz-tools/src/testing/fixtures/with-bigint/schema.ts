export const app = {
  wasmSchema: {
    counters: {
      columns: [
        {
          name: "largeCount",
          column_type: { type: "BigInt" },
          nullable: false,
          default: { type: "BigInt", value: 9007199254740993n },
        },
      ],
    },
  },
};
