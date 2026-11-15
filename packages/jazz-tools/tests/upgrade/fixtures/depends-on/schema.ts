import { schema as s } from "jazz-tools";
import { migrations } from "./migrations";

const schema = {
  items: s.table(
    {
      currency: s.string(),
      amount: s.int().transform({
        dependsOn: ["currency"],
        from: (cents, { currency }) => `${cents / 100} ${currency}`,
        to: (text) => Math.round(Number.parseFloat(text) * 100),
      }),
    },
    {},
  ),
};

export const app: s.App<s.Schema<typeof schema>> = s.defineApp(schema, { migrations });
