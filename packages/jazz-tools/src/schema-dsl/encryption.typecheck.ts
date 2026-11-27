import { migration as m, schema as s } from "../index.js";

const encrypted = s.string().encrypted();

// @ts-expect-error encrypt the whole array column
s.array(encrypted);
// @ts-expect-error computed fields are not stored
s.computed(encrypted, ["title"]);
// @ts-expect-error transform views cannot be encrypted
s.string().transform("asTitle", encrypted);
// @ts-expect-error a column builder is not a JSON schema
s.json(encrypted);
// @ts-expect-error migration array elements describe stored types
m.add.array({ of: encrypted, default: null });
// @ts-expect-error migration array elements describe stored types
m.drop.array({ of: encrypted, backwardsDefault: null });
// @ts-expect-error a column builder is not a JSON schema
m.add.json({ schema: encrypted, default: null });
// @ts-expect-error a column builder is not a JSON schema
m.drop.json({ schema: encrypted, backwardsDefault: null });
