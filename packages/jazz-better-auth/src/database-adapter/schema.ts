import { BetterAuthDbSchema, FieldAttribute } from "better-auth/db";
import { type Account, co, z } from "jazz-tools";

type ZodPrimitiveSchema =
  | z.z.ZodString
  | z.z.ZodNumber
  | z.z.ZodBoolean
  | z.z.ZodNull
  | z.z.ZodDate
  | z.z.ZodLiteral;

type ZodOptionalPrimitiveSchema = z.z.ZodOptional<ZodPrimitiveSchema>;

type RootSchema = Record<keyof BetterAuthDbSchema, co.List<co.Map<any>>>;
type DbSchema = Record<keyof BetterAuthDbSchema, co.Map<any>>;
type WorkerAccount = co.Account<{
  profile: co.Profile;
  root: co.Map<RootSchema>;
}>;

type JazzSchema = {
  WorkerAccount: WorkerAccount;
  dbSchema: DbSchema;
  rootSchema: RootSchema;
};

export function createJazzSchema(schema: BetterAuthDbSchema): JazzSchema {
  const dbSchema: DbSchema = {};
  const rootSchema: RootSchema = {};

  for (const [key, value] of Object.entries(schema)) {
    const coMapSchema: Record<
      string,
      ZodPrimitiveSchema | ZodOptionalPrimitiveSchema
    > = {};

    for (const [fieldName, field] of Object.entries(value.fields)) {
      // console.log({fieldName, field});
      coMapSchema[field.fieldName || fieldName] = convertFieldToCoValue(field);
    }

    const coMap = co.map(coMapSchema);
    dbSchema[key] = coMap;
    rootSchema[key] = co.list(coMap);
  }

  const WorkerAccount = co
    .account({
      profile: co.profile(),
      root: co.map(rootSchema),
    })
    .withMigration(async (account) => {
      if (account.root === undefined) {
        const rootValues = Object.fromEntries(
          Object.entries(rootSchema).map(([key, value]) => [
            key,
            value.create([], account),
          ]),
        );

        account.root = co.map(rootSchema).create(rootValues, account);
      } else {
        await account.ensureLoaded({
          resolve: {
            root: true,
          },
        });
      }

      for (const [key, value] of Object.entries(rootSchema)) {
        if (account.root?.[key] === undefined) {
          account.root![key] = value.create([], account);
        }
      }
    });

  return {
    WorkerAccount,
    dbSchema,
    rootSchema,
  };
}

function convertFieldToCoValue(field: FieldAttribute) {
  let zodType: ZodPrimitiveSchema | ZodOptionalPrimitiveSchema;

  switch (field.type) {
    case "string":
      zodType = z.string();
      break;
    case "number":
      zodType = z.number();
      break;
    case "boolean":
      zodType = z.boolean();
      break;
    case "date":
      zodType = z.date();
      break;
    default:
      throw new Error(`Unsupported field type: ${field.type}`);
  }

  if (field.required === false) {
    zodType = zodType.optional();
  }

  return zodType;
}
