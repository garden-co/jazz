import { BetterAuthDbSchema, FieldAttribute } from "better-auth/db";
import { Group, co, z } from "jazz-tools";

type TableRow = co.Map<any>;
type Table = co.List<TableRow>;
export type Database = co.Map<{
  group: typeof Group;
  tables: co.Map<{
    [key: string]: Table;
  }>;
}>;

type WorkerAccount = co.Account<{
  profile: co.Profile;
  root: co.Map<any>;
}>;

type JazzSchema = {
  WorkerAccount: WorkerAccount;
  DatabaseRoot: Database;
  loadDatabase: (
    account: co.loaded<co.Account>,
    options?: Parameters<Database["loadUnique"]>[2],
  ) => Promise<co.loaded<Database>>;
};

const DATABASE_ROOT_ID = "better-auth-root";

export function createJazzSchema(schema: BetterAuthDbSchema): JazzSchema {
  const tablesSchema = generateSchemaFromBetterAuthSchema(schema);

  const DatabaseRoot: Database = co.map({
    group: Group,
    tables: co.map(tablesSchema),
  });

  const WorkerAccount: WorkerAccount = co
    .account({
      profile: co.profile(),
      root: co.map({}),
    })
    .withMigration(async (account) => {
      const dbRoot = await DatabaseRoot.loadUnique(
        DATABASE_ROOT_ID,
        account.id,
        {
          resolve: {
            group: true,
            tables: true,
          },
        },
      );

      if (!dbRoot) {
        // Create a group for the first time
        // it will be the owner of the all tables and data
        const adminGroup = Group.create({ owner: account });
        DatabaseRoot.upsertUnique({
          value: {
            group: adminGroup,
            // create empty tables for each model
            tables: Object.fromEntries(
              Object.keys(tablesSchema).map((key) => [key, []]),
            ),
          },
          unique: DATABASE_ROOT_ID,
          owner: account,
        });
      } else {
        // partial migrations
        for (const [key, value] of Object.entries(
          DatabaseRoot.shape.tables.shape,
        )) {
          if (dbRoot.tables[key] === undefined) {
            dbRoot.tables[key] = value.create([], dbRoot.group);
          }
        }
      }
    });

  return {
    WorkerAccount,
    DatabaseRoot,
    async loadDatabase(account, options) {
      const db = await DatabaseRoot.loadUnique(
        DATABASE_ROOT_ID,
        account.id,
        options || {
          resolve: {
            group: true,
            tables: true,
          },
        },
      );

      if (!db) {
        throw new Error("Database not found");
      }

      return db;
    },
  };
}

type ZodPrimitiveSchema =
  | z.z.ZodString
  | z.z.ZodNumber
  | z.z.ZodBoolean
  | z.z.ZodNull
  | z.z.ZodDate
  | z.z.ZodLiteral;
type ZodOptionalPrimitiveSchema = z.z.ZodOptional<ZodPrimitiveSchema>;

function generateSchemaFromBetterAuthSchema(schema: BetterAuthDbSchema) {
  const tablesSchema: Record<string, Table> = {};

  for (const [key, value] of Object.entries(schema)) {
    const modelShape: Record<
      string,
      ZodPrimitiveSchema | ZodOptionalPrimitiveSchema
    > = {};

    for (const [fieldName, field] of Object.entries(value.fields)) {
      modelShape[field.fieldName || fieldName] = convertFieldToCoValue(field);
    }

    modelShape["_deleted"] = z.boolean();

    const coMap = co.map(modelShape);
    tablesSchema[key] = co.list(coMap);
  }

  return tablesSchema;
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
