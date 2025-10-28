import { BetterAuthDbSchema, FieldAttribute } from "better-auth/db";
import { Group, co, z } from "jazz-tools";

type TableRow = co.Map<any>;
export type TableItem = co.loaded<TableRow>;

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
  betterAuthSchema: BetterAuthDbSchema;
  loadDatabase: (
    account: co.loaded<co.Account>,
    options?: Parameters<Database["loadUnique"]>[2],
  ) => Promise<co.loaded<Database, { group: true }>>;
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
        account.$jazz.id,
        {
          resolve: {
            group: true,
            tables: true,
          },
          loadAs: account,
        },
      );

      if (!dbRoot.$isLoaded) {
        // Create a group for the first time
        // it will be the owner of the all tables and data
        const adminGroup = Group.create({ owner: account });
        await DatabaseRoot.upsertUnique({
          value: {
            group: adminGroup,
            // create empty tables for each model
            tables: co
              .map(tablesSchema)
              .create(
                Object.fromEntries(
                  Object.entries(tablesSchema).map(([key, value]) => [
                    key,
                    value.create([], adminGroup),
                  ]),
                ),
                adminGroup,
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
            dbRoot.tables.$jazz.set(key, value.create([], dbRoot.group));
          }
        }
      }
    });

  return {
    WorkerAccount,
    DatabaseRoot,
    betterAuthSchema: schema,
    async loadDatabase(account, options) {
      if (
        options?.resolve === false ||
        (typeof options?.resolve === "object" &&
          options?.resolve.group !== true)
      ) {
        throw new Error("Group is required to load the database");
      }

      const db = (await DatabaseRoot.loadUnique(
        DATABASE_ROOT_ID,
        account.$jazz.id,
        {
          resolve: {
            group: true,
            tables: true,
          },
          loadAs: account,
          ...options,
        },
      )) as co.loaded<Database, { group: true }>;

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
    > = {
      _deleted: z.boolean(),
    };

    for (const [fieldName, field] of Object.entries(value.fields)) {
      modelShape[field.fieldName || fieldName] = convertFieldToCoValue(field);
    }

    const coMap = co.map(modelShape);
    tablesSchema[key] = co.list(coMap);
  }

  if (tablesSchema["user"] && tablesSchema["session"]) {
    tablesSchema["user"] = co.list(
      co
        .map({
          ...tablesSchema["user"].element.shape,
          sessions: tablesSchema["session"],
        })
        .withMigration((user) => {
          if (user.sessions === undefined) {
            user.$jazz.set(
              "sessions",
              tablesSchema["session"]!.create([], user.$jazz.owner),
            );
          }
        }),
    );
  } else {
    throw new Error(
      "Cannot find user and session tables, sessions will not be persisted",
    );
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

export function tableItem2Record(
  tableItem: TableItem | null | undefined,
): Record<string, any> | null | undefined {
  if (!tableItem) {
    return tableItem;
  }

  // tableItem.toJSON() transforms Date objects to ISO strings
  // by returning ...rest, we keep the objects
  const { $jazz, ...rest } = tableItem;
  return {
    ...rest,
    id: $jazz.id,
  };
}
