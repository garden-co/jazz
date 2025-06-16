import { CryptoProvider } from "cojson";
import { Account, Group, RefsToResolveStrict } from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";
import { Loaded, ResolveQuery } from "../zodSchema.js";
import { AnyCoMapSchema, CoMapSchema } from "./CoMapSchema.js";

export type AccountSchema<
  Shape extends {
    profile: AnyCoMapSchema<{
      name: z.core.$ZodString<string>;
      inbox?: z.core.$ZodOptional<z.core.$ZodString>;
      inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
    }>;
    root: AnyCoMapSchema;
  } = {
    profile: CoMapSchema<{
      name: z.core.$ZodString<string>;
      inbox?: z.core.$ZodOptional<z.core.$ZodString>;
      inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
    }>;
    root: CoMapSchema<{}>;
  },
> = Omit<CoMapSchema<Shape>, "create" | "load" | "withMigration"> & {
  builtin: "Account";

  create: (options: {
    creationProps?: { name: string };
    crypto?: CryptoProvider;
  }) => Promise<AccountInstance<Shape>>;

  load: <R extends ResolveQuery<AccountSchema<Shape>>>(
    id: string,
    options?: {
      loadAs?: Account | AnonymousJazzAgent;
      resolve?: RefsToResolveStrict<AccountSchema<Shape>, R>;
    },
  ) => Promise<Loaded<AccountSchema<Shape>, R> | null>;

  createAs: (
    as: Account,
    options: {
      creationProps?: { name: string };
    },
  ) => Promise<AccountInstance<Shape>>;

  getMe: () => AccountInstanceCoValuesNullable<Shape>;

  withMigration(
    migration: (
      account: Loaded<AccountSchema<Shape>>,
      creationProps?: { name: string },
    ) => void,
  ): AccountSchema<Shape>;

  getCoSchema: () => typeof Account;
};

export type DefaultProfileShape = {
  name: z.core.$ZodString<string>;
  inbox: z.core.$ZodOptional<z.core.$ZodString>;
  inboxInvite: z.core.$ZodOptional<z.core.$ZodString>;
};

export type CoProfileSchema<
  Shape extends z.core.$ZodLooseShape = DefaultProfileShape,
  Config extends z.core.$ZodObjectConfig = z.core.$ZodObjectConfig,
> = CoMapSchema<Shape & DefaultProfileShape, Config, Group>;

// less precise verion to avoid circularity issues and allow matching against
export type AnyAccountSchema<
  Shape extends z.core.$ZodLooseShape = z.core.$ZodLooseShape,
> = z.core.$ZodObject<Shape> & {
  collaborative: true;
  builtin: "Account";
};

export type AccountInstance<Shape extends z.core.$ZodLooseShape> = {
  -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchema<Shape[key]>;
} & Account;

export type AccountInstanceCoValuesNullable<
  Shape extends z.core.$ZodLooseShape,
> = {
  -readonly [key in keyof Shape]: InstanceOrPrimitiveOfSchemaCoValuesNullable<
    Shape[key]
  >;
} & Account;
