import { CryptoProvider } from "cojson";
import {
  Account,
  AccountCreationProps,
  Group,
  RefsToResolveStrict,
} from "../../../internal.js";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesNullable } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesNullable.js";
import { z } from "../zodReExport.js";
import { Loaded, ResolveQuery } from "../zodSchema.js";
import { AnyCoMapSchema, CoMapSchema } from "./CoMapSchema.js";

export type BaseProfileShape = {
  name: z.core.$ZodString<string>;
  inbox?: z.core.$ZodOptional<z.core.$ZodString>;
  inboxInvite?: z.core.$ZodOptional<z.core.$ZodString>;
};

export type BaseAccountShape = {
  profile: AnyCoMapSchema<BaseProfileShape>;
  root: AnyCoMapSchema;
};

export type DefaultAccountShape = {
  profile: CoMapSchema<BaseProfileShape>;
  root: CoMapSchema<{}>;
};

export type AccountSchema<
  Shape extends BaseAccountShape = DefaultAccountShape,
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

  getCoValueClass: () => typeof Account;
};

export function enrichAccountSchema<Shape extends BaseAccountShape>(
  schema: AnyAccountSchema<Shape>,
  coValueClass: typeof Account,
): AccountSchema<Shape> {
  const enrichedSchema = Object.assign(schema, {
    create: (...args: any[]) => {
      // @ts-expect-error
      return coValueClass.create(...args);
    },
    createAs: (...args: any[]) => {
      // @ts-expect-error
      return coValueClass.createAs(...args);
    },
    getMe: (...args: any[]) => {
      // @ts-expect-error
      return coValueClass.getMe(...args);
    },
    load: (...args: any[]) => {
      // @ts-expect-error
      return coValueClass.load(...args);
    },
    subscribe: (...args: any[]) => {
      // @ts-expect-error
      return coValueClass.subscribe(...args);
    },
    withHelpers: (helpers: (Self: z.core.$ZodType) => object) => {
      return Object.assign(schema, helpers(schema));
    },
    fromRaw: (...args: any[]) => {
      // @ts-expect-error
      return coValueClass.fromRaw(...args);
    },
    withMigration: (
      migration: (
        value: any,
        creationProps?: AccountCreationProps,
      ) => void | Promise<void>,
    ) => {
      (coValueClass.prototype as Account).migrate = async function (
        this,
        creationProps,
      ) {
        await migration(this, creationProps);
      };

      return enrichedSchema;
    },
    getCoValueClass: () => {
      return coValueClass;
    },
  }) as unknown as AccountSchema<Shape>;
  return enrichedSchema;
}

export type DefaultProfileShape = {
  name: z.core.$ZodString<string>;
  inbox: z.core.$ZodOptional<z.core.$ZodString>;
  inboxInvite: z.core.$ZodOptional<z.core.$ZodString>;
};

export type CoProfileSchema<
  Shape extends z.core.$ZodLooseShape = DefaultProfileShape,
  Config extends z.core.$ZodObjectConfig = z.core.$ZodObjectConfig,
> = CoMapSchema<Shape & DefaultProfileShape, Config, Group>;

// less precise version to avoid circularity issues and allow matching against
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
