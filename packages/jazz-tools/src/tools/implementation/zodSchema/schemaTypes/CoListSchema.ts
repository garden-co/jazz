import {
  Account,
  BranchDefinition,
  CoList,
  Group,
  ID,
  MaybeLoaded,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  SubscribeListenerOptions,
  coOptionalDefiner,
  unstable_mergeBranchWithResolve,
} from "../../../internal.js";
import { CoValueUniqueness } from "cojson";
import { AnonymousJazzAgent } from "../../anonymousJazzAgent.js";
import { CoListSchemaInit } from "../typeConverters/CoFieldSchemaInit.js";
import { InstanceOrPrimitiveOfSchema } from "../typeConverters/InstanceOrPrimitiveOfSchema.js";
import { InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded } from "../typeConverters/InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded.js";
import { AnyZodOrCoValueSchema } from "../zodSchema.js";
import { CoOptionalSchema } from "./CoOptionalSchema.js";
import { CoreCoValueSchema } from "./CoValueSchema.js";

export class CoListSchema<T extends AnyZodOrCoValueSchema>
  implements CoreCoListSchema<T>
{
  collaborative = true as const;
  builtin = "CoList" as const;

  constructor(
    public element: T,
    private coValueClass: typeof CoList,
  ) {}

  create(
    items: CoListSchemaInit<T>,
    options?:
      | { owner: Group; unique?: CoValueUniqueness["uniqueness"] }
      | Group,
  ): CoListInstance<T>;
  /** @deprecated Creating CoValues with an Account as owner is deprecated. Use a Group instead. */
  create(
    items: CoListSchemaInit<T>,
    options?:
      | { owner: Account | Group; unique?: CoValueUniqueness["uniqueness"] }
      | Account
      | Group,
  ): CoListInstance<T>;
  create(
    items: CoListSchemaInit<T>,
    options?:
      | { owner: Account | Group; unique?: CoValueUniqueness["uniqueness"] }
      | Account
      | Group,
  ): CoListInstance<T> {
    return this.coValueClass.create(items as any, options) as CoListInstance<T>;
  }

  load<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      unstable_branch?: BranchDefinition;
    },
  ): Promise<MaybeLoaded<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.load(id, options);
  }

  unstable_merge<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    id: string,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
      branch: BranchDefinition;
    },
  ): Promise<void> {
    // @ts-expect-error
    return unstable_mergeBranchWithResolve(this.coValueClass, id, options);
  }

  subscribe<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    id: string,
    options: SubscribeListenerOptions<CoListInstanceCoValuesMaybeLoaded<T>, R>,
    listener: (
      value: Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>,
      unsubscribe: () => void,
    ) => void,
  ): () => void {
    return this.coValueClass.subscribe(id, options, listener);
  }

  getCoValueClass(): typeof CoList {
    return this.coValueClass;
  }

  /** @deprecated Use `CoList.upsertUnique` and `CoList.loadUnique` instead. */
  findUnique(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    as?: Account | Group | AnonymousJazzAgent,
  ): ID<CoListInstanceCoValuesMaybeLoaded<T>> {
    return this.coValueClass.findUnique(unique, ownerID, as);
  }

  upsertUnique<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>> = true,
  >(options: {
    value: CoListSchemaInit<T>;
    unique: CoValueUniqueness["uniqueness"];
    owner: Account | Group;
    resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
  }): Promise<MaybeLoaded<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.upsertUnique(options);
  }

  loadUnique<
    const R extends RefsToResolve<CoListInstanceCoValuesMaybeLoaded<T>> = true,
  >(
    unique: CoValueUniqueness["uniqueness"],
    ownerID: ID<Account> | ID<Group>,
    options?: {
      resolve?: RefsToResolveStrict<CoListInstanceCoValuesMaybeLoaded<T>, R>;
      loadAs?: Account | AnonymousJazzAgent;
    },
  ): Promise<MaybeLoaded<Resolved<CoListInstanceCoValuesMaybeLoaded<T>, R>>> {
    // @ts-expect-error
    return this.coValueClass.loadUnique(unique, ownerID, options);
  }

  optional(): CoOptionalSchema<this> {
    return coOptionalDefiner(this);
  }
}

export function createCoreCoListSchema<T extends AnyZodOrCoValueSchema>(
  element: T,
): CoreCoListSchema<T> {
  return {
    collaborative: true as const,
    builtin: "CoList" as const,
    element,
  };
}

// less precise version to avoid circularity issues and allow matching against
export interface CoreCoListSchema<
  T extends AnyZodOrCoValueSchema = AnyZodOrCoValueSchema,
> extends CoreCoValueSchema {
  builtin: "CoList";
  element: T;
}

export type CoListInstance<T extends AnyZodOrCoValueSchema> = CoList<
  InstanceOrPrimitiveOfSchema<T>
>;

export type CoListInstanceCoValuesMaybeLoaded<T extends AnyZodOrCoValueSchema> =
  CoList<InstanceOrPrimitiveOfSchemaCoValuesMaybeLoaded<T>>;
