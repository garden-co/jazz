import {
  Account,
  CoValueCreateOptions,
  CoValueCreateOptionsInternal,
  Group,
  TypeSym,
  type GroupRole,
} from "../../internal.js";

/**
 * Defines how a nested CoValue’s owner is obtained when creating CoValues from JSON.
 *
 * This configuration is not used when using an explicit .create() for nested CoValues.
 * In that case, {@link SchemaPermissions.default} is used.
 */
export type OnInlineCreateOptions =
  /**
   * Always create a new group for CoValues created inline
   */
  | "newGroup"
  /**
   * Use the same owner as the container CoValue
   */
  | "sameAsContainer"
  /**
   * Create a new group that includes the container CoValue's owner as a member (effectively inheriting
   * all permissions from the container)
   */
  | "extendsContainer"
  /**
   * Similar to "extendsContainer", but allows overriding the role of the container CoValue's owner
   */
  | { extendsContainer: GroupRole }
  /**
   * Create a new group and configure it as needed
   */
  | InlineGroupConfigurationCallback;

export type InlineGroupConfigurationCallback = (
  newGroup: Group,
  context: { containerOwner: Group },
) => void;

export type OnCreateCallback = (newGroup: Group) => void;

/**
 * Internal callback type used by RefPermissions that includes init for discriminated union support.
 * @internal
 */
export type RefOnCreateCallback = (newGroup: Group, init?: unknown) => void;

/**
 * Permissions to be used when creating or composing CoValues
 * @param default - default owner to be used when creating a CoValue without providing an explicit owner.
 * @param onInlineCreate - defines how a nested CoValue's owner is obtained when creating CoValues from JSON.
 * @param onCreate - callback that runs every time a CoValue is created. Can be used to configure the CoValue's owner.
 * Runs both when creating CoValues with `.create()` and when creating CoValues from JSON.
 * @default { default: () => Group.create(), onInlineCreate: "extendsContainer" }
 */
export type SchemaPermissions = {
  /**
   * default owner to be used when creating a CoValue without providing an explicit owner.
   */
  default?: () => Group;
  /**
   * Defines how a nested CoValue's owner is obtained when creating CoValues from JSON.
   */
  onInlineCreate?: OnInlineCreateOptions;
  /**
   * callback that runs every time a CoValue is created. Can be used to configure the CoValue's owner.
   * Runs both when creating CoValues with `.create()` and when creating CoValues from JSON.
   */
  onCreate?: OnCreateCallback;
  /**
   * Restrict deletion operations on CoList values to manager/admin roles.
   */
  writer?: "appendOnly";
};

export let DEFAULT_SCHEMA_PERMISSIONS: SchemaPermissions = {
  default: () => Group.create(),
  onInlineCreate: "extendsContainer",
};

/**
 * Update the default schema permissions for all new CoValue schemas.
 * Schemas created before calling this function will not be affected.
 */
export function setDefaultSchemaPermissions(permissions: SchemaPermissions) {
  DEFAULT_SCHEMA_PERMISSIONS = {
    ...DEFAULT_SCHEMA_PERMISSIONS,
    ...permissions,
  };
}

/**
 * Parsed {@link SchemaPermissions}, used by CoValue classes to set up permissions for referenced CoValues.
 */
export type RefPermissions = {
  newInlineOwnerStrategy: NewInlineOwnerStrategy;
  onCreate?: RefOnCreateCallback;
};

/**
 * A function that creates a new owner for a new CoValue created inline.
 * @param createNewGroup - A function that creates a new group.
 * @param containerOwner - The owner of the container CoValue.
 * @param init - The value used to create the new CoValue. Necessary to determine the concrete
 * strategy to use in discriminated unions.
 * @returns The new owner.
 */
export type NewInlineOwnerStrategy = (
  createNewGroup: () => Group,
  containerOwner: Group,
  init?: unknown,
) => Group;

export const extendContainerOwnerFactory =
  (roleOverride?: GroupRole): NewInlineOwnerStrategy =>
  (createNewGroup: () => Group, containerOwner: Group): Group => {
    const node = containerOwner.$jazz.localNode;
    const rawGroup = node.createGroup();
    const owner = new Group({ fromRaw: rawGroup });
    owner.addMember(containerOwner, roleOverride);
    return owner;
  };

/**
 * A function that creates a new owner for a new CoValue by extending the container CoValue's owner
 * (without overriding its role)
 */
export const extendContainerOwner = extendContainerOwnerFactory();

export function schemaToRefPermissions(
  permissions: SchemaPermissions,
): RefPermissions {
  const newInlineOwnerStrategy = parseOnInlineCreate(
    permissions.onInlineCreate,
  );
  const onCreate: RefOnCreateCallback | undefined = permissions.onCreate
    ? (newGroup, _init) => permissions.onCreate?.(newGroup)
    : undefined;
  return {
    newInlineOwnerStrategy,
    onCreate,
  };
}

function parseOnInlineCreate(
  onInlineCreate?: OnInlineCreateOptions,
): NewInlineOwnerStrategy {
  if (!onInlineCreate || onInlineCreate === "extendsContainer") {
    return extendContainerOwner;
  }
  if (
    typeof onInlineCreate === "object" &&
    "extendsContainer" in onInlineCreate
  ) {
    return extendContainerOwnerFactory(onInlineCreate.extendsContainer);
  }
  if (onInlineCreate === "newGroup") {
    return (createNewGroup) => createNewGroup();
  }
  if (onInlineCreate === "sameAsContainer") {
    return (_createNewGroup, containerOwner) => containerOwner;
  }
  return (createNewGroup, containerOwner) => {
    const newGroup = createNewGroup();
    onInlineCreate(newGroup, { containerOwner });
    return newGroup;
  };
}

export function getDefaultRefPermissions(): RefPermissions {
  return schemaToRefPermissions(DEFAULT_SCHEMA_PERMISSIONS);
}

export function withSchemaPermissions<T extends { owner?: Account | Group }>(
  options?: T | Account | Group,
  schemaPermissions?: SchemaPermissions,
): T & { onCreate?: OnCreateCallback; restrictDeletion?: boolean } {
  const onCreate = schemaPermissions?.onCreate;
  const schemaRestrictDeletion = schemaPermissions?.writer === "appendOnly";
  if (!options) {
    const owner = schemaPermissions?.default?.() ?? Group.create();
    return {
      owner,
      onCreate,
      ...(schemaRestrictDeletion ? { restrictDeletion: true } : {}),
    } as T & { onCreate?: OnCreateCallback };
  }
  if (TypeSym in options) {
    return {
      owner: options,
      onCreate,
      ...(schemaRestrictDeletion ? { restrictDeletion: true } : {}),
    } as T & {
      onCreate?: OnCreateCallback;
    };
  }
  const owner =
    options.owner ?? schemaPermissions?.default?.() ?? Group.create();
  const optionRestrictDeletion = schemaPermissions?.writer === "appendOnly";
  return {
    ...options,
    owner,
    onCreate,
    ...(schemaRestrictDeletion || optionRestrictDeletion
      ? { restrictDeletion: true }
      : {}),
  } as T & { onCreate?: OnCreateCallback };
}
