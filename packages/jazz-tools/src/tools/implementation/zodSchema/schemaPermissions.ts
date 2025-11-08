import { Group, type GroupRole } from "../../internal.js";

/**
 * Callback to configure a CoValue's group when using `.create()` without providing an explicit owner.
 */
export type GroupConfigurationCallback = (newGroup: Group) => void;

/**
 * Defines how a nested CoValue’s owner is obtained when creating CoValues from JSON.
 *
 * This configuration is not used when using an explicit .create() for nested CoValues.
 * In that case, {@link SchemaPermissions.onCreate} is used.
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

/**
 * Permissions to be used when creating or composing CoValues
 * @param onCreate - allows configuring a CoValue’s group when using `.create()` without providing an explicit owner.
 * @param onInlineCreate - defines how a nested CoValue’s owner is obtained when creating CoValues from JSON.
 * @default { onInlineCreate: "extendsContainer" }
 */
export type SchemaPermissions = {
  onCreate?: GroupConfigurationCallback;
  onInlineCreate?: OnInlineCreateOptions;
};

/**
 * Parsed {@link SchemaPermissions}, used by CoValue classes to set up permissions for referenced CoValues.
 */
export type RefPermissions = {
  newInlineOwnerStrategy: NewInlineOwnerStrategy;
};

/**
 * A function that creates a new owner for a new CoValue created inline.
 * @param createNewGroup - A function that creates a new group.
 * @param containerOwner - The owner of the container CoValue.
 * @returns The new owner.
 */
export type NewInlineOwnerStrategy = (
  createNewGroup: () => Group,
  containerOwner: Group,
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
  return {
    newInlineOwnerStrategy,
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
