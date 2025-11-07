import { Group, type GroupRole } from "../../internal.js";

/**
 * Configure how the owner of a new CoValue should be obtained.
 *
 * This configuration is overriden if an `owner` is explicitly provided when creating the CoValue.
 */
export type OnInlineCreateOptions =
  /**
   * Always create a new group for CoValues created inline.
   */
  | "newGroup"
  /**
   * Use the same owner as the container CoValue
   */
  | "equalsContainer"
  /**
   * Create a new group that includes the container CoValue's owner as a member (effectively inheriting
   * all permissions from the container)
   */
  | "extendsContainer"
  /**
   * Similar to "extendsContainer", but allows overriding the role of the container CoValue's owner.
   */
  | { extendsContainer: GroupRole }
  /**
   * Create a new group and configure it as needed
   */
  | GroupConfigurationCallback;

export type GroupConfigurationCallback = (
  newGroup: Group,
  context: { containerOwner: Group },
) => void;

/**
 * Permissions to be used when creating or composing CoValues
 * @param onInlineCreate - Defines how a nested CoValue’s owner is obtained when creating inline CoValues.
 * @default { onInlineCreate: "extendsContainer" }
 */
export type SchemaPermissions = { onInlineCreate?: OnInlineCreateOptions };

/**
 * Parsed {@link SchemaPermissions}, used by CoValue classes to set up permissions for referenced CoValues.
 */
export type RefPermissions = { newOwnerStrategy: NewOwnerStrategy };

/**
 * A function that creates a new owner for a new CoValue.
 * @param createNewGroup - A function that creates a new group.
 * @param containerOwner - The owner of the container CoValue. Can be undefined if the CoValue is created outside of a container.
 * @returns The new owner.
 */
export type NewOwnerStrategy = (
  createNewGroup: () => Group,
  containerOwner: Group,
) => Group;

export const extendContainerOwnerFactory =
  (roleOverride?: GroupRole): NewOwnerStrategy =>
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
  if (
    !permissions.onInlineCreate ||
    permissions.onInlineCreate === "extendsContainer"
  ) {
    return { newOwnerStrategy: extendContainerOwner };
  }
  if (
    typeof permissions.onInlineCreate === "object" &&
    "extendsContainer" in permissions.onInlineCreate
  ) {
    return {
      newOwnerStrategy: extendContainerOwnerFactory(
        permissions.onInlineCreate.extendsContainer,
      ),
    };
  }
  if (permissions.onInlineCreate === "newGroup") {
    return { newOwnerStrategy: (createNewGroup) => createNewGroup() };
  }
  if (permissions.onInlineCreate === "equalsContainer") {
    return {
      newOwnerStrategy: (createNewGroup, containerOwner) =>
        containerOwner ?? createNewGroup(),
    };
  }
  const groupConfigurationCallback = permissions.onInlineCreate;
  return {
    newOwnerStrategy: (createNewGroup, containerOwner) => {
      const newGroup = createNewGroup();
      groupConfigurationCallback(newGroup, { containerOwner });
      return newGroup;
    },
  };
}
