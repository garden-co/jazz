# Design: Group Asymmetric Key for Key Revelation

## Overview

This feature adds an asymmetric encryption key pair to groups, enabling non-members to reveal child group keys to parent groups without requiring the creation of per-member writeOnly keys.

### Problem Statement

When a user extends a child group to a parent group they don't have access to, they currently need to:

1. Create a symmetric "writeOnly key" in the parent group for themselves
2. Reveal this writeOnly key to all parent group members
3. Use this writeOnly key to encrypt the child's read key for the parent group

This approach has drawbacks:
- Creates a new symmetric key in the parent group for each non-member extension
- Increases storage overhead in the parent group
- Requires complex revelation chains for the writeOnly keys

### Proposed Solution

Add a group-level asymmetric key pair:
- **Public key** (`groupSealerID`): Stored in the group, visible to anyone who can load the group
- **Private key** (`groupSealerSecret`): Revealed only to actual members of the parent group via their personal sealer

Non-members can encrypt child key revelations using the parent group's public key. Members can decrypt using the private key they have access to.

## Architecture / Components

### New Fields in GroupShape

```typescript
export type GroupShape = {
  // Existing fields...
  readKey?: KeyID;
  
  // NEW: Group asymmetric key (public portion only)
  // The private key is NOT stored - it's derived from the readKey
  groupSealer?: SealerID;
  
  // NEW: Key revelations encrypted with group sealer (for non-member extensions)
  // stored as: `${KeyID}_for_${SealerID}`: SealedForGroup<KeySecret>
  [keyForSealer: `${KeyID}_for_${SealerID}`]: SealedForGroup<KeySecret>;
  
  // ... existing member roles, parent/child references
};
```

### New Crypto Type

```typescript
// Asymmetric encryption of a value to a group's sealer (one-way, no sender authentication)
export type SealedForGroup<T> = `sealedForGroup_U${string}` & { __type: T };
```

### Key Components Modified

1. **Group Creation** (`RawGroup.create`)
   - Generate a new group sealer key pair
   - Store the public key (`groupSealer`) in the group
   - Reveal the private key to the creating admin

2. **Member Addition** (`addMemberInternal`)
   - Reveal the group sealer secret to new members (same pattern as readKey)

3. **Key Rotation** (`rotateReadKey`)
   - Optionally rotate the group sealer (or keep it stable since it's only for receiving revelations)

4. **Extending Without Access** (`revealReadKeyToParentGroup`)
   - Replace writeOnly key creation with sealing to the parent's `groupSealer`
   - Use the new `sealForGroup` crypto operation

5. **Key Resolution** (`getUncachedReadKey`)
   - Add path to check for keys revealed via group sealer
   - Decrypt using the group sealer secret (available to members)

### Crypto Operations

Uses **anonymous box** pattern (similar to libsodium's `crypto_box_seal`):
- Sender generates ephemeral X25519 key pair
- Derives shared secret from ephemeral private key + recipient public key
- Encrypts message with derived key
- Sends ephemeral public key + ciphertext
- Recipient derives same shared secret from their private key + ephemeral public key

```typescript
abstract class CryptoProvider {
  // NEW: Derive group sealer key pair deterministically from read key
  // This ensures concurrent migrations by different admins produce the same result
  groupSealerFromReadKey(readKeySecret: KeySecret): { id: SealerID; secret: SealerSecret } {
    // Derive sealer secret using BLAKE3 with context
    const sealerBytes = this.blake3HashOnceWithContext(
      textEncoder.encode(readKeySecret),
      { context: textEncoder.encode("groupSealer") },
    );
    const secret = `sealerSecret_z${base58.encode(sealerBytes)}` as SealerSecret;
    return {
      secret,
      id: this.getSealerID(secret),
    };
  }

  // NEW: Encrypt data to a group's sealer (anonymous box - no sender authentication)
  // The ephemeral public key is embedded in the output
  abstract sealForGroup<T extends JsonValue>({
    message,
    to,
    nOnceMaterial,
  }: {
    message: T;
    to: SealerID;
    nOnceMaterial: { in: RawCoID; tx: TransactionID };
  }): SealedForGroup<T>;

  // NEW: Decrypt data sealed to a group
  // Extracts ephemeral public key from sealed data, derives shared secret
  abstract unsealForGroup<T extends JsonValue>(
    sealed: SealedForGroup<T>,
    groupSealerSecret: SealerSecret,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined;
}
```

## Data Models

### GroupShape Changes

```typescript
export type GroupShape = {
  // Core group key (unchanged)
  readKey?: KeyID;
  
  // NEW: Group-level asymmetric encryption key (public portion only)
  // Private key is derived from readKey, not stored
  groupSealer?: SealerID;
  
  // Member write keys (unchanged)
  [writeKeyFor: `writeKeyFor_${RawAccountID | AgentID}`]: KeyID;
  
  // Key revelations to members (unchanged)
  [revelationFor: `${KeyID}_for_${RawAccountID | AgentID}`]: Sealed<KeySecret>;
  [revelationFor: `${KeyID}_for_${Everyone}`]: KeySecret;
  [oldKeyForNewKey: `${KeyID}_for_${KeyID}`]: Encrypted<KeySecret, ...>;
  
  // NEW: Key revelations encrypted to group sealer (from non-members)
  [keyForSealer: `${KeyID}_for_${SealerID}`]: SealedForGroup<KeySecret>;
  
  // Member roles and relationships (unchanged)
  [member: RawAccountID | AgentID]: Role;
  [parent: `parent_${RawGroupID}`]: Role | "extend";
  [child: `child_${RawGroupID}`]: "extend" | "revoked";
};
```

### Migration Considerations

- Existing groups without `groupSealer` continue to work with writeOnly keys
- New groups get `groupSealer` on creation
- Optional migration: add `groupSealer` to existing groups on next admin action

## Implementation Flow

### Creating a Group

```typescript
private initializeGroupSealer() {
  const { secret: readKeySecret } = this.getCurrentReadKey();
  if (!readKeySecret) {
    throw new Error("Cannot initialize group sealer without read key");
  }
  
  // Derive group sealer deterministically from read key
  // This ensures concurrent initializations produce the same result
  const { id } = this.crypto.groupSealerFromReadKey(readKeySecret);
  
  // Store public key in group (idempotent - same value if already set)
  // Private key is NOT stored - members derive it from the read key
  this.set("groupSealer", id, "trusting");
}

// Get the group sealer secret by deriving it from the read key
getGroupSealerSecret(): SealerSecret | undefined {
  const { secret: readKeySecret } = this.getCurrentReadKey();
  if (!readKeySecret) return undefined;
  
  return this.crypto.groupSealerFromReadKey(readKeySecret).secret;
}
```

### Implications of Deterministic Derivation

1. **No storage overhead**: The sealer secret is derived on-demand, not stored or revealed
2. **Concurrent migration safety**: Multiple admins can call `initializeGroupSealer()` simultaneously and all will derive the same key pair
3. **Automatic access**: Any member with access to the read key automatically has access to the sealer secret

### Key Rotation

When the read key rotates, the group sealer must also be updated:

```typescript
rotateReadKey() {
  // ... existing read key rotation logic ...
  
  // Derive and store new group sealer from new read key
  const { id: newGroupSealer } = this.crypto.groupSealerFromReadKey(newReadKeySecret);
  this.set("groupSealer", newGroupSealer, "trusting");
}
```

**Decrypting old revelations**: When resolving keys, we need to check revelations against all historical group sealers, not just the current one:

```typescript
private getUncachedReadKey(keyID: KeyID): KeySecret | undefined {
  // ... existing checks ...
  
  // Check for revelation via ANY parent group sealer (current or historical)
  for (const parentGroup of this.getParentGroups()) {
    // Try current group sealer
    const currentSealer = parentGroup.get("groupSealer");
    if (currentSealer) {
      const key = this.tryDecryptWithSealer(keyID, parentGroup, currentSealer);
      if (key) return key;
    }
    
    // Try historical group sealers (derived from historical read keys)
    for (const historicalReadKey of parentGroup.getHistoricalReadKeys()) {
      const { id: historicalSealer } = this.crypto.groupSealerFromReadKey(historicalReadKey);
      const key = this.tryDecryptWithSealer(keyID, parentGroup, historicalSealer);
      if (key) return key;
    }
  }
}
```

This ensures:
- New extensions use the current group sealer
- Old revelations remain decryptable via historical sealers
- Members who have access to historical read keys can decrypt old revelations

### Extending Without Access (the key change)

```typescript
private revealReadKeyToParentGroup(
  parent: RawGroup,
  readKeyId: KeyID,
  readKeySecret: KeySecret,
  { revealAllWriteOnlyKeys }: { revealAllWriteOnlyKeys: boolean },
) {
  const parentGroupSealer = parent.get("groupSealer");
  
  if (!isAccountRole(parent.myRole())) {
    if (parentGroupSealer) {
      // NEW PATH: Use group sealer instead of writeOnly key
      this.storeKeyRevelationForGroupSealer(
        parent,
        parentGroupSealer,
        readKeyId,
        readKeySecret,
      );
    } else {
      // FALLBACK: Legacy groups without groupSealer
      const writeOnlyKeyID = parent.internalCreateWriteOnlyKeyForMember(
        this.core.node.getCurrentAgent().id,
        this.core.node.getCurrentAgent().currentAgentID(),
      );
      // ... existing logic
    }
    return;
  }
  
  // Existing path: member has access to parent's read key
  const { id: parentReadKeyID, secret: parentReadKeySecret } =
    parent.getCurrentReadKey();
  // ... existing symmetric encryption logic
}

private storeKeyRevelationForGroupSealer(
  parent: RawGroup,
  groupSealer: SealerID,
  childKeyID: KeyID,
  childKeySecret: KeySecret,
) {
  // Store in the CHILD group - encrypted to parent's public key
  this.set(
    `${childKeyID}_for_${groupSealer}`,
    this.crypto.sealForGroup({
      message: childKeySecret,
      to: groupSealer,
      nOnceMaterial: {
        in: this.id,
        tx: this.core.nextTransactionID(),
      },
    }),
    "trusting",
  );
}
```

### Resolving Keys (modified getUncachedReadKey)

```typescript
private getUncachedReadKey(keyID: KeyID): KeySecret | undefined {
  // ... existing direct revelation checks ...
  
  // NEW: Check for revelation via parent group sealer
  for (const parentGroup of this.getParentGroups()) {
    const groupSealer = parentGroup.get("groupSealer");
    if (!groupSealer) continue;
    
    // Look up the sealed revelation and its transaction info
    const sealedKeyEdit = this.getLastKeyEdit(`${keyID}_for_${groupSealer}`);
    if (!sealedKeyEdit) continue;
    
    const groupSealerSecret = parentGroup.getGroupSealerSecret();
    if (!groupSealerSecret) continue;
    
    // Recover nOnceMaterial from the stored transaction info
    const key = this.crypto.unsealForGroup(
      sealedKeyEdit.value,
      groupSealerSecret,
      {
        in: this.id,
        tx: sealedKeyEdit.tx,
      },
    );
    
    if (key) return key;
  }
  
  // ... existing key-for-key revelation checks ...
}
```

## Permission Validation

The `groupSealer` field follows the same permission rules as the `readKey` field:

### Transaction Validation (`packages/cojson/src/permissions.ts`)

```typescript
// In determineValidTransactionsForGroup, add groupSealer to the type union:
const change = changes[0] as
  | MapOpPayload<RawAccountID | AgentID | Everyone, Role>
  | MapOpPayload<"readKey", JsonValue>
  | MapOpPayload<"groupSealer", SealerID>  // NEW
  | MapOpPayload<"profile", CoID<RawProfile>>
  // ... other fields

// Add validation case for groupSealer:
if (change.key === "groupSealer") {
  if (!canAdmin(transactorRole)) {
    transaction.markInvalid("Only admins can set groupSealer");
    continue;
  }

  transaction.markValid();
  continue;
}
```

### Permission Rules

| Field | Who can set | Validation |
|-------|-------------|------------|
| `readKey` | Admin, Manager | `canAdmin(transactorRole)` |
| `groupSealer` | Admin, Manager | `canAdmin(transactorRole)` (same as readKey) |

### Rationale

- The `groupSealer` is derived from the `readKey`, so only users who can set the `readKey` should be able to set the `groupSealer`
- This maintains consistency: rotating the read key and updating the group sealer are logically coupled operations
- Prevents unauthorized users from setting an invalid group sealer that doesn't match the read key

## Security Considerations

1. **Forward Secrecy**: The group sealer does not provide forward secrecy. If the private key is compromised, all past revelations to it are compromised. This is acceptable because:
   - Key revelations are already visible to all group members
   - Compromise of any member already exposes the revealed keys

2. **Anonymous Sender**: Unlike the existing `seal` operation, `sealForGroup` doesn't authenticate the sender. This is intentional - we want non-members to be able to contribute. Permission validation happens at the CoValue level (checking if the sender is admin of the child group).

3. **Group Sealer Rotation**: Consider whether group sealer should rotate:
   - **No rotation needed**: Old revelations remain decryptable, which is desired
   - **Rotation on admin change**: Could be added later if needed

## Testing Strategy

### Integration Tests

```typescript
describe("Group Asymmetric Key", () => {
  it("should create groups with groupSealer", async () => {
    const { node } = await createTestNode();
    const group = node.createGroup();
    
    expect(group.get("groupSealer")).toBeDefined();
    expect(group.getGroupSealerSecret()).toBeDefined();
  });

  it("should reveal groupSealer to new members", async () => {
    const { node: adminNode } = await createTestNode();
    const { node: memberNode, account: member } = await createTestNode();
    
    const group = adminNode.createGroup();
    group.addMember(member, "writer");
    
    // Sync to member
    await syncNodes(adminNode, memberNode);
    
    const memberGroup = memberNode.expectCoValueLoaded(group.id);
    expect(memberGroup.getGroupSealerSecret()).toBeDefined();
  });

  it("should allow non-member to extend child to parent using groupSealer", async () => {
    const { node: parentAdmin } = await createTestNode();
    const { node: childAdmin } = await createTestNode();
    
    const parentGroup = parentAdmin.createGroup();
    const childGroup = childAdmin.createGroup();
    
    // childAdmin is NOT a member of parentGroup
    // But can still extend to it using the groupSealer
    childGroup.extend(parentGroup);
    
    // parentAdmin should be able to read child content
    await syncNodes(childAdmin, parentAdmin);
    
    const childAsParent = parentAdmin.expectCoValueLoaded(childGroup.id);
    expect(childAsParent.getCurrentReadKey().secret).toBeDefined();
  });

  it("should not create writeOnly key when parent has groupSealer", async () => {
    const { node: parentAdmin } = await createTestNode();
    const { node: childAdmin } = await createTestNode();
    
    const parentGroup = parentAdmin.createGroup();
    const childGroup = childAdmin.createGroup();
    
    const writeKeysBefore = parentGroup.getWriteOnlyKeys();
    
    childGroup.extend(parentGroup);
    
    const writeKeysAfter = parentGroup.getWriteOnlyKeys();
    expect(writeKeysAfter.length).toBe(writeKeysBefore.length);
  });

  it("should fallback to writeOnly key for legacy groups", async () => {
    // Test with a group that doesn't have groupSealer (simulated legacy)
    // Verify existing behavior still works
  });
});
```

### Edge Cases to Test

1. Parent group without `groupSealer` (legacy fallback)
2. Multiple non-member extensions to same parent
3. Key rotation in child after extension via groupSealer
4. Member removal from parent (still can decrypt old revelations)
5. Concurrent extensions from different non-members

## Design Decisions

1. **nOnceMaterial handling**: Store tx info alongside the sealed data. When decrypting, the transaction info is recovered from the CoValue edit history. This is more secure than deterministic nonces.

2. **Storage location**: Key revelations via group sealer are stored in the **child group** as `${childKeyID}_for_${parentGroupSealer}`. This keeps the parent group cleaner and follows the pattern that the child is revealing its keys.

3. **Crypto primitive**: Use anonymous box (`crypto_box_seal` style) with an ephemeral sender key per message. This is ideal since:
   - We don't need sender authentication (permission validation happens at CoValue level)
   - Provides perfect forward secrecy for the sender's identity
   - Standard, well-understood cryptographic pattern

---

Does the design look good? If so, we can move on to the implementation plan.
