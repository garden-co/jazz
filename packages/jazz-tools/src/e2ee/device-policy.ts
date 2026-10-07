import type { PolicyContext } from "../permissions/index.js";
import type { deviceRequestApp } from "./device-requests.js";

export function applyDeviceRequestPermissions({
  policy,
  session,
  allOf,
}: PolicyContext<typeof deviceRequestApp>): void {
  const recoveryRoots = policy.__e2ee_recovery_roots;
  recoveryRoots.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
  // Parsing future recovery authority is safe; authoring is enabled by account recovery.
  recoveryRoots.allowInsert.never();
  recoveryRoots.allowUpdate.never();
  recoveryRoots.allowDelete.never();
  const publicSuccessors = policy.__e2ee_public_account_successors;
  publicSuccessors.allowRead.where(
    session.where({ authMode: { in: ["local-first", "external"] } }),
  );
  publicSuccessors.allowInsert.where((record) =>
    allOf([
      { accountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_account_identities.exists.where({
        id: record.accountId,
        "$createdBy.account": session.user.account,
      }),
      policy.__e2ee_device_requests.exists.where({
        id: record.signerId,
        "$createdBy.account": session.user.account,
      }),
      policy.__e2ee_device_requests.exists.where({
        id: record.removedDeviceId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  publicSuccessors.allowUpdate.never();
  publicSuccessors.allowDelete.never();
  const publicApprovals = policy.__e2ee_public_device_approvals;
  publicApprovals.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
  // These policies bind references, not cryptographic eligibility.
  publicApprovals.allowInsert.where((record) =>
    allOf([
      { accountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_account_identities.exists.where({
        id: record.accountId,
        "$createdBy.account": session.user.account,
      }),
      policy.__e2ee_device_requests.exists.where({
        id: record.deviceId,
        "$createdBy.account": session.user.account,
      }),
      policy.__e2ee_device_requests.exists.where({
        id: record.signerId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  publicApprovals.allowUpdate.never();
  publicApprovals.allowDelete.never();
  const roots = policy.__e2ee_account_roots;
  roots.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
  roots.allowInsert.where((record) =>
    allOf([
      { accountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_account_identities.exists.where({
        id: record.accountId,
        deviceId: record.deviceId,
        epochId: record.epochId,
        ledgerVersion: record.ledgerVersion,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  roots.allowUpdate.never();
  roots.allowDelete.never();
  // Public keys are separate from the private enrolment handshake.
  const keys = policy.__e2ee_device_keys;
  keys.allowRead.where(session.where({ authMode: { in: ["local-first", "external"] } }));
  keys.allowInsert.where((record) =>
    allOf([
      { "$createdBy.account": session.user.account },
      policy.__e2ee_device_requests.exists.where({
        id: record.deviceId,
        "$createdBy.account": session.user.account,
        publicKey: record.publicKey,
        mechanism: record.mechanism,
        version: record.version,
        signingPublicKey: record.signingPublicKey,
        signingMechanism: record.signingMechanism,
        signingVersion: record.signingVersion,
      }),
    ]),
  );
  keys.allowUpdate.never();
  keys.allowDelete.never();
  const identities = policy.__e2ee_account_identities;
  const successors = policy.__e2ee_account_successors;
  successors.allowRead.where({
    accountId: session.user.account,
    "$createdBy.account": session.user.account,
  });
  successors.allowInsert.where((record) =>
    allOf([
      { accountId: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_account_identities.exists.where({
        id: record.accountId,
        "$createdBy.account": session.user.account,
      }),
      policy.__e2ee_device_requests.exists.where({
        id: record.signerId,
        "$createdBy.account": session.user.account,
      }),
      policy.__e2ee_device_requests.exists.where({
        id: record.removedDeviceId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  successors.allowUpdate.never();
  successors.allowDelete.never();
  identities.allowRead.where({
    id: session.user.account,
    "$createdBy.account": session.user.account,
  });
  identities.allowInsert.where((identity) =>
    allOf([
      { id: session.user.account, "$createdBy.account": session.user.account },
      policy.__e2ee_device_requests.exists.where({
        id: identity.deviceId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  identities.allowUpdate.never();
  identities.allowDelete.never();
  const requests = policy.__e2ee_device_requests;
  requests.allowRead.where({ "$createdBy.account": session.user.account });
  requests.allowInsert.where({ "$createdBy.account": session.user.account });
  requests.allowUpdate.never();
  requests.allowDelete.never();
  const challenges = policy.__e2ee_device_challenges;
  challenges.allowRead.where({ "$createdBy.account": session.user.account });
  challenges.allowInsert.where((challenge) =>
    allOf([
      { "$createdBy.account": session.user.account },
      policy.__e2ee_device_requests.exists.where({
        id: challenge.deviceId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  challenges.allowUpdate.never();
  challenges.allowDelete.never();
  policy.__e2ee_device_proofs.allowRead.where({ "$createdBy.account": session.user.account });
  policy.__e2ee_device_proofs.allowInsert.where((record) =>
    allOf([
      { "$createdBy.account": session.user.account },
      policy.__e2ee_device_challenges.exists.where({
        id: record.challengeId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  policy.__e2ee_device_proofs.allowUpdate.never();
  policy.__e2ee_device_proofs.allowDelete.never();
  policy.__e2ee_device_approvals.allowRead.where({ "$createdBy.account": session.user.account });
  policy.__e2ee_device_approvals.allowInsert.where((record) =>
    allOf([
      { "$createdBy.account": session.user.account },
      policy.__e2ee_device_challenges.exists.where({
        id: record.challengeId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  policy.__e2ee_device_approvals.allowUpdate.never();
  policy.__e2ee_device_approvals.allowDelete.never();
  policy.__e2ee_device_deliveries.allowRead.where({ "$createdBy.account": session.user.account });
  policy.__e2ee_device_deliveries.allowInsert.where((record) =>
    allOf([
      { "$createdBy.account": session.user.account },
      policy.__e2ee_device_challenges.exists.where({
        id: record.challengeId,
        "$createdBy.account": session.user.account,
      }),
    ]),
  );
  policy.__e2ee_device_deliveries.allowUpdate.never();
  policy.__e2ee_device_deliveries.allowDelete.never();
}
