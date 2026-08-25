use super::*;

/// Identity, claims, and policy mode used by policy augmentation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PolicyContext {
    /// Internal/system reads bypass row-level policy.
    System,
    /// Authenticated identity plus trusted server/session claims.
    Identity {
        /// Missing-policy behavior.
        mode: PolicyEnforcementMode,
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorSubject,
        /// Trusted claims available to policy queries.
        claims: BTreeMap<String, Value>,
        /// Author recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorSubject>,
    },
    /// A policy-authorization subplan evaluates claim-dependent policy logic
    /// for an identity. Its sources are system-authorized so dependency-table
    /// policies do not recursively compose into the policy being evaluated;
    /// the subplan's own predicates and authenticated claims remain enforced.
    AuthorizationSubplan {
        /// The source whose policy this subplan proves. Its own read policy is
        /// suspended to avoid recursive policy evaluation.
        protected_source: SourceId,
        /// Authorization kind being evaluated. Policy predicates select their
        /// operation-specific authority without recursively invoking source
        /// read policies.
        role: PolicyDecisionRole,
        /// Missing-policy behavior.
        mode: PolicyEnforcementMode,
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorSubject,
        /// Trusted claims available to policy queries.
        claims: BTreeMap<String, Value>,
        /// Author recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorSubject>,
    },
}

/// Missing-policy behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicyEnforcementMode {
    /// Local/offline runtimes without a compiled policy bundle remain usable.
    PermissiveLocal,
    /// Enforcing runtimes fail closed for missing explicit policy.
    Enforcing,
}

/// Stable policy identity used to decide whether compiled programs can share work.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PolicySharingKey {
    /// Internal/system policy bypass.
    System,
    /// Authenticated policy context. Claim values are runtime parameters; this
    /// key records only the claim paths that the lowered graph depends on.
    Identity {
        /// Missing-policy behavior.
        mode: PolicyEnforcementMode,
        /// Identity whose permissions are being evaluated.
        permission_subject: AuthorSubject,
        /// Identity recorded on writes, when it differs from the permission subject.
        attribution: Option<AuthorSubject>,
        /// Trusted claim paths referenced by the lowered graph.
        claim_paths: BTreeSet<ClaimPath>,
    },
}
