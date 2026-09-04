# INV-QUERY-1A

- Status: target
- Coverage: untested

## Invariant

A Groove node descriptor MUST fully encode every input that can affect node output, including authorization-relevant literals, policy bindings, and read-view source selection. Prepared claim inputs are encoded as one graph-wide declared parameter set (canonical names, paths where needed, and types), never as a subject's bound values: values bind per execution, so descriptor-identical graphs may share across identities. A claim used as a maintained-window partition dimension MAY occur structurally in an arrangement key, whose one arrangement holds all values; it MUST NOT occur as a baked subject-specific constant. This is the precondition for sharing one live node across multiple retention scopes: retainer tags do not participate in node identity, and sharing is valid only for descriptor-identical graphs with descriptor-identical canonical input refs.

## Enforced by (tests)

—

## Implementation

`jazz/SPEC/6_queries.md::6.3`
