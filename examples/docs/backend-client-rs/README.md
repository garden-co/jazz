# backend-client-docs

The docs' snippets for a Rust backend client: a machine that connects to a
TypeScript main backend's `/jazz` with its workload's OIDC token, trusted there
as a scoped machine (`trustWorkload({ …, as: (claims) => ({ userId }) })`). The
docs include them by region from `src/lib.rs`; this crate only has to compile.
It uses the todo app's bindings from `../todo-server-rs`.
