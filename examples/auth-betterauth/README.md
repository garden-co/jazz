# Jazz + BetterAuth Demo

This example demonstrates how to integrate Jazz with BetterAuth for authentication
and use JWT claims in ReBAC policies.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   React App     │────▶│  BetterAuth     │     │  groove-server  │
│   (Frontend)    │     │  (Auth Server)  │     │  (Jazz Server)  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │  1. Login/Signup      │                       │
         │─────────────────────▶│                       │
         │                       │                       │
         │  2. JWT Token         │                       │
         │◀─────────────────────│                       │
         │                       │                       │
         │  3. Connect with JWT  │                       │
         │──────────────────────────────────────────────▶│
         │                       │                       │
         │                       │  4. Validate JWT      │
         │                       │◀──────────────────────│
         │                       │                       │
         │  5. Sync with claims-aware policies          │
         │◀─────────────────────────────────────────────│
```

## Features Demonstrated

- **BetterAuth Integration**: Email/password authentication with custom JWT claims
- **Claims in Policies**: Use `@viewer.claims.subscriptionTier`, `@viewer.claims.orgId`, etc.
- **Array Claims**: Role-based access with `@viewer.claims.roles CONTAINS 'admin'`
- **Organization Scoping**: Multi-tenant access control with `org_id = @viewer.claims.orgId`
- **Auto-Provisioning**: Automatic Jazz user creation on first login

## Running the Demo

### Prerequisites

- Node.js 18+
- pnpm
- Rust toolchain (for groove-server)

### 1. Install Dependencies

```bash
cd examples/auth-betterauth
pnpm install
```

### 2. Start BetterAuth Server

```bash
pnpm run dev:auth
# Runs on http://localhost:3001
```

### 3. Start groove-server

```bash
cd ../../crates
cargo run -p groove-server -- --config ../examples/auth-betterauth/groove-server.toml
# Runs on http://localhost:8080
```

### 4. Start React App

```bash
pnpm run dev:client
# Runs on http://localhost:5173
```

Or run everything together:

```bash
pnpm run dev
```

## JWT Token Claims

BetterAuth is configured to include these claims in the JWT:

```json
{
  "sub": "user_123",
  "email": "user@example.com",
  "name": "John Doe",
  "subscriptionTier": "pro",
  "orgId": "org_456",
  "roles": ["member", "admin"]
}
```

## Example Policies

```sql
-- Personal documents
CREATE POLICY ON documents FOR SELECT
  WHERE author = @viewer;

-- Organization-scoped documents
CREATE POLICY ON org_documents FOR SELECT
  WHERE org_id = @viewer.claims.orgId;

-- Premium features by subscription tier
CREATE POLICY ON premium_features FOR SELECT
  WHERE @viewer.claims.subscriptionTier = 'pro'
     OR @viewer.claims.subscriptionTier = 'enterprise';

-- Admin-only settings
CREATE POLICY ON admin_settings FOR UPDATE
  WHERE @viewer.claims.roles CONTAINS 'admin';
```

## Configuration

### groove-server.toml

```toml
[auth]
provider = "betterauth"

[auth.jwt]
jwks_url = "http://localhost:3001/api/auth/jwks"
issuer = "http://localhost:3001"
user_id_claim = "sub"

[auth.provisioning]
auto_provision = true
users_table = "users"
```

### BetterAuth Server

The auth server (`server/auth-server.ts`) configures custom JWT claims:

```typescript
jwt: {
  definePayload: async ({ user, session }) => ({
    sub: user.id,
    email: user.email,
    name: user.name,
    subscriptionTier: user.subscriptionTier || "free",
    orgId: session.activeOrganizationId || null,
    roles: user.roles || ["member"],
  }),
}
```
