# Jazz + WorkOS Demo

This example demonstrates how to integrate Jazz with WorkOS for enterprise SSO
authentication and use automatic role/permissions claims in ReBAC policies.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   React App     │────▶│     WorkOS      │     │  groove-server  │
│   (Frontend)    │     │   (AuthKit)     │     │  (Jazz Server)  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │  1. SSO Redirect      │                       │
         │─────────────────────▶│                       │
         │                       │                       │
         │  2. IdP Login         │                       │
         │   (Okta, Azure, etc.) │                       │
         │                       │                       │
         │  3. Access Token      │                       │
         │◀─────────────────────│                       │
         │   (with claims)       │                       │
         │                       │                       │
         │  4. Connect with JWT  │                       │
         │──────────────────────────────────────────────▶│
         │                       │                       │
         │                       │  5. Validate via JWKS │
         │                       │◀──────────────────────│
         │                       │                       │
         │  6. Sync with permission-aware policies      │
         │◀─────────────────────────────────────────────│
```

## WorkOS Automatic Claims

WorkOS access tokens automatically include these claims:

| Claim | Description | Example |
|-------|-------------|---------|
| `sub` | WorkOS user ID | `user_01H...` |
| `org_id` | Organization selected at sign-in | `org_01H...` |
| `role` | Organization membership role | `admin` |
| `permissions` | Array of permission slugs | `["documents:read", "admin:settings"]` |
| `sid` | Session ID | `session_01H...` |

These claims come from:
- **Directory Sync**: Groups from SCIM sync map to roles
- **Admin Portal**: Role → permission mappings configured in WorkOS
- **Sign-in Selection**: User selects organization at login

## Features Demonstrated

- **Enterprise SSO**: SAML/OIDC via Okta, Azure AD, Google Workspace, etc.
- **Permission-Based Policies**: `@viewer.claims.permissions CONTAINS 'documents:write'`
- **Role-Based Access**: `@viewer.claims.role = 'admin'`
- **Organization Scoping**: `org_id = @viewer.claims.org_id`
- **Team Membership**: `team_id IN @viewer.claims.groups`

## Running the Demo

### Prerequisites

- Node.js 18+
- pnpm
- WorkOS account with AuthKit enabled
- Rust toolchain (for groove-server)

### 1. Configure WorkOS

1. Create a WorkOS account at https://workos.com
2. Enable AuthKit in your environment
3. Configure at least one SSO connection (or use test user)
4. Note your Client ID for the JWKS URL

### 2. Environment Variables

The client ID is already configured in the demo. No environment variables needed.

### 3. Install Dependencies

```bash
cd examples/auth-workos
pnpm install
```

### 4. Start groove-server

The `groove-server.toml` is already configured:

```toml
[auth.jwt]
jwks_url = "https://api.workos.com/sso/jwks/client_01JX28XKCGFWXHBMX2FW66JTRM"
```

Then start the server:

```bash
cd ../../crates
cargo run -p groove-server -- --config ../examples/auth-workos/groove-server.toml
```

### 5. Start React App

```bash
pnpm run dev
# Runs on http://localhost:5174
```

## Example Policies

```sql
-- Permission-based document access
CREATE POLICY ON documents FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND (
      sensitivity = 'public'
      OR @viewer.claims.permissions CONTAINS 'documents:read'
    );

-- Write requires specific permission
CREATE POLICY ON documents FOR UPDATE
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'documents:write';

-- Admin-only by role
CREATE POLICY ON settings FOR UPDATE
  WHERE @viewer.claims.role = 'admin';

-- Team-based access via directory groups
CREATE POLICY ON projects FOR SELECT
  WHERE team_id IN @viewer.claims.groups;
```

## Configuration

### groove-server.toml

```toml
[auth]
provider = "workos"

[auth.jwt]
jwks_url = "https://api.workos.com/sso/jwks/client_01JX28XKCGFWXHBMX2FW66JTRM"
issuer = "https://api.workos.com/"
user_id_claim = "sub"

[auth.provisioning]
auto_provision = true
users_table = "users"
```

## WorkOS Dashboard Setup

### 1. Configure Roles

In the WorkOS Admin Portal, create roles like:
- `admin` - Full access
- `editor` - Read/write access
- `viewer` - Read-only access

### 2. Map Permissions

Assign permission slugs to each role:
- `admin`: `documents:*`, `users:*`, `admin:*`
- `editor`: `documents:read`, `documents:write`
- `viewer`: `documents:read`

### 3. Directory Sync

Connect your identity provider via SCIM to sync:
- Users and their groups
- Group → Role mappings
- Automatic deprovisioning

The synced data flows automatically into JWT claims!
