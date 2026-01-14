/**
 * Jazz + WorkOS Demo App
 *
 * This demo shows how to:
 * 1. Authenticate users with WorkOS AuthKit (enterprise SSO)
 * 2. Use WorkOS automatic claims (org_id, role, permissions)
 * 3. Connect to groove-server with the JWT
 * 4. Use permissions-based access control in Jazz policies
 *
 * NOTE: This is a demonstration app. To run it, you need:
 * - A WorkOS account with AuthKit enabled
 * - Environment variables: VITE_WORKOS_CLIENT_ID
 */

import { useState } from "react";

// Mock WorkOS token for demonstration purposes
// In production, this would come from WorkOS AuthKit
const MOCK_WORKOS_TOKEN = {
  sub: "user_01H1234567890ABCDEF",
  org_id: "org_01H9876543210FEDCBA",
  role: "admin",
  permissions: [
    "documents:read",
    "documents:write",
    "documents:delete",
    "admin:settings",
    "users:read",
    "users:invite",
  ],
  sid: "session_01HABCDEF123456789",
  iss: "https://api.workos.com/",
  exp: Math.floor(Date.now() / 1000) + 3600,
};

function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [showMockLogin, setShowMockLogin] = useState(false);

  // In a real app, you'd use WorkOS AuthKit:
  // const { signIn, signOut, isAuthenticated } = useAuthKit();

  const handleMockLogin = () => {
    setIsAuthenticated(true);
    setShowMockLogin(false);
  };

  const handleLogout = () => {
    setIsAuthenticated(false);
  };

  return (
    <div>
      <h1>Jazz + WorkOS Demo</h1>
      <p>
        Enterprise SSO with automatic role and permissions claims for Jazz
        policies.
      </p>

      {!isAuthenticated ? (
        <div className="card">
          <h2>Sign In with WorkOS</h2>
          <p>
            WorkOS AuthKit provides enterprise SSO (SAML, OIDC) with automatic
            role and permissions claims from directory sync.
          </p>

          {!showMockLogin ? (
            <>
              <button onClick={() => setShowMockLogin(true)}>
                Sign in with SSO (Demo)
              </button>
              <p
                style={{ marginTop: "1rem", color: "#666", fontSize: "0.9rem" }}
              >
                Note: This demo uses mock data. In production, WorkOS AuthKit
                handles the full SSO flow.
              </p>
            </>
          ) : (
            <div style={{ marginTop: "1rem" }}>
              <p>
                In production, users would be redirected to their identity
                provider (Okta, Azure AD, Google Workspace, etc.)
              </p>
              <button onClick={handleMockLogin}>
                Continue with Demo Token
              </button>
              <button
                onClick={() => setShowMockLogin(false)}
                style={{ background: "#666" }}
              >
                Cancel
              </button>
            </div>
          )}
        </div>
      ) : (
        <>
          <div className="user-info">
            <strong>Logged in via WorkOS SSO</strong>
            <br />
            <small>User ID: {MOCK_WORKOS_TOKEN.sub}</small>
            <br />
            <small>Organization: {MOCK_WORKOS_TOKEN.org_id}</small>
            <br />
            <span className="role-badge">{MOCK_WORKOS_TOKEN.role}</span>
            <br />
            <button onClick={handleLogout} style={{ marginTop: "0.5rem" }}>
              Sign Out
            </button>
          </div>

          <div className="card">
            <h3>WorkOS Access Token Claims</h3>
            <p>
              WorkOS automatically includes these claims in the access token.
              They come from directory sync and role assignments in the WorkOS
              dashboard.
            </p>

            <table className="claims-table">
              <thead>
                <tr>
                  <th>Claim</th>
                  <th>Value</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                    <code>sub</code>
                  </td>
                  <td>{MOCK_WORKOS_TOKEN.sub}</td>
                  <td>WorkOS user ID</td>
                </tr>
                <tr>
                  <td>
                    <code>org_id</code>
                  </td>
                  <td>{MOCK_WORKOS_TOKEN.org_id}</td>
                  <td>Organization selected at sign-in</td>
                </tr>
                <tr>
                  <td>
                    <code>role</code>
                  </td>
                  <td>
                    <span className="role-badge">{MOCK_WORKOS_TOKEN.role}</span>
                  </td>
                  <td>Organization membership role</td>
                </tr>
                <tr>
                  <td>
                    <code>permissions</code>
                  </td>
                  <td>
                    {MOCK_WORKOS_TOKEN.permissions.map((p) => (
                      <span key={p} className="permission-badge">
                        {p}
                      </span>
                    ))}
                  </td>
                  <td>Permissions derived from roles</td>
                </tr>
                <tr>
                  <td>
                    <code>sid</code>
                  </td>
                  <td>{MOCK_WORKOS_TOKEN.sid}</td>
                  <td>Session ID</td>
                </tr>
              </tbody>
            </table>
          </div>

          <div className="card">
            <h3>Jazz Policies with WorkOS Claims</h3>
            <p>
              Use WorkOS claims directly in Jazz policies for enterprise access
              control:
            </p>

            <pre>
              {`-- Organization-scoped access
CREATE POLICY ON documents FOR SELECT
  WHERE org_id = @viewer.claims.org_id;

-- Permission-based access
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

-- Delete requires delete permission
CREATE POLICY ON documents FOR DELETE
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'documents:delete';

-- Admin-only settings by role
CREATE POLICY ON settings FOR UPDATE
  WHERE @viewer.claims.role = 'admin';

-- User management by permission
CREATE POLICY ON users FOR SELECT
  WHERE org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'users:read';

CREATE POLICY ON users FOR INSERT
  CHECK (
    @new.org_id = @viewer.claims.org_id
    AND @viewer.claims.permissions CONTAINS 'users:invite'
  );`}
            </pre>
          </div>

          <div className="card">
            <h3>groove-server Configuration</h3>
            <pre>
              {`# groove-server.toml
[auth]
provider = "workos"

[auth.jwt]
# WorkOS JWKS endpoint (replace CLIENT_ID with your client ID)
jwks_url = "https://api.workos.com/sso/jwks/client_01H..."
issuer = "https://api.workos.com/"
user_id_claim = "sub"

[auth.provisioning]
auto_provision = true
users_table = "users"`}
            </pre>
          </div>

          <div className="card">
            <h3>WorkOS Directory Sync</h3>
            <p>
              WorkOS automatically syncs users and groups from identity
              providers via SCIM. The synced data flows into JWT claims:
            </p>
            <ul>
              <li>
                <strong>Groups → Roles</strong>: Directory groups map to roles
                via the WorkOS Admin Portal
              </li>
              <li>
                <strong>Roles → Permissions</strong>: Each role has associated
                permission slugs
              </li>
              <li>
                <strong>Permissions in Token</strong>: All permissions flow into
                the <code>permissions</code> array claim
              </li>
            </ul>
            <p>
              This means your Jazz policies can check fine-grained permissions
              like{" "}
              <code>
                @viewer.claims.permissions CONTAINS 'documents:delete'
              </code>
              without any custom code.
            </p>
          </div>
        </>
      )}
    </div>
  );
}

export default App;
