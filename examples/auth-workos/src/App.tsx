/**
 * Jazz + WorkOS Demo App
 *
 * This demo shows how to:
 * 1. Authenticate users with WorkOS AuthKit (enterprise SSO)
 * 2. Use WorkOS automatic claims (org_id, role, permissions)
 * 3. Connect to groove-server with the JWT
 * 4. Use permissions-based access control in Jazz policies
 */

import { useAuth } from "@workos-inc/authkit-react";

// Helper to decode JWT for display
function parseJwt(token: string): Record<string, unknown> {
  try {
    const base64Url = token.split(".")[1];
    const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    const jsonPayload = decodeURIComponent(
      atob(base64)
        .split("")
        .map((c) => `%${`00${c.charCodeAt(0).toString(16)}`.slice(-2)}`)
        .join(""),
    );
    return JSON.parse(jsonPayload);
  } catch {
    return { error: "Failed to decode token" };
  }
}

function App() {
  const { isLoading, user, getAccessToken, signIn, signOut } = useAuth();

  if (isLoading) {
    return (
      <div className="card">
        <p>Loading...</p>
      </div>
    );
  }

  return (
    <div>
      <h1>Jazz + WorkOS Demo</h1>
      <p>
        Enterprise SSO with automatic role and permissions claims for Jazz
        policies.
      </p>

      {!user ? (
        <div className="card">
          <h2>Sign In with WorkOS</h2>
          <p>
            WorkOS AuthKit provides enterprise SSO (SAML, OIDC) with automatic
            role and permissions claims from directory sync.
          </p>

          <button onClick={() => signIn()}>Sign in with SSO</button>

          <p style={{ marginTop: "1rem", color: "#666", fontSize: "0.9rem" }}>
            This demo uses WorkOS Test SSO IdP. Click sign in to authenticate.
          </p>
        </div>
      ) : (
        <AuthenticatedContent
          user={user}
          getAccessToken={getAccessToken}
          signOut={signOut}
        />
      )}
    </div>
  );
}

function AuthenticatedContent({
  user,
  getAccessToken,
  signOut,
}: {
  user: {
    id: string;
    email: string;
    firstName?: string | null;
    lastName?: string | null;
  };
  getAccessToken: () => Promise<string>;
  signOut: () => Promise<void>;
}) {
  const [accessToken, setAccessToken] = useState<string | null>(null);
  const [tokenClaims, setTokenClaims] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Fetch access token on mount
  useEffect(() => {
    async function fetchToken() {
      try {
        const token = await getAccessToken();
        setAccessToken(token);
        setTokenClaims(parseJwt(token));
      } catch (err) {
        setError(`Failed to get access token: ${err}`);
      }
    }
    fetchToken();
  }, [getAccessToken]);

  const displayName =
    user.firstName && user.lastName
      ? `${user.firstName} ${user.lastName}`
      : user.email;

  return (
    <>
      <div className="user-info">
        <strong>Logged in via WorkOS SSO</strong>
        <br />
        <small>User: {displayName}</small>
        <br />
        <small>Email: {user.email}</small>
        <br />
        <small>User ID: {user.id}</small>
        <br />
        <button onClick={() => signOut()} style={{ marginTop: "0.5rem" }}>
          Sign Out
        </button>
      </div>

      {error && <div className="error">{error}</div>}

      {accessToken && tokenClaims && (
        <div className="card">
          <h3>WorkOS Access Token</h3>
          <p>
            This token is sent to groove-server for authentication. The claims
            can be used in Jazz policies.
          </p>

          <h4>Raw Token</h4>
          <pre
            style={{
              background: "#f5f5f5",
              padding: "1rem",
              overflow: "auto",
              fontSize: "0.8rem",
              maxHeight: "150px",
            }}
          >
            {accessToken}
          </pre>

          <h4>Decoded Claims</h4>
          <pre
            style={{
              background: "#f5f5f5",
              padding: "1rem",
              overflow: "auto",
              fontSize: "0.8rem",
            }}
          >
            {JSON.stringify(tokenClaims, null, 2)}
          </pre>

          <h4>Claims Table</h4>
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
                <td>{String(tokenClaims.sub || "N/A")}</td>
                <td>WorkOS user ID</td>
              </tr>
              <tr>
                <td>
                  <code>org_id</code>
                </td>
                <td>{String(tokenClaims.org_id || "N/A")}</td>
                <td>Organization selected at sign-in</td>
              </tr>
              <tr>
                <td>
                  <code>role</code>
                </td>
                <td>
                  {tokenClaims.role ? (
                    <span className="role-badge">
                      {String(tokenClaims.role)}
                    </span>
                  ) : (
                    "N/A"
                  )}
                </td>
                <td>Organization membership role</td>
              </tr>
              <tr>
                <td>
                  <code>permissions</code>
                </td>
                <td>
                  {Array.isArray(tokenClaims.permissions) ? (
                    tokenClaims.permissions.map((p) => (
                      <span key={String(p)} className="permission-badge">
                        {String(p)}
                      </span>
                    ))
                  ) : (
                    <em>None configured</em>
                  )}
                </td>
                <td>Permissions derived from roles</td>
              </tr>
              <tr>
                <td>
                  <code>sid</code>
                </td>
                <td>{String(tokenClaims.sid || "N/A")}</td>
                <td>Session ID</td>
              </tr>
              <tr>
                <td>
                  <code>iss</code>
                </td>
                <td>{String(tokenClaims.iss || "N/A")}</td>
                <td>Token issuer</td>
              </tr>
              <tr>
                <td>
                  <code>exp</code>
                </td>
                <td>
                  {tokenClaims.exp
                    ? new Date(Number(tokenClaims.exp) * 1000).toLocaleString()
                    : "N/A"}
                </td>
                <td>Token expiration</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

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
# WorkOS JWKS endpoint
jwks_url = "https://api.workos.com/sso/jwks/client_01JX28XKCGFWXHBMX2FW66JTRM"
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
          WorkOS automatically syncs users and groups from identity providers
          via SCIM. The synced data flows into JWT claims:
        </p>
        <ul>
          <li>
            <strong>Groups → Roles</strong>: Directory groups map to roles via
            the WorkOS Admin Portal
          </li>
          <li>
            <strong>Roles → Permissions</strong>: Each role has associated
            permission slugs
          </li>
          <li>
            <strong>Permissions in Token</strong>: All permissions flow into the{" "}
            <code>permissions</code> array claim
          </li>
        </ul>
        <p>
          This means your Jazz policies can check fine-grained permissions like{" "}
          <code>@viewer.claims.permissions CONTAINS 'documents:delete'</code>{" "}
          without any custom code.
        </p>
      </div>
    </>
  );
}

// Import React hooks
import { useEffect, useState } from "react";

export default App;
