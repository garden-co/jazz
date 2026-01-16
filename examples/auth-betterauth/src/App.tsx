/**
 * Jazz + BetterAuth Demo App
 *
 * This demo shows how to:
 * 1. Authenticate users with BetterAuth
 * 2. Get JWT tokens with custom claims
 * 3. Connect to groove-server with the JWT
 * 4. Use claims in Jazz policies for access control
 */

import { useCallback, useEffect, useState } from "react";
import { SyncTest } from "./SyncTest";
import {
  type User,
  getJazzToken,
  getSession,
  signIn,
  signOut,
  signUp,
} from "./auth";

function App() {
  const [user, setUser] = useState<User | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Auth form state
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [name, setName] = useState("");
  const [isSignUp, setIsSignUp] = useState(false);

  // Check session on mount
  useEffect(() => {
    async function checkSession() {
      setLoading(true);
      const sessionUser = await getSession();
      if (sessionUser) {
        setUser(sessionUser);
        const jazzToken = await getJazzToken();
        setToken(jazzToken);
      }
      setLoading(false);
    }
    checkSession();
  }, []);

  const handleAuth = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      setError(null);
      setLoading(true);

      try {
        const result = isSignUp
          ? await signUp(email, password, name)
          : await signIn(email, password);

        if ("error" in result) {
          setError(result.error);
        } else {
          setUser(result.user);
          const jazzToken = await getJazzToken();
          setToken(jazzToken);
        }
      } catch (err) {
        setError(String(err));
      } finally {
        setLoading(false);
      }
    },
    [email, password, name, isSignUp],
  );

  const handleSignOut = useCallback(async () => {
    await signOut();
    setUser(null);
    setToken(null);
  }, []);

  if (loading) {
    return (
      <div className="card">
        <p>Loading...</p>
      </div>
    );
  }

  return (
    <div>
      <h1>Jazz + BetterAuth Demo</h1>

      {error && <div className="error">{error}</div>}

      {!user ? (
        <div className="card">
          <h2>{isSignUp ? "Sign Up" : "Sign In"}</h2>
          <form onSubmit={handleAuth}>
            {isSignUp && (
              <div>
                <input
                  type="text"
                  placeholder="Name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  required
                />
              </div>
            )}
            <div>
              <input
                type="email"
                placeholder="Email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                required
              />
            </div>
            <div>
              <input
                type="password"
                placeholder="Password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
              />
            </div>
            <button type="submit">{isSignUp ? "Sign Up" : "Sign In"}</button>
            <button
              type="button"
              className="secondary"
              onClick={() => setIsSignUp(!isSignUp)}
            >
              {isSignUp
                ? "Have an account? Sign In"
                : "Need an account? Sign Up"}
            </button>
          </form>
        </div>
      ) : (
        <>
          <div className="user-info">
            <strong>Logged in as:</strong> {user.name || user.email}
            <br />
            <small>ID: {user.id}</small>
            <br />
            <button onClick={handleSignOut}>Sign Out</button>
          </div>

          {token && <SyncTest token={token} userId={user.id} />}

          {token && (
            <div className="card">
              <h3>JWT Token (for Jazz)</h3>
              <p>
                This token is sent to groove-server for authentication. The
                claims in this token can be used in Jazz policies.
              </p>
              <pre
                data-testid="jwt-token"
                style={{
                  background: "#f5f5f5",
                  padding: "1rem",
                  overflow: "auto",
                  fontSize: "0.8rem",
                  maxHeight: "200px",
                }}
              >
                {token}
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
                {JSON.stringify(parseJwt(token), null, 2)}
              </pre>
            </div>
          )}

          <div className="card">
            <h3>Example Jazz Policies</h3>
            <p>With the JWT claims above, you can write policies like:</p>
            <pre
              style={{
                background: "#f0f0f0",
                padding: "1rem",
                overflow: "auto",
                fontSize: "0.85rem",
              }}
            >
              {`-- Access control using JWT claims

-- Only allow users to see their own documents
CREATE POLICY ON documents FOR SELECT
  WHERE author_id = @viewer;

-- Premium features only for pro/enterprise users
CREATE POLICY ON premium_features FOR SELECT
  WHERE @viewer.claims.subscriptionTier = 'pro'
     OR @viewer.claims.subscriptionTier = 'enterprise';

-- Organization-scoped documents
CREATE POLICY ON org_documents FOR SELECT
  WHERE org_id = @viewer.claims.orgId;

-- Role-based admin access
CREATE POLICY ON admin_settings FOR UPDATE
  WHERE @viewer.claims.roles CONTAINS 'admin';`}
            </pre>
          </div>

          <div className="card">
            <h3>groove-server Configuration</h3>
            <p>Configure groove-server to validate BetterAuth tokens:</p>
            <pre
              style={{
                background: "#f0f0f0",
                padding: "1rem",
                overflow: "auto",
                fontSize: "0.85rem",
              }}
            >
              {`# groove-server.toml
[auth]
provider = "betterauth"

[auth.jwt]
jwks_url = "http://localhost:3001/api/auth/jwks"
issuer = "http://localhost:3001"
user_id_claim = "sub"

[auth.provisioning]
auto_provision = true
users_table = "users"`}
            </pre>
          </div>
        </>
      )}
    </div>
  );
}

// Helper to decode JWT for display
function parseJwt(token: string): Record<string, unknown> {
  try {
    const base64Url = token.split(".")[1];
    const base64 = base64Url.replace(/-/g, "+").replace(/_/g, "/");
    const jsonPayload = decodeURIComponent(
      atob(base64)
        .split("")
        .map((c) => `%${(`00${c.charCodeAt(0).toString(16)}`).slice(-2)}`)
        .join(""),
    );
    return JSON.parse(jsonPayload);
  } catch {
    return { error: "Failed to decode token" };
  }
}

export default App;
