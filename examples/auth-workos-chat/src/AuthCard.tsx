import type { User } from "@workos-inc/authkit-react";
import { CHAT_ID } from "../constants.js";

export type AuthCardProps = {
  role: string | null;
  statusDetail: string;
  user: User | null;
  onSignIn: () => void | Promise<void>;
  onSignOut: () => void | Promise<void>;
};

export function AuthCard({ role, statusDetail, user, onSignIn, onSignOut }: AuthCardProps) {
  return (
    <aside className="auth-card">
      <div className="status-card" data-testid="auth-status">
        <div className="status-copy">
          <span className="status-name">
            {user ? `${user.firstName} ${user.lastName}`.trim() : "Anonymous"}
          </span>
          <span className="status-detail">{statusDetail}</span>
        </div>
        {role ? <span className="admin-badge">{role}</span> : null}
      </div>

      {user ? (
        <div className="signed-in-card">
          <button
            type="button"
            data-testid="logout-button"
            onClick={() => {
              void onSignOut();
            }}
          >
            Log out
          </button>
        </div>
      ) : (
        <div className="signed-out-card">
          <button
            type="button"
            data-testid="workos-login"
            onClick={() => {
              void onSignIn();
            }}
          >
            Continue with WorkOS
          </button>
          <p className="helper-text">
            Use WorkOS to join <code>{CHAT_ID}</code>. Announcements stay visible while signed out.
          </p>
        </div>
      )}
    </aside>
  );
}
