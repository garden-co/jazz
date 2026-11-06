# Start a local sync server for the app's backend. It verifies no user JWTs:
# users sign in through the backend (createJazz({ authenticate })).
# #region auth-server-cli
NODE_ENV=production \
jazz-tools server "$JAZZ_APP_ID" \
  --port 1625 \
  --data-dir ./data \
  --allow-local-first-auth \
  --backend-secret "$JAZZ_BACKEND_SECRET" \
  --admin-secret "$JAZZ_ADMIN_SECRET"
# #endregion auth-server-cli
