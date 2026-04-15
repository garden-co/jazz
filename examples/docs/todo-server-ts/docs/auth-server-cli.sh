# Start a local server with external auth enabled.
# #region auth-server-cli
NODE_ENV=production \
jazz-tools server "$JAZZ_APP_ID" \
  --port 1625 \
  --data-dir ./data \
  --allow-local-first-auth \
  --jwks-url https://auth.example.com/.well-known/jwks.json \
  --backend-secret "$JAZZ_BACKEND_SECRET" \
  --admin-secret "$JAZZ_ADMIN_SECRET"
# #endregion auth-server-cli
