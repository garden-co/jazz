#!/usr/bin/env bash

export JAZZ_APP_ID="replace-with-your-app-id"
export JAZZ_ADMIN_SECRET="replace-with-admin-secret"

jazz-tools server "$JAZZ_APP_ID" \
  --port 1625 \
  --data-dir ./data \
  --admin-secret "$JAZZ_ADMIN_SECRET"
