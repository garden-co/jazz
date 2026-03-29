#!/usr/bin/env bash
# #region claude-code
claude mcp add jazz-docs -- npx jazz-tools@alpha mcp
# #endregion claude-code

# #region gemini
gemini mcp add jazz-docs npx jazz-tools@alpha mcp
# #endregion gemini

# #region codex
codex mcp add jazz-docs -- npx jazz-tools@alpha mcp
# #endregion codex

# #region opencode
opencode mcp add jazz-docs -- npx jazz-tools@alpha mcp
# #endregion opencode
