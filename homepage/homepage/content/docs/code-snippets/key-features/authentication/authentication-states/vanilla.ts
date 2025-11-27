const apiKey = "";
// #region Basic
// This comes from your own implementation.
// See https://jazz.tools/docs/vanilla/project-setup for more
// @ts-expect-error this doesn't exist
const { isAuthenticated } = authSecretStorage;
// #endregion

// #region SyncSettings
// This comes from your own implementation.
// See https://jazz.tools/docs/vanilla/project-setup for more
// @ts-expect-error this doesn't exist
const { me, logOut, authSecretStorage } = await createVanillaJazzApp({
  sync: {
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // Controls when sync is enabled for
    // both Anonymous Authentication and Authenticated Account
    when: "always", // or "signedUp" or "never"
  },
});
// #endregion

// #region DisableAnonSync
// @ts-expect-error this doesn't exist & redeclared
const { me, logOut, authSecretStorage } = await createVanillaJazzApp({
  sync: {
    peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
    // This makes the app work in local mode when using Anonymous Authentication
    when: "signedUp",
  },
});
// #endregion
