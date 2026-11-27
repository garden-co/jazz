// Instant is hosted: the browser syncs with it directly. This app's own
// routes sign people in (app/api/auth) and create profiles (app/api/bootstrap).
export default {
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
};
