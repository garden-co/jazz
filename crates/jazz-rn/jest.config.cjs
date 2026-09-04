/**
 * RN 0.81's preset setup is ESM, while this package's Jest 29 runner loads
 * setup files through CommonJS. The relay unit suite is a Node-level adapter
 * contract (all RN native modules are mocked), so use the RN Babel transform
 * without loading that incompatible device preset. Device behavior remains in
 * the installed-app acceptance suite.
 */
module.exports = {
  testEnvironment: "node",
  testMatch: ["<rootDir>/src/**/__tests__/**/*.[jt]s?(x)"],
  transform: {
    "^.+\\.[jt]sx?$": ["babel-jest", { presets: ["module:@react-native/babel-preset"] }],
  },
  modulePathIgnorePatterns: ["<rootDir>/example/node_modules", "<rootDir>/lib/"],
};
