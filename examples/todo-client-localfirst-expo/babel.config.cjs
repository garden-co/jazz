module.exports = function (api) {
  api.cache(true);
  return {
    presets: [["babel-preset-expo", { unstable_transformImportMeta: true }]],
    plugins: [
      // Jazz codegen emits `declare` fields; the default flow-strip-types
      // plugin rejects them unless this option is set.
      ["@babel/plugin-transform-flow-strip-types", { allowDeclareFields: true }],
    ],
  };
};
