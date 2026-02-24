// Replace import.meta with an RN-safe stub (no Node "url" module). The worker path
// that uses import.meta.url is never run in RN; this only avoids parse/runtime errors.
function importMetaTransform(api) {
  const t = api.types;
  return {
    visitor: {
      MetaProperty(path) {
        if (path.node.meta.name === "import" && path.node.property.name === "meta") {
          path.replaceWith(
            t.objectExpression([t.objectProperty(t.identifier("url"), t.identifier("undefined"))]),
          );
        }
      },
    },
  };
}

module.exports = function (api) {
  api.cache(true);
  return {
    presets: [["babel-preset-expo", { unstable_transformImportMeta: true}]],
    plugins: [importMetaTransform, ['@babel/plugin-transform-flow-strip-types', {allowDeclareFields: true}]],
  };
};
