export const jazzDark = {
  name: "jazz-dark",
  type: "dark",
  fg: "#d1d1d1",
  settings: [
    {
      scope: [
        "keyword.operator.accessor",
        "meta.group.braces.round.function.arguments",
        "meta.template.expression",
        "markup.fenced_code meta.embedded.block",
      ],
      settings: {
        foreground: "#969696",
      },
    },
    {
      scope: "emphasis",
      settings: {
        fontStyle: "italic",
      },
    },
    {
      scope: ["strong", "markup.heading.markdown", "markup.bold.markdown"],
      settings: {
        fontStyle: "bold",
      },
    },
    {
      scope: ["markup.italic.markdown"],
      settings: {
        fontStyle: "italic",
      },
    },
    {
      scope: "meta.link.inline.markdown",
      settings: {
        fontStyle: "underline",
        foreground: "#ffab70",
      },
    },
    {
      scope: ["string", "markup.fenced_code", "markup.inline"],
      settings: {
        foreground: "#feb179",
      },
    },
    {
      scope: ["comment", "string.quoted.docstring.multi"],
      settings: {
        foreground: "#aaa",
      },
    },
    {
      scope: [
        "constant.numeric",
        "constant.language",
        "constant.other.placeholder",
        "constant.character.format.placeholder",
        "variable.language.this",
        "variable.other.object",
        "variable.other.class",
        "variable.other.constant",
        "meta.property-name",
        "meta.property-value",
        "support",
      ],
      settings: {
        foreground: "#2dc9c9",
      },
    },
    {
      scope: [
        "keyword",
        "storage.modifier",
        "storage.type",
        "storage.control.clojure",
        "entity.name.function.clojure",
        "entity.name.tag.yaml",
        "support.function.node",
        "support.type.property-name.json",
        "punctuation.separator.key-value",
        "punctuation.definition.template-expression",
      ],
      settings: {
        foreground: "#7b8bff",
      },
    },
    {
      scope: [
        "punctuation.definition.arguments",
        "punctuation.definition.dict",
        "punctuation.separator",
        "meta.function-call.arguments",
      ],
      settings: {
        foreground: "#bbbbbb",
      },
    },
    {
      scope: "variable.parameter.function",
      settings: {
        foreground: "#bbbbbb",
      },
    },
    {
      scope: [
        "support.function",
        "entity.name.type",
        "entity.other.inherited-class",
        "meta.function-call",
        "meta.instance.constructor",
        "entity.other.attribute-name",
        "entity.name.function",
        "constant.keyword.clojure",
      ],
      settings: {
        foreground: "#9babff",
      },
    },
    {
      scope: [
        "entity.name.tag",
        "string.quoted",
        "string.regexp",
        "string.interpolated",
        "string.template",
        "string.unquoted.plain.out.yaml",
        "keyword.other.template",
      ],
      settings: {
        foreground: "#42bb69",
      },
    },
  ],
};
