export const jazzLight = {
  name: "jazz-light",
  type: "light",
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
        foreground: "#1aa245",
      },
    },
    {
      scope: ["string", "markup.fenced_code", "markup.inline"],
      settings: {
        foreground: "#4e3a2c",
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
        foreground: "#00a5a5",
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
        foreground: "#7b8bff",
      },
    },
    {
      scope: "variable.parameter.function",
      settings: {
        foreground: "#ff9800",
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
        foreground: "#445dd7",
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
