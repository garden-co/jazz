/* istanbul ignore file -- @preserve */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { ItemsSym, TypeSym } from "./symbols.js";

(globalThis as any).devtoolsFormatters = [
  {
    header: (object: any) => {
      if (object[TypeSym] === "CoMap") {
        return ["div", {}, ["span", {}, object.constructor.name]];
      } else if (object[TypeSym] === "CoList") {
        return [
          "div",
          {},
          ["span", {}, object.constructor.name + "(" + object.length + ") "],
        ];
      } else if (object[TypeSym] === "Account") {
        return [
          "div",
          {},
          [
            "span",
            {},
            object.constructor.name +
              "(" +
              object.$jazz.refs.profile.value?.name +
              (object.isMe ? " ME" : "") +
              ")",
          ],
        ];
      } else {
        return null;
      }
    },
    hasBody: function () {
      return true;
    },
    body: function (object: any) {
      if (object[TypeSym] === "CoMap" || object[TypeSym] === "Account") {
        return [
          "div",
          { style: "margin-left: 15px" },
          ["div", "id: ", ["object", { object: object.id }]],
          ...Object.entries(object).map(([k, v]) => [
            "div",
            { style: "white-space: nowrap;" },
            ["span", { style: "font-weight: bold; opacity: 0.6" }, k, ": "],
            ["object", { object: v }],
            ...(typeof object._schema[k] === "function"
              ? v === null
                ? [
                    [
                      "span",
                      { style: "opacity: 0.5" },
                      ` (pending ${object._schema[k].name} `,
                      ["object", { object: object.$jazz.refs[k] }],
                      ")",
                    ],
                  ]
                : []
              : []),
          ]),
        ];
      } else if (object[TypeSym] === "CoList") {
        return [
          "div",
          { style: "margin-left: 15px" },
          ["div", "id: ", ["object", { object: object.id }]],
          ...(object as any[]).map((v, i) => [
            "div",
            { style: "white-space: nowrap;" },
            ["span", { style: "font-weight: bold; opacity: 0.6" }, i, ": "],
            ["object", { object: v }],
            ...(typeof object._schema[ItemsSym] === "function"
              ? v === null
                ? [
                    [
                      "span",
                      { style: "opacity: 0.5" },
                      ` (pending ${object._schema[ItemsSym].name} `,
                      ["object", { object: object.$jazz.refs[i] }],
                      ")",
                    ],
                  ]
                : []
              : []),
          ]),
        ];
      }
    },
  },
];
