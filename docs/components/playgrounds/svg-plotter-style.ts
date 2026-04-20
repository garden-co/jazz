type SvgStyleRules = {
  classRules: Map<string, Map<string, string>>;
  idRules: Map<string, Map<string, string>>;
  tagRules: Map<string, Map<string, string>>;
};

type StyleTarget = {
  classNames?: string[];
  id?: string | null;
  tagName?: string | null;
};

function getOrCreateRule(
  map: Map<string, Map<string, string>>,
  selector: string,
): Map<string, string> {
  const existing = map.get(selector);
  if (existing) return existing;

  const created = new Map<string, string>();
  map.set(selector, created);
  return created;
}

function parseDeclarations(block: string) {
  const declarations = new Map<string, string>();

  for (const declaration of block.split(";")) {
    const separatorIndex = declaration.indexOf(":");
    if (separatorIndex === -1) continue;

    const property = declaration.slice(0, separatorIndex).trim().toLowerCase();
    const value = declaration.slice(separatorIndex + 1).trim();

    if (property && value) {
      declarations.set(property, value);
    }
  }

  return declarations;
}

function mergeDeclarations(target: Map<string, string>, declarations: Map<string, string>) {
  for (const [property, value] of declarations) {
    target.set(property, value);
  }
}

export function parseEmbeddedSvgStyles(cssTexts: string[]) {
  const rules: SvgStyleRules = {
    classRules: new Map(),
    idRules: new Map(),
    tagRules: new Map(),
  };

  for (const cssText of cssTexts) {
    const normalizedCss = cssText.replace(/\/\*[\s\S]*?\*\//g, "");

    for (const match of normalizedCss.matchAll(/([^{}]+)\{([^{}]+)\}/g)) {
      const selectors = match[1]?.split(",").map((selector) => selector.trim()) ?? [];
      const declarations = parseDeclarations(match[2] ?? "");

      if (declarations.size === 0) continue;

      for (const selector of selectors) {
        if (/^\.[\w-]+$/.test(selector)) {
          mergeDeclarations(getOrCreateRule(rules.classRules, selector.slice(1)), declarations);
          continue;
        }

        if (/^#[\w-]+$/.test(selector)) {
          mergeDeclarations(getOrCreateRule(rules.idRules, selector.slice(1)), declarations);
          continue;
        }

        if (/^[a-z][\w:-]*$/i.test(selector)) {
          mergeDeclarations(getOrCreateRule(rules.tagRules, selector.toLowerCase()), declarations);
        }
      }
    }
  }

  return rules;
}

export function getEmbeddedSvgStyleValue(
  rules: ReturnType<typeof parseEmbeddedSvgStyles>,
  target: StyleTarget,
  property: string,
) {
  const normalizedProperty = property.toLowerCase();
  let resolvedValue: string | null = null;

  if (target.tagName) {
    const tagRule = rules.tagRules.get(target.tagName.toLowerCase());
    const tagValue = tagRule?.get(normalizedProperty);
    if (tagValue) {
      resolvedValue = tagValue;
    }
  }

  if (target.id) {
    const idValue = rules.idRules.get(target.id)?.get(normalizedProperty);
    if (idValue) {
      resolvedValue = idValue;
    }
  }

  for (const className of target.classNames ?? []) {
    const classValue = rules.classRules.get(className)?.get(normalizedProperty);
    if (classValue) {
      resolvedValue = classValue;
    }
  }

  return resolvedValue;
}
