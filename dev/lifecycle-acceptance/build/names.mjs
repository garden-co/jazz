/** The npm names Jazz publishes, and nobody else may serve here. */
export const isJazzName = (name) => /^(jazz-|create-jazz$|@garden-co\/)/.test(name);
