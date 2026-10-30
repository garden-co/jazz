/**
 * How many ancestors a page grant reaches. The sidebar stops offering
 * "Add subpage" at this depth, so every page a person can create inherits
 * grants from the whole chain above it.
 */
export const PAGE_TREE_MAX_DEPTH = 8;
