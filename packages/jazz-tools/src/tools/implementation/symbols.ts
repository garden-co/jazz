export type JazzToolsSymbol = ItemsMarker | MembersSym;

// this is only used as a key for marker fields to help the type system
export const ItemsMarker = "$items$";
export type ItemsMarker = typeof ItemsMarker;

export const MembersSym = "$members$";
export type MembersSym = typeof MembersSym;

export const TypeSym = "$type$";
export type TypeSym = typeof TypeSym;
