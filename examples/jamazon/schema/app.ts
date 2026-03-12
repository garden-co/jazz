// AUTO-GENERATED FILE - DO NOT EDIT
import type { WasmSchema, QueryBuilder } from "jazz-tools";
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | { [key: string]: JsonValue }
  | JsonValue[];

export interface Product {
  id: string;
  name: string;
  brand: string;
  category: string;
  description: string;
  image_url: string;
  price_cents: number;
  rating: number;
  in_stock: number;
}

export interface CartItem {
  id: string;
  owner_id: string;
  product: string;
  quantity: number;
}

export interface Order {
  id: string;
  owner_id: string;
  created_at: Date;
  total_cents: number;
  item_count: number;
}

export interface OrderItem {
  id: string;
  order: string;
  product: string;
  quantity: number;
  unit_price_cents: number;
}

export interface ProductInit {
  name: string;
  brand: string;
  category: string;
  description: string;
  image_url: string;
  price_cents: number;
  rating: number;
  in_stock: number;
}

export interface CartItemInit {
  owner_id: string;
  product: string;
  quantity: number;
}

export interface OrderInit {
  owner_id: string;
  created_at: Date;
  total_cents: number;
  item_count: number;
}

export interface OrderItemInit {
  order: string;
  product: string;
  quantity: number;
  unit_price_cents: number;
}

export interface ProductWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  name?: string | { eq?: string; ne?: string; contains?: string };
  brand?: string | { eq?: string; ne?: string; contains?: string };
  category?: string | { eq?: string; ne?: string; contains?: string };
  description?: string | { eq?: string; ne?: string; contains?: string };
  image_url?: string | { eq?: string; ne?: string; contains?: string };
  price_cents?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  rating?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  in_stock?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface CartItemWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  owner_id?: string | { eq?: string; ne?: string; contains?: string };
  product?: string | { eq?: string; ne?: string };
  quantity?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface OrderWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  owner_id?: string | { eq?: string; ne?: string; contains?: string };
  created_at?:
    | Date
    | number
    | {
        eq?: Date | number;
        gt?: Date | number;
        gte?: Date | number;
        lt?: Date | number;
        lte?: Date | number;
      };
  total_cents?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  item_count?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface OrderItemWhereInput {
  id?: string | { eq?: string; ne?: string; in?: string[] };
  order?: string | { eq?: string; ne?: string };
  product?: string | { eq?: string; ne?: string };
  quantity?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
  unit_price_cents?:
    | number
    | { eq?: number; ne?: number; gt?: number; gte?: number; lt?: number; lte?: number };
}

export interface ProductInclude {
  cart_itemsViaProduct?: true | CartItemInclude | CartItemQueryBuilder;
  order_itemsViaProduct?: true | OrderItemInclude | OrderItemQueryBuilder;
}

export interface CartItemInclude {
  product?: true | ProductInclude | ProductQueryBuilder;
}

export interface OrderInclude {
  order_itemsViaOrder?: true | OrderItemInclude | OrderItemQueryBuilder;
}

export interface OrderItemInclude {
  order?: true | OrderInclude | OrderQueryBuilder;
  product?: true | ProductInclude | ProductQueryBuilder;
}

export interface ProductRelations {
  cart_itemsViaProduct: CartItem[];
  order_itemsViaProduct: OrderItem[];
}

export interface CartItemRelations {
  product: Product;
}

export interface OrderRelations {
  order_itemsViaOrder: OrderItem[];
}

export interface OrderItemRelations {
  order: Order;
  product: Product;
}

export type ProductWithIncludes<I extends ProductInclude = {}> = Product & {
  cart_itemsViaProduct?: NonNullable<I["cart_itemsViaProduct"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? CartItem[]
      : RelationInclude extends CartItemQueryBuilder<
            infer QueryInclude extends CartItemInclude,
            infer QuerySelect extends keyof CartItem | "*"
          >
        ? CartItemSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends CartItemInclude
          ? CartItemWithIncludes<RelationInclude>[]
          : never
    : never;
  order_itemsViaProduct?: NonNullable<I["order_itemsViaProduct"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? OrderItem[]
      : RelationInclude extends OrderItemQueryBuilder<
            infer QueryInclude extends OrderItemInclude,
            infer QuerySelect extends keyof OrderItem | "*"
          >
        ? OrderItemSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends OrderItemInclude
          ? OrderItemWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type CartItemWithIncludes<I extends CartItemInclude = {}> = CartItem & {
  product?: NonNullable<I["product"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Product
      : RelationInclude extends ProductQueryBuilder<
            infer QueryInclude extends ProductInclude,
            infer QuerySelect extends keyof Product | "*"
          >
        ? ProductSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ProductInclude
          ? ProductWithIncludes<RelationInclude>
          : never
    : never;
};

export type OrderWithIncludes<I extends OrderInclude = {}> = Order & {
  order_itemsViaOrder?: NonNullable<I["order_itemsViaOrder"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? OrderItem[]
      : RelationInclude extends OrderItemQueryBuilder<
            infer QueryInclude extends OrderItemInclude,
            infer QuerySelect extends keyof OrderItem | "*"
          >
        ? OrderItemSelectedWithIncludes<QueryInclude, QuerySelect>[]
        : RelationInclude extends OrderItemInclude
          ? OrderItemWithIncludes<RelationInclude>[]
          : never
    : never;
};

export type OrderItemWithIncludes<I extends OrderItemInclude = {}> = OrderItem & {
  order?: NonNullable<I["order"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Order
      : RelationInclude extends OrderQueryBuilder<
            infer QueryInclude extends OrderInclude,
            infer QuerySelect extends keyof Order | "*"
          >
        ? OrderSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends OrderInclude
          ? OrderWithIncludes<RelationInclude>
          : never
    : never;
  product?: NonNullable<I["product"]> extends infer RelationInclude
    ? RelationInclude extends true
      ? Product
      : RelationInclude extends ProductQueryBuilder<
            infer QueryInclude extends ProductInclude,
            infer QuerySelect extends keyof Product | "*"
          >
        ? ProductSelectedWithIncludes<QueryInclude, QuerySelect>
        : RelationInclude extends ProductInclude
          ? ProductWithIncludes<RelationInclude>
          : never
    : never;
};

export type ProductSelected<S extends keyof Product | "*" = keyof Product> = "*" extends S
  ? Product
  : Pick<Product, Extract<S | "id", keyof Product>>;

export type ProductSelectedWithIncludes<
  I extends ProductInclude = {},
  S extends keyof Product | "*" = keyof Product,
> = ProductSelected<S> & Omit<ProductWithIncludes<I>, keyof Product>;

export type CartItemSelected<S extends keyof CartItem | "*" = keyof CartItem> = "*" extends S
  ? CartItem
  : Pick<CartItem, Extract<S | "id", keyof CartItem>>;

export type CartItemSelectedWithIncludes<
  I extends CartItemInclude = {},
  S extends keyof CartItem | "*" = keyof CartItem,
> = CartItemSelected<S> & Omit<CartItemWithIncludes<I>, keyof CartItem>;

export type OrderSelected<S extends keyof Order | "*" = keyof Order> = "*" extends S
  ? Order
  : Pick<Order, Extract<S | "id", keyof Order>>;

export type OrderSelectedWithIncludes<
  I extends OrderInclude = {},
  S extends keyof Order | "*" = keyof Order,
> = OrderSelected<S> & Omit<OrderWithIncludes<I>, keyof Order>;

export type OrderItemSelected<S extends keyof OrderItem | "*" = keyof OrderItem> = "*" extends S
  ? OrderItem
  : Pick<OrderItem, Extract<S | "id", keyof OrderItem>>;

export type OrderItemSelectedWithIncludes<
  I extends OrderItemInclude = {},
  S extends keyof OrderItem | "*" = keyof OrderItem,
> = OrderItemSelected<S> & Omit<OrderItemWithIncludes<I>, keyof OrderItem>;

export const wasmSchema: WasmSchema = {
  products: {
    columns: [
      {
        name: "name",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "brand",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "category",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "description",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "image_url",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "price_cents",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "rating",
        column_type: {
          type: "Double",
        },
        nullable: false,
      },
      {
        name: "in_stock",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "True",
        },
      },
      insert: {},
      update: {},
      delete: {},
    },
  },
  cart_items: {
    columns: [
      {
        name: "owner_id",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "product",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "products",
      },
      {
        name: "quantity",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      update: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        with_check: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      delete: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
    },
  },
  orders: {
    columns: [
      {
        name: "owner_id",
        column_type: {
          type: "Text",
        },
        nullable: false,
      },
      {
        name: "created_at",
        column_type: {
          type: "Timestamp",
        },
        nullable: false,
      },
      {
        name: "total_cents",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "item_count",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
      },
      insert: {},
      update: {},
      delete: {},
    },
  },
  order_items: {
    columns: [
      {
        name: "order",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "orders",
      },
      {
        name: "product",
        column_type: {
          type: "Uuid",
        },
        nullable: false,
        references: "products",
      },
      {
        name: "quantity",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
      {
        name: "unit_price_cents",
        column_type: {
          type: "Integer",
        },
        nullable: false,
      },
    ],
    policies: {
      select: {
        using: {
          type: "True",
        },
      },
      insert: {},
      update: {},
      delete: {},
    },
  },
};

export class ProductQueryBuilder<
  I extends ProductInclude = {},
  S extends keyof Product | "*" = keyof Product,
> implements QueryBuilder<ProductSelectedWithIncludes<I, S>> {
  readonly _table = "products";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: ProductSelectedWithIncludes<I, S>;
  declare readonly _initType: ProductInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<ProductInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: ProductWhereInput): ProductQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Product | "*">(
    ...columns: [NewS, ...NewS[]]
  ): ProductQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends ProductInclude>(relations: NewI): ProductQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Product, direction: "asc" | "desc" = "asc"): ProductQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): ProductQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): ProductQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "cart_itemsViaProduct" | "order_itemsViaProduct"): ProductQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: ProductWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): ProductQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends ProductInclude = I,
    CloneS extends keyof Product | "*" = S,
  >(): ProductQueryBuilder<CloneI, CloneS> {
    const clone = new ProductQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class CartItemQueryBuilder<
  I extends CartItemInclude = {},
  S extends keyof CartItem | "*" = keyof CartItem,
> implements QueryBuilder<CartItemSelectedWithIncludes<I, S>> {
  readonly _table = "cart_items";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: CartItemSelectedWithIncludes<I, S>;
  declare readonly _initType: CartItemInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<CartItemInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: CartItemWhereInput): CartItemQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof CartItem | "*">(
    ...columns: [NewS, ...NewS[]]
  ): CartItemQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends CartItemInclude>(relations: NewI): CartItemQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof CartItem, direction: "asc" | "desc" = "asc"): CartItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): CartItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): CartItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "product"): CartItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: CartItemWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): CartItemQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends CartItemInclude = I,
    CloneS extends keyof CartItem | "*" = S,
  >(): CartItemQueryBuilder<CloneI, CloneS> {
    const clone = new CartItemQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class OrderQueryBuilder<
  I extends OrderInclude = {},
  S extends keyof Order | "*" = keyof Order,
> implements QueryBuilder<OrderSelectedWithIncludes<I, S>> {
  readonly _table = "orders";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: OrderSelectedWithIncludes<I, S>;
  declare readonly _initType: OrderInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<OrderInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: OrderWhereInput): OrderQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof Order | "*">(
    ...columns: [NewS, ...NewS[]]
  ): OrderQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends OrderInclude>(relations: NewI): OrderQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof Order, direction: "asc" | "desc" = "asc"): OrderQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): OrderQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): OrderQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "order_itemsViaOrder"): OrderQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: OrderWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): OrderQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends OrderInclude = I,
    CloneS extends keyof Order | "*" = S,
  >(): OrderQueryBuilder<CloneI, CloneS> {
    const clone = new OrderQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export class OrderItemQueryBuilder<
  I extends OrderItemInclude = {},
  S extends keyof OrderItem | "*" = keyof OrderItem,
> implements QueryBuilder<OrderItemSelectedWithIncludes<I, S>> {
  readonly _table = "order_items";
  readonly _schema: WasmSchema = wasmSchema;
  declare readonly _rowType: OrderItemSelectedWithIncludes<I, S>;
  declare readonly _initType: OrderItemInit;
  private _conditions: Array<{ column: string; op: string; value: unknown }> = [];
  private _includes: Partial<OrderItemInclude> = {};
  private _selectColumns?: string[];
  private _orderBys: Array<[string, "asc" | "desc"]> = [];
  private _limitVal?: number;
  private _offsetVal?: number;
  private _hops: string[] = [];
  private _gatherVal?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };

  where(conditions: OrderItemWhereInput): OrderItemQueryBuilder<I, S> {
    const clone = this._clone();
    for (const [key, value] of Object.entries(conditions)) {
      if (value === undefined) continue;
      if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        for (const [op, opValue] of Object.entries(value)) {
          if (opValue !== undefined) {
            clone._conditions.push({ column: key, op, value: opValue });
          }
        }
      } else {
        clone._conditions.push({ column: key, op: "eq", value });
      }
    }
    return clone;
  }

  select<NewS extends keyof OrderItem | "*">(
    ...columns: [NewS, ...NewS[]]
  ): OrderItemQueryBuilder<I, NewS> {
    const clone = this._clone<I, NewS>();
    clone._selectColumns = [...columns] as string[];
    return clone;
  }

  include<NewI extends OrderItemInclude>(relations: NewI): OrderItemQueryBuilder<I & NewI, S> {
    const clone = this._clone<I & NewI, S>();
    clone._includes = { ...this._includes, ...relations };
    return clone;
  }

  orderBy(column: keyof OrderItem, direction: "asc" | "desc" = "asc"): OrderItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._orderBys.push([column as string, direction]);
    return clone;
  }

  limit(n: number): OrderItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._limitVal = n;
    return clone;
  }

  offset(n: number): OrderItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._offsetVal = n;
    return clone;
  }

  hopTo(relation: "order" | "product"): OrderItemQueryBuilder<I, S> {
    const clone = this._clone();
    clone._hops.push(relation);
    return clone;
  }

  gather(options: {
    start: OrderItemWhereInput;
    step: (ctx: { current: string }) => QueryBuilder<unknown>;
    maxDepth?: number;
  }): OrderItemQueryBuilder<I, S> {
    if (options.start === undefined) {
      throw new Error("gather(...) requires start where conditions.");
    }
    if (typeof options.step !== "function") {
      throw new Error("gather(...) requires step callback.");
    }

    const maxDepth = options.maxDepth ?? 10;
    if (!Number.isInteger(maxDepth) || maxDepth <= 0) {
      throw new Error("gather(...) maxDepth must be a positive integer.");
    }
    if (Object.keys(this._includes).length > 0) {
      throw new Error("gather(...) does not support include(...) in MVP.");
    }
    if (this._hops.length > 0) {
      throw new Error("gather(...) must be called before hopTo(...).");
    }

    const currentToken = "__jazz_gather_current__";
    const stepOutput = options.step({ current: currentToken });
    if (
      !stepOutput ||
      typeof stepOutput !== "object" ||
      typeof (stepOutput as { _build?: unknown })._build !== "function"
    ) {
      throw new Error("gather(...) step must return a query expression built from app.<table>.");
    }

    const stepBuilt = JSON.parse(stepOutput._build()) as {
      table?: unknown;
      conditions?: Array<{ column: string; op: string; value: unknown }>;
      hops?: unknown;
    };

    if (typeof stepBuilt.table !== "string" || !stepBuilt.table) {
      throw new Error("gather(...) step query is missing table metadata.");
    }
    if (!Array.isArray(stepBuilt.conditions)) {
      throw new Error("gather(...) step query is missing condition metadata.");
    }

    const stepHops = Array.isArray(stepBuilt.hops)
      ? stepBuilt.hops.filter((hop): hop is string => typeof hop === "string")
      : [];
    if (stepHops.length !== 1) {
      throw new Error("gather(...) step must include exactly one hopTo(...).");
    }

    const currentConditions = stepBuilt.conditions.filter(
      (condition) => condition.op === "eq" && condition.value === currentToken,
    );
    if (currentConditions.length !== 1) {
      throw new Error(
        "gather(...) step must include exactly one where condition bound to current.",
      );
    }

    const currentCondition = currentConditions[0];
    const stepConditions = stepBuilt.conditions.filter(
      (condition) => !(condition.op === "eq" && condition.value === currentToken),
    );

    const withStart = this.where(options.start);
    const clone = withStart._clone();
    clone._hops = [];
    clone._gatherVal = {
      max_depth: maxDepth,
      step_table: stepBuilt.table,
      step_current_column: currentCondition.column,
      step_conditions: stepConditions,
      step_hops: stepHops,
    };

    return clone;
  }

  _build(): string {
    return JSON.stringify({
      table: this._table,
      conditions: this._conditions,
      includes: this._includes,
      select: this._selectColumns,
      orderBy: this._orderBys,
      limit: this._limitVal,
      offset: this._offsetVal,
      hops: this._hops,
      gather: this._gatherVal,
    });
  }

  toJSON(): unknown {
    return JSON.parse(this._build());
  }

  private _clone<
    CloneI extends OrderItemInclude = I,
    CloneS extends keyof OrderItem | "*" = S,
  >(): OrderItemQueryBuilder<CloneI, CloneS> {
    const clone = new OrderItemQueryBuilder<CloneI, CloneS>();
    clone._conditions = [...this._conditions];
    clone._includes = { ...this._includes };
    clone._selectColumns = this._selectColumns ? [...this._selectColumns] : undefined;
    clone._orderBys = [...this._orderBys];
    clone._limitVal = this._limitVal;
    clone._offsetVal = this._offsetVal;
    clone._hops = [...this._hops];
    clone._gatherVal = this._gatherVal
      ? {
          ...this._gatherVal,
          step_conditions: this._gatherVal.step_conditions.map((condition) => ({ ...condition })),
          step_hops: [...this._gatherVal.step_hops],
        }
      : undefined;
    return clone;
  }
}

export interface GeneratedApp {
  products: ProductQueryBuilder;
  cart_items: CartItemQueryBuilder;
  orders: OrderQueryBuilder;
  order_items: OrderItemQueryBuilder;
  wasmSchema: WasmSchema;
}

export const app: GeneratedApp = {
  products: new ProductQueryBuilder(),
  cart_items: new CartItemQueryBuilder(),
  orders: new OrderQueryBuilder(),
  order_items: new OrderItemQueryBuilder(),
  wasmSchema,
};
