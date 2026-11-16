import { schema as s } from "jazz-tools";
import { schema } from "./schema";

const { definePermissions, exists, allowedTo, session, anyOf, allOf, gte, lte } = schema;

const isWarehouseOperator = (warehouse_id: s.Uuid) =>
  exists.warehouse_operators({ warehouse_id, account_id: session.user });

const operatorIdIsMe = { operator_id: session.user };

const isStock = (warehouse_id: s.Uuid, item_id: s.Uuid) => exists.stock({ warehouse_id, item_id });

export const permissions = definePermissions({
  better_auth_account: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_jwks: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_session: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_user: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  better_auth_verification: {
    read: false,
    insert: false,
    update: false,
    delete: false,
  },
  customers: {
    read: true,
    insert: (customer) =>
      allOf(
        anyOf(allowedTo.update(customer.warehouse), isWarehouseOperator(customer.warehouse_id)),
        customer.district.where({ warehouse_id: customer.warehouse_id }),
      ),
    updateIf: (customer) =>
      anyOf(allowedTo.update(customer.warehouse), isWarehouseOperator(customer.warehouse_id)),
    updateTo: (customer) =>
      allOf(
        anyOf(allowedTo.update(customer.warehouse), isWarehouseOperator(customer.warehouse_id)),
        customer.district.where({ warehouse_id: customer.warehouse_id }),
      ),
    delete: (customer) =>
      anyOf(allowedTo.update(customer.warehouse), isWarehouseOperator(customer.warehouse_id)),
  },
  deliveries: {
    read: true,
    insert: (delivery) =>
      allOf(
        anyOf(allowedTo.update(delivery.warehouse), isWarehouseOperator(delivery.warehouse_id)),
        delivery.order.where({
          warehouse_id: delivery.warehouse_id,
          district_id: delivery.district_id,
        }),
      ),
    updateIf: (delivery) =>
      anyOf(allowedTo.update(delivery.warehouse), isWarehouseOperator(delivery.warehouse_id)),
    updateTo: (delivery) =>
      allOf(
        anyOf(allowedTo.update(delivery.warehouse), isWarehouseOperator(delivery.warehouse_id)),
        delivery.order.where({
          warehouse_id: delivery.warehouse_id,
          district_id: delivery.district_id,
        }),
      ),
    delete: (delivery) =>
      anyOf(allowedTo.update(delivery.warehouse), isWarehouseOperator(delivery.warehouse_id)),
  },
  districts: {
    read: true,
    insert: (district) =>
      anyOf(allowedTo.update(district.warehouse), isWarehouseOperator(district.warehouse_id)),
    update: (district) =>
      anyOf(allowedTo.update(district.warehouse), isWarehouseOperator(district.warehouse_id)),
    delete: (district) =>
      anyOf(allowedTo.update(district.warehouse), isWarehouseOperator(district.warehouse_id)),
  },
  items: {
    read: true,
    insert: operatorIdIsMe,
    update: operatorIdIsMe,
    delete: operatorIdIsMe,
  },
  order_lines: {
    read: true,
    insert: (orderLine) =>
      allOf(
        anyOf(allowedTo.update(orderLine.warehouse), isWarehouseOperator(orderLine.warehouse_id)),
        orderLine.order.where({ warehouse_id: orderLine.warehouse_id }),
        isStock(orderLine.warehouse_id, orderLine.item_id),
      ),
    updateIf: (orderLine) =>
      anyOf(allowedTo.update(orderLine.warehouse), isWarehouseOperator(orderLine.warehouse_id)),
    updateTo: (orderLine) =>
      allOf(
        anyOf(allowedTo.update(orderLine.warehouse), isWarehouseOperator(orderLine.warehouse_id)),
        orderLine.order.where({ warehouse_id: orderLine.warehouse_id }),
        isStock(orderLine.warehouse_id, orderLine.item_id),
      ),
    delete: (orderLine) =>
      anyOf(allowedTo.update(orderLine.warehouse), isWarehouseOperator(orderLine.warehouse_id)),
  },
  orders: {
    read: true,
    insert: (order) =>
      allOf(
        anyOf(allowedTo.update(order.warehouse), isWarehouseOperator(order.warehouse_id)),
        order.district.where({ warehouse_id: order.warehouse_id }),
        order.customer.where({ warehouse_id: order.warehouse_id, district_id: order.district_id }),
      ),
    updateIf: (order) =>
      anyOf(allowedTo.update(order.warehouse), isWarehouseOperator(order.warehouse_id)),
    updateTo: (order) =>
      allOf(
        anyOf(allowedTo.update(order.warehouse), isWarehouseOperator(order.warehouse_id)),
        order.district.where({ warehouse_id: order.warehouse_id }),
        order.customer.where({ warehouse_id: order.warehouse_id, district_id: order.district_id }),
      ),
    delete: (order) =>
      anyOf(allowedTo.update(order.warehouse), isWarehouseOperator(order.warehouse_id)),
  },
  payments: {
    read: true,
    insert: (payment) =>
      allOf(
        anyOf(allowedTo.update(payment.warehouse), isWarehouseOperator(payment.warehouse_id)),
        payment.customer.where({ warehouse_id: payment.warehouse_id }),
        anyOf(
          { order_id: null },
          payment.order.where({
            warehouse_id: payment.warehouse_id,
            customer_id: payment.customer_id,
          }),
        ),
      ),
    updateIf: (payment) =>
      anyOf(allowedTo.update(payment.warehouse), isWarehouseOperator(payment.warehouse_id)),
    updateTo: (payment) =>
      allOf(
        anyOf(allowedTo.update(payment.warehouse), isWarehouseOperator(payment.warehouse_id)),
        payment.customer.where({ warehouse_id: payment.warehouse_id }),
        anyOf(
          { order_id: null },
          payment.order.where({
            warehouse_id: payment.warehouse_id,
            customer_id: payment.customer_id,
          }),
        ),
      ),
    delete: (payment) =>
      anyOf(allowedTo.update(payment.warehouse), isWarehouseOperator(payment.warehouse_id)),
  },
  stock: {
    read: true,
    insert: (stock) =>
      allOf(
        anyOf(allowedTo.update(stock.warehouse), isWarehouseOperator(stock.warehouse_id)),
        stock.item.where({}),
        allOf({ on_hand: gte(0) }, { reorder_level: gte(0) }, { reorder_level: lte(100) }),
      ),
    updateIf: (stock) =>
      anyOf(allowedTo.update(stock.warehouse), isWarehouseOperator(stock.warehouse_id)),
    updateTo: (stock) =>
      allOf(
        anyOf(allowedTo.update(stock.warehouse), isWarehouseOperator(stock.warehouse_id)),
        stock.item.where({}),
        allOf({ on_hand: gte(0) }, { reorder_level: gte(0) }, { reorder_level: lte(100) }),
      ),
    delete: (stock) =>
      anyOf(allowedTo.update(stock.warehouse), isWarehouseOperator(stock.warehouse_id)),
  },
  warehouse_operators: {
    read: true,
    insert: (warehouseOperator) => allowedTo.update(warehouseOperator.warehouse),
    update: false,
    delete: (warehouseOperator) =>
      anyOf(allowedTo.update(warehouseOperator.warehouse), { account_id: session.user }),
  },
  warehouses: {
    read: true,
    insert: operatorIdIsMe,
    updateIf: operatorIdIsMe,
    updateTo: (warehous) =>
      anyOf(
        operatorIdIsMe,
        exists.warehouse_operators({ warehouse_id: warehous.id, account_id: warehous.operator_id }),
      ),
  },
});
