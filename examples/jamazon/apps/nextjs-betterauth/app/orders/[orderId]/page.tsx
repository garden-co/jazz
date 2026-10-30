"use client";

import { use } from "react";
import { OrderView } from "@/src/components/OrderView";

export default function OrderPage({ params }: { params: Promise<{ orderId: string }> }) {
  return <OrderView orderId={use(params).orderId} />;
}
