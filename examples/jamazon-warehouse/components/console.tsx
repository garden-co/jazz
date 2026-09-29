"use client";

import { AppShell } from "@astryxdesign/core/AppShell";
import { Button } from "@astryxdesign/core/Button";
import { Selector } from "@astryxdesign/core/Selector";
import { SideNav, SideNavItem, SideNavSection } from "@astryxdesign/core/SideNav";
import { VStack } from "@astryxdesign/core/Stack";
import { Text } from "@astryxdesign/core/Text";
import { TopNav, TopNavHeading } from "@astryxdesign/core/TopNav";
import { useAll, useSession } from "jazz-tools/react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { createContext, useContext, useEffect, useState, type ReactNode } from "react";
import type { District, Warehouse } from "@/schema";
import { authClient, operatorToken } from "@/src/lib/auth-client";
import { consoleQueries } from "@/src/warehouse";
import { JoinWarehouse } from "./join-warehouse";
import { Loading } from "./providers";

export interface Scope {
  accountId: string;
  warehouse: Warehouse;
  district: District;
  districts: District[];
  /** The warehouse this operator is staffed on. */
  homeWarehouse: Warehouse;
  /** Writes are only permitted in the operator's own warehouse. */
  canOperate: boolean;
}

const ScopeContext = createContext<Scope | null>(null);

export function useScope(): Scope {
  const scope = useContext(ScopeContext);
  if (!scope) throw new Error("useScope must be used inside the console");
  return scope;
}

const NAV = [
  { href: "/", label: "Dashboard" },
  { href: "/orders/new", label: "New order" },
  { href: "/orders", label: "Pending orders" },
  { href: "/deliveries", label: "Delivery" },
  { href: "/payments", label: "Payment" },
  { href: "/orders/status", label: "Order status" },
  { href: "/stock", label: "Stock level" },
] as const;

/** Ask the trusted server route to seed (once) and, optionally, staff this operator. */
export async function bootstrap(warehouseId?: string): Promise<void> {
  const response = await fetch("/api/bootstrap", {
    method: "POST",
    credentials: "same-origin",
    headers: {
      authorization: `Bearer ${await operatorToken()}`,
      "content-type": "application/json",
    },
    body: JSON.stringify(warehouseId ? { warehouseId } : {}),
  });
  if (!response.ok) throw new Error("The warehouse could not be prepared. Try again.");
}

export function Console({ children }: { children: ReactNode }) {
  const accountId = useSession()?.user.account ?? undefined;
  const [prepared, setPrepared] = useState<"pending" | "ready" | Error>("pending");
  useEffect(() => {
    let cancelled = false;
    bootstrap().then(
      () => !cancelled && setPrepared("ready"),
      (error: Error) => !cancelled && setPrepared(error),
    );
    return () => {
      cancelled = true;
    };
  }, []);

  const memberships = useAll(accountId ? consoleQueries.membershipsOf(accountId) : undefined);
  const warehouses = useAll(consoleQueries.warehouses);

  if (prepared instanceof Error) throw prepared;
  if (prepared === "pending" || !accountId || !memberships.data || !warehouses.data) {
    return <Loading />;
  }
  const homeWarehouse = warehouses.data.find(
    (warehouse) => warehouse.id === memberships.data[0]?.warehouse_id,
  );
  if (!homeWarehouse) return <JoinWarehouse warehouses={warehouses.data} />;
  return (
    <ScopedShell accountId={accountId} homeWarehouse={homeWarehouse} warehouses={warehouses.data}>
      {children}
    </ScopedShell>
  );
}

function ScopedShell({
  accountId,
  homeWarehouse,
  warehouses,
  children,
}: {
  accountId: string;
  homeWarehouse: Warehouse;
  warehouses: Warehouse[];
  children: ReactNode;
}) {
  const pathname = usePathname();
  const [warehouseId, setWarehouseId] = useState(homeWarehouse.id);
  const [districtId, setDistrictId] = useState<string>();
  const warehouse = warehouses.find((candidate) => candidate.id === warehouseId) ?? homeWarehouse;
  const districts = useAll(consoleQueries.districtsOf(warehouse.id));
  const district =
    districts.data?.find((candidate) => candidate.id === districtId) ?? districts.data?.[0];

  const switcher = (
    <VStack gap={3}>
      <Selector
        label="Warehouse"
        size="sm"
        value={warehouse.id}
        onChange={(id) => {
          setWarehouseId(id);
          setDistrictId(undefined);
        }}
        options={warehouses.map((candidate) => ({
          value: candidate.id,
          label: candidate.name,
          description: candidate.id === homeWarehouse.id ? "Yours" : "View only",
        }))}
      />
      <Selector
        label="District"
        size="sm"
        value={district?.id ?? ""}
        onChange={setDistrictId}
        options={(districts.data ?? []).map((candidate) => ({
          value: candidate.id,
          label: candidate.name,
        }))}
      />
    </VStack>
  );

  return (
    <AppShell
      variant="section"
      height="auto"
      contentPadding={4}
      topNav={
        <TopNav
          label="Console"
          heading={<TopNavHeading heading="Jamazon Warehouse" headingHref="/" as={Link} />}
          endContent={
            <Button
              label="Sign out"
              variant="ghost"
              size="sm"
              onClick={() => void authClient.signOut()}
            />
          }
        />
      }
      sideNav={
        <SideNav
          topContent={switcher}
          footer={
            <Text type="supporting" color="secondary">
              Operating {homeWarehouse.name}
            </Text>
          }
        >
          <SideNavSection title="Operations" isHeaderHidden>
            {NAV.map((item) => (
              <SideNavItem
                key={item.href}
                as={Link}
                href={item.href}
                label={item.label}
                isSelected={pathname === item.href}
              />
            ))}
          </SideNavSection>
        </SideNav>
      }
    >
      {district ? (
        <ScopeContext.Provider
          value={{
            accountId,
            warehouse,
            district,
            districts: districts.data ?? [],
            homeWarehouse,
            canOperate: warehouse.id === homeWarehouse.id,
          }}
        >
          {children}
        </ScopeContext.Provider>
      ) : (
        <Loading />
      )}
    </AppShell>
  );
}
