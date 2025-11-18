import { createCoValueSubscriptionContext } from "jazz-tools/react-core";
import { Organization } from "@/schema.ts";

export const {
  Provider: OrganizationProvider,
  useSelector: useOrganizationSelector,
} = createCoValueSubscriptionContext(Organization, { projects: true });
