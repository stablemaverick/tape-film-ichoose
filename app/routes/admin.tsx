import { Outlet } from "react-router";

/**
 * Layout shell for /admin/* routes (no Shopify session — each child handles its own auth).
 */
export default function AdminLayout() {
  return <Outlet />;
}
