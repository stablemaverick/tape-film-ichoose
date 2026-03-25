import { authenticate } from "../shopify.server";
import { supabase } from "../lib/supabase.server";

export async function action({ request }: { request: Request }) {
  try {
    const { topic, payload } = await authenticate.webhook(request);

    if (topic !== "ORDERS_PAID" && topic !== "orders/paid") {
      return new Response("Ignored", { status: 200 });
    }

    const order = payload as any;

    const shopifyOrderId = String(order.id || "");
    const shopifyOrderName = String(order.name || "");
    const customerEmail =
      String(order.email || order?.customer?.email || "").trim() || null;

    const lineItems = Array.isArray(order.line_items) ? order.line_items : [];

    const supplierRows = lineItems
      .filter((item: any) => {
        const props = Array.isArray(item.properties) ? item.properties : [];
        return props.some(
          (p: any) =>
            String(p?.name || "").toLowerCase() === "source" &&
            String(p?.value || "").toLowerCase() === "catalog",
        );
      })
      .map((item: any) => {
        const props = Array.isArray(item.properties) ? item.properties : [];

        const getProp = (key: string) =>
          props.find(
            (p: any) => String(p?.name || "").toLowerCase() === key.toLowerCase(),
          )?.value ?? null;

        return {
          shopify_order_id: shopifyOrderId,
          shopify_order_name: shopifyOrderName,
          customer_email: customerEmail,
          supplier: getProp("supplier"),
          title: String(item.title || ""),
          product_code: getProp("product_code"),
          barcode: getProp("barcode"),
          quantity: Number(item.quantity || 1),
          unit_sale_price: item.price ? Number(item.price) : null,
          status: "pending",
        };
      });

    if (supplierRows.length > 0) {
      const { error } = await supabase.from("supplier_orders").insert(supplierRows);

      if (error) {
        console.error("Failed to insert supplier orders:", error);
        return new Response("Insert failed", { status: 500 });
      }
    }

    return new Response("OK", { status: 200 });
  } catch (error) {
    console.error("orders/paid webhook error:", error);
    return new Response("Webhook error", { status: 500 });
  }
}