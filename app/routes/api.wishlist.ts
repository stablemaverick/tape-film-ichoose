import { authenticate } from "../shopify.server";
import {
  addWishlistItem,
  enrichWishlistItemsWithCatalog,
  listWishlistItems,
  removeWishlistItem,
  resolveShopifyCustomerId,
} from "../lib/wishlist.server";

/**
 * GET  /api/wishlist?email=   — list items for Shopify customer (by email lookup)
 * POST /api/wishlist         — add item (JSON body)
 * DELETE /api/wishlist       — remove item (JSON body)
 */
export async function loader({ request }: { request: Request }) {
  try {
    const { admin, session } = await authenticate.admin(request);
    const url = new URL(request.url);
    const email = String(url.searchParams.get("email") || "").trim();
    if (!email) {
      return Response.json({ error: "Missing email query parameter" }, { status: 400 });
    }

    const customerId = await resolveShopifyCustomerId(admin, email);
    if (!customerId) {
      return Response.json(
        { error: "No Shopify customer found with this email on this shop" },
        { status: 404 },
      );
    }

    const result = await listWishlistItems(session.shop, customerId);
    if (!result.ok) {
      return Response.json({ error: result.error }, { status: 500 });
    }

    const items = await enrichWishlistItemsWithCatalog(result.items);

    return Response.json({
      ok: true,
      shopDomain: session.shop,
      items,
    });
  } catch (err) {
    return Response.json(
      {
        error: "Wishlist list failed",
        details: err instanceof Error ? err.message : String(err),
      },
      { status: 500 },
    );
  }
}

export async function action({ request }: { request: Request }) {
  try {
    const { admin, session } = await authenticate.admin(request);
    const shop = session.shop;

    if (request.method === "POST") {
      let body: Record<string, unknown>;
      try {
        body = (await request.json()) as Record<string, unknown>;
      } catch {
        return Response.json({ error: "Invalid JSON body" }, { status: 400 });
      }

      const email = String(body.email || "").trim();
      const catalogItemId = String(body.catalogItemId || "").trim();
      const titleSnapshot = String(body.title || body.titleSnapshot || "").trim();
      const filmId = body.filmId != null ? String(body.filmId) : null;
      const shopifyVariantId =
        body.shopifyVariantId != null ? String(body.shopifyVariantId) : null;

      if (!email || !catalogItemId || !titleSnapshot) {
        return Response.json(
          { error: "Missing email, catalogItemId, or title" },
          { status: 400 },
        );
      }

      const customerId = await resolveShopifyCustomerId(admin, email);
      if (!customerId) {
        return Response.json(
          { error: "No Shopify customer found with this email on this shop" },
          { status: 404 },
        );
      }

      const result = await addWishlistItem({
        shopDomain: shop,
        shopifyCustomerId: customerId,
        catalogItemId,
        filmId,
        shopifyVariantId,
        titleSnapshot,
        source: "agent_ui",
      });

      if (!result.ok) {
        return Response.json({ error: result.error }, { status: 500 });
      }

      return Response.json({
        ok: true,
        item: result.item,
        duplicate: result.duplicate,
      });
    }

    if (request.method === "DELETE") {
      let body: Record<string, unknown>;
      try {
        body = (await request.json()) as Record<string, unknown>;
      } catch {
        return Response.json({ error: "Invalid JSON body" }, { status: 400 });
      }

      const email = String(body.email || "").trim();
      const wishlistItemId = String(body.wishlistItemId || "").trim();
      if (!email || !wishlistItemId) {
        return Response.json(
          { error: "Missing email or wishlistItemId" },
          { status: 400 },
        );
      }

      const customerId = await resolveShopifyCustomerId(admin, email);
      if (!customerId) {
        return Response.json(
          { error: "No Shopify customer found with this email on this shop" },
          { status: 404 },
        );
      }

      const result = await removeWishlistItem(shop, customerId, wishlistItemId);
      if (!result.ok) {
        const status = result.error === "Not found" ? 404 : 500;
        return Response.json({ error: result.error }, { status });
      }

      return Response.json({ ok: true });
    }

    return Response.json({ error: "Method not allowed" }, { status: 405 });
  } catch (err) {
    return Response.json(
      {
        error: "Wishlist action failed",
        details: err instanceof Error ? err.message : String(err),
      },
      { status: 500 },
    );
  }
}
