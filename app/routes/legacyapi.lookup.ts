import { authenticate } from "../shopify.server";

export async function loader({ request }: { request: Request }) {
  const url = new URL(request.url);
  const q = url.searchParams.get("q")?.trim() || "";

  if (!q) {
    return Response.json({ error: "Missing query" });
  }

  const { admin } = await authenticate.admin(request);

// 1️⃣ Search products
const searchResponse = await admin.graphql(
  `#graphql
  query SearchProducts($query: String!) {
    products(first: 5, query: $query) {
      edges {
        node {
          id
          title
          handle
          vendor
          totalInventory

          filmReleased: metafield(namespace: "custom", key: "film_released") {
            value
          }

          studio: metafield(namespace: "custom", key: "studio") {
            value
          }

          director: metafield(namespace: "custom", key: "director") {
            value
          }

          preorder: metafield(namespace: "custom", key: "preorder") {
            value
          }

          backorder: metafield(namespace: "custom", key: "backorder") {
            value
          }

          mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") {
            value
          }

          variants(first: 1) {
            edges {
              node {
                id
                title
                price
                sku
                barcode
                inventoryQuantity
              }
            }
          }
        }
      }
    }
  }
  `,
  {
    variables: {
      query: `title:*${q}*`,
    },
  },
);

  const json = await searchResponse.json();

  const products =
  json.data?.products?.edges?.map((edge: any) => {
    const node = edge.node;
    const variant = node.variants.edges[0]?.node;

    const preorder = node.preorder?.value === "true";
    const backorder = node.backorder?.value === "true";

    let status = "out_of_stock";

    if (node.totalInventory > 0) status = "in_stock";
    else if (preorder) status = "preorder";
    else if (backorder) status = "backorder";

    return {
      id: node.id,
      title: node.title,
      handle: node.handle,
      vendor: node.vendor,

      price: variant?.price,
      variantId: variant?.id,
      sku: variant?.sku,
      barcode: variant?.barcode,
      variantTitle: variant?.title,

      inventory: node.totalInventory,
      status,

      director: node.director?.value || null,
      studio: node.studio?.value || null,
      filmReleased: node.filmReleased?.value || null,
      mediaReleaseDate: node.mediaReleaseDate?.value || null,

      preorder,
      backorder,
    };
  }) || [];
  return Response.json({ results: products });
}