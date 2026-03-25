import { authenticate } from "../shopify.server";

export async function loader({ request }: { request: Request }) {
  const url = new URL(request.url);
  const handle = url.searchParams.get("handle") || "";

  const { admin } = await authenticate.admin(request);

  const response = await admin.graphql(
    `#graphql
    query ProductStatus($handle: String!) {
      productByHandle(handle: $handle) {
        id
        title
        handle
        totalInventory
        metafield(namespace: "custom", key: "preorder") {
          value
        }
        backorder: metafield(namespace: "custom", key: "backorder") {
          value
        }
        mediaReleaseDate: metafield(namespace: "custom", key: "media_release_date") {
          value
        }
        variants(first: 10) {
          edges {
            node {
              id
              title
              sku
              inventoryQuantity
              price
            }
          }
        }
      }
    }
  `,
    { variables: { handle } }
  );

  const json = await response.json();

  const product = json.data?.productByHandle;

  if (!product) {
    return Response.json({ error: "Product not found" });
  }

  const preorder = product.metafield?.value === "true";
  const backorder = product.backorder?.value === "true";
  const mediaReleaseDate = product.mediaReleaseDate?.value || null;

  let status = "out_of_stock";

  if (product.totalInventory > 0) status = "in_stock";
  else if (preorder) status = "preorder";
  else if (backorder) status = "backorder";

  return Response.json({
    product: {
      title: product.title,
      handle: product.handle,
      inventory: product.totalInventory,
      preorder,
      backorder,
      mediaReleaseDate,
      status,
    },
  });
}