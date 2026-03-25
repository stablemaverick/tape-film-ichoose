import { authenticate } from "../shopify.server";

export async function loader({ request }: { request: Request }) {
  const url = new URL(request.url);
  const q = url.searchParams.get("q")?.trim() || "";

  if (!q) {
    return Response.json({ products: [] });
  }

  const { admin } = await authenticate.admin(request);

  const response = await admin.graphql(
    `#graphql
    query SearchProducts($query: String!) {
      products(first: 10, query: $query) {
        edges {
          node {
            id
            title
            handle
            totalInventory
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
      }
    }`,
    {
      variables: {
        query: `title:*${q}*`,
      },
    },
  );

  const json = await response.json();

  const products =
    json.data?.products?.edges?.map((edge: any) => ({
      id: edge.node.id,
      title: edge.node.title,
      handle: edge.node.handle,
      totalInventory: edge.node.totalInventory,
      variants:
        edge.node.variants?.edges?.map((variantEdge: any) => ({
          id: variantEdge.node.id,
          title: variantEdge.node.title,
          sku: variantEdge.node.sku,
          inventoryQuantity: variantEdge.node.inventoryQuantity,
          price: variantEdge.node.price,
        })) || [],
    })) || [];

  return Response.json({ products });
}