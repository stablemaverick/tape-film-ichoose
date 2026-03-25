import { authenticate } from "../shopify.server";

export async function action({ request }: { request: Request }) {
  const { admin } = await authenticate.admin(request);

  let body: any;
  try {
    body = await request.json();
  } catch {
    return Response.json({ error: "Invalid JSON body" }, { status: 400 });
  }

  const email = String(body.email || "").trim();
  const variantId = String(body.variantId || "").trim();
  const quantity = Number(body.quantity || 1);

  if (!email || !variantId) {
    return Response.json(
      { error: "Missing email or variantId" },
      { status: 400 },
    );
  }

  if (!Number.isFinite(quantity) || quantity < 1) {
    return Response.json(
      { error: "Quantity must be 1 or more" },
      { status: 400 },
    );
  }

  const response = await admin.graphql(
    `#graphql
    mutation CreateDraftOrder($input: DraftOrderInput!) {
      draftOrderCreate(input: $input) {
        draftOrder {
          id
          name
          invoiceUrl
          status
          totalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          lineItems(first: 10) {
            edges {
              node {
                title
                quantity
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    `,
    {
      variables: {
        input: {
          email,
          lineItems: [
            {
              variantId,
              quantity,
            },
          ],
        },
      },
    },
  );

  const json = await response.json();
  const result = json?.data?.draftOrderCreate;

  if (result?.userErrors?.length) {
    return Response.json(
      { error: result.userErrors },
      { status: 400 },
    );
  }

  return Response.json({
    draftOrder: result?.draftOrder || null,
  });
}