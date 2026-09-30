/**
 * Regression: legacy agent-query must not use catalogue stock/price as commerce authority.
 */

import { describe, expect, it } from "vitest";
import {
  mapOrderingAgentToAgentQueryResponse,
  scrubLegacyAgentOptions,
  shouldDelegateToOrderingAgent,
} from "../app/lib/ordering-agent-bridge.server";
import type { StructuredTapeAgentParse } from "../app/lib/tape-agent-query-parser.server";

const baseStructured = (over: Partial<StructuredTapeAgentParse> = {}): StructuredTapeAgentParse => ({
  primaryIntent: "availability",
  secondaryIntents: [],
  facets: { title: "True Romance", availabilityOnly: true },
  residualQuery: "True Romance",
  rawQuery: "Do you have True Romance?",
  ...over,
});

describe("ordering-agent-bridge", () => {
  it("delegates title availability intents to Ordering Agent", () => {
    expect(
      shouldDelegateToOrderingAgent(
        "Do you have True Romance on Blu-ray?",
        "all",
        baseStructured(),
      ),
    ).toBe(true);
  });

  it("does not delegate browse in_stock mode", () => {
    expect(
      shouldDelegateToOrderingAgent(
        "",
        "in_stock",
        baseStructured({ primaryIntent: "availability", facets: { availabilityBrowse: true, availabilityOnly: true } }),
      ),
    ).toBe(false);
  });

  it("scrubs supplier/cost fields from legacy options", () => {
    const scrubbed = scrubLegacyAgentOptions({
      title: "X",
      price: 12,
      costGbp: 5,
      supplierStock: 9,
      supplier_sku: "abc",
    });
    expect(scrubbed.costGbp).toBeUndefined();
    expect(scrubbed.supplierStock).toBeUndefined();
    expect(scrubbed.supplier_sku).toBeUndefined();
  });

  it("maps Ordering Agent answer without supplier leaks", () => {
    const mapped = mapOrderingAgentToAgentQueryResponse(
      {
        type: "answer",
        message: "Yes — Creepozoids is Available to Order — A$43.99.",
        conversation_id: "c1",
        release: {
          title: "Creepozoids Blu-Ray",
          availability: "available_to_order",
          price: 43.99,
          currency: "AUD",
          release_variant_id: "rid",
          shopify_listed: false,
          product_url: null,
          availability_label: "Available to Order — A$43.99",
        },
      },
      "Can you order Creepozoids?",
    );
    expect(mapped.commerceAuthority).toBe("inventory_intelligence");
    const rec = mapped.recommendedOption as Record<string, unknown>;
    expect(rec.price).toBe(43.99);
    expect(rec.costGbp).toBeUndefined();
    expect(rec.supplierStock).toBeUndefined();
    expect(JSON.stringify(mapped).toLowerCase()).not.toContain("lasgo");
  });
});
