/**
 * Route stock/price/order-availability intents to Ordering Agent V1 (II-backed).
 * Keeps browse/discovery modes on intelligence-search but strips supplier/cost leaks.
 */

import { spawnSync } from "node:child_process";
import path from "node:path";
import type { StructuredTapeAgentParse } from "./tape-agent-query-parser.server";

const COMMERCE_INTENT_RE =
  /\b(do you have|can you get|can i get|can you order|available|in stock|how much|price|order me|get me)\b/i;

export function shouldDelegateToOrderingAgent(
  message: string,
  intentMode: string,
  structured: StructuredTapeAgentParse,
): boolean {
  const mode = String(intentMode || "all");
  // Browse / discovery modes stay on catalogue search (non-authoritative for stock/price).
  if (
    mode === "new_releases" ||
    mode === "in_stock" ||
    mode === "preorders" ||
    mode === "director" ||
    mode === "label_studio" ||
    mode === "best_edition"
  ) {
    return false;
  }
  if (!message.trim()) return false;

  if (structured.primaryIntent === "availability" && !structured.facets.availabilityBrowse) {
    return true;
  }
  if (structured.primaryIntent === "title_lookup") return true;
  if (COMMERCE_INTENT_RE.test(message)) return true;
  return false;
}

export function runOrderingAgentCli(message: string, conversationId?: string): Record<string, unknown> {
  const root = process.cwd();
  const script = path.join(root, "scripts/ordering/run_ordering_agent.py");
  const python = path.join(root, "venv/bin/python");
  const envFile = process.env.ORDERING_AGENT_ENV_FILE || process.env.STOCK_AVAILABILITY_ENV_FILE || ".env";
  const args = [script, "--env-file", envFile, "--message", message];
  if (conversationId) {
    args.push("--conversation-id", conversationId);
  }
  const result = spawnSync(python, args, {
    cwd: root,
    encoding: "utf-8",
    maxBuffer: 4 * 1024 * 1024,
  });
  if (result.error) {
    return {
      type: "error",
      error: "AGENT_ERROR",
      message: "Sorry — I couldn’t complete that request right now.",
    };
  }
  try {
    return JSON.parse((result.stdout || "").trim() || "{}") as Record<string, unknown>;
  } catch {
    return {
      type: "error",
      error: "AGENT_ERROR",
      message: "Sorry — I couldn’t complete that request right now.",
    };
  }
}

/** Map Ordering Agent V1 response into a safe admin-UI-compatible payload. */
export function mapOrderingAgentToAgentQueryResponse(
  oa: Record<string, unknown>,
  message: string,
): Record<string, unknown> {
  const type = String(oa.type || "");
  const release = (oa.release || {}) as Record<string, unknown>;
  const choices = Array.isArray(oa.choices) ? (oa.choices as Record<string, unknown>[]) : [];

  const scrubbedOptions = choices.map((c) => ({
    title: c.title || c.label,
    format: c.format,
    releaseVariantId: c.release_variant_id,
    availabilityLabel: undefined,
    // Explicitly omit cost/supplier fields
  }));

  if (type === "answer" && release.release_variant_id) {
    const option = {
      title: release.title,
      format: release.format,
      price: release.price,
      currency: release.currency || "AUD",
      availability: release.availability,
      availabilityLabel: release.availability_label,
      releaseVariantId: release.release_variant_id,
      shopifyListed: release.shopify_listed === true,
      productUrl: release.product_url || null,
      // Never populate catalogue/supplier leaks:
      costGbp: undefined,
      supplierStock: undefined,
    };
    return {
      reply: oa.message,
      intent: "availability",
      commerceAuthority: "inventory_intelligence",
      recommendedOption: option,
      alternativeOptions: [],
      options: [option],
      conversation_id: oa.conversation_id,
      orderingAgent: {
        type,
        release,
      },
    };
  }

  if (type === "clarify") {
    return {
      reply: oa.message,
      intent: "availability",
      commerceAuthority: "inventory_intelligence",
      recommendedOption: null,
      alternativeOptions: scrubbedOptions,
      options: scrubbedOptions,
      conversation_id: oa.conversation_id,
      orderingAgent: { type, choices },
    };
  }

  return {
    reply: oa.message || "I couldn’t find that in the TAPE ordering catalogue.",
    intent: "availability",
    commerceAuthority: "inventory_intelligence",
    recommendedOption: null,
    alternativeOptions: [],
    options: [],
    conversation_id: oa.conversation_id,
    orderingAgent: { type, error: oa.error },
    error: type === "error" ? oa.error : undefined,
  };
}

/** Strip supplier/cost fields from legacy catalogue-shaped options. */
export function scrubLegacyAgentOptions<T extends Record<string, unknown>>(opt: T): T {
  const copy = { ...opt };
  delete copy.costGbp;
  delete copy.cost_gbp;
  delete copy.supplierStock;
  delete copy.supplier_stock;
  delete copy.supplierSku;
  delete copy.supplier_sku;
  delete copy.unit_cost;
  return copy;
}
