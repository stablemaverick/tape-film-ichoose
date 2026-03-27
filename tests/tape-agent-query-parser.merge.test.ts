import assert from "node:assert/strict";
import { describe, test } from "node:test";
import {
  mergeLlmTapeAgentParse,
  parseTapeAgentQueryDeterministic,
} from "../app/lib/tape-agent-query-parser.server";

describe("mergeLlmTapeAgentParse (LLM enrichment only)", () => {
  test("never changes primaryIntent when LLM suggests franchise (facet may still enrich)", () => {
    const det = parseTapeAgentQueryDeterministic("hard boiled 4k");
    assert.equal(det.primaryIntent, "title_lookup");
    const out = mergeLlmTapeAgentParse(det, {
      franchise: "star wars",
      cleaned_query: "something else",
    });
    assert.equal(out.primaryIntent, "title_lookup");
    assert.equal(out.facets.title, "hard boiled");
    assert.equal(out.facets.franchise, "star wars");
    assert.equal(out.residualQuery, "hard boiled");
  });

  test("never applies commercial flags from LLM", () => {
    const det = parseTapeAgentQueryDeterministic("criterion films in stock");
    assert.equal(det.primaryIntent, "availability");
    assert.equal(det.facets.availabilityOnly, true);
    assert.equal(det.facets.availabilityBrowse, true);
    const out = mergeLlmTapeAgentParse(det, {
      cleaned_query: "ignore",
    });
    assert.equal(out.primaryIntent, "availability");
    assert.equal(out.facets.availabilityOnly, true);
    assert.equal(out.facets.preorderOnly, undefined);
  });

  test("fills missing franchise only; does not override deterministic franchise", () => {
    const det = parseTapeAgentQueryDeterministic("star wars 4k");
    assert.equal(det.facets.franchise, "star wars");
    const out = mergeLlmTapeAgentParse(det, { franchise: "other franchise" });
    assert.equal(out.facets.franchise, "star wars");
  });

  test("fills missing person from LLM", () => {
    const det: ReturnType<typeof parseTapeAgentQueryDeterministic> = {
      primaryIntent: "search",
      secondaryIntents: [],
      facets: {},
      residualQuery: "obscure",
      rawQuery: "obscure",
    };
    const out = mergeLlmTapeAgentParse(det, { person: "Jane Doe" });
    assert.equal(out.facets.person, "Jane Doe");
  });

  test("expands person when LLM is a clear superset (kurosawa → akira kurosawa)", () => {
    const det = parseTapeAgentQueryDeterministic("criterion films by kurosawa");
    assert.equal(det.facets.person, "kurosawa");
    const out = mergeLlmTapeAgentParse(det, { person: "Akira Kurosawa" });
    assert.equal(out.facets.person, "Akira Kurosawa");
    assert.equal(out.primaryIntent, "person");
    assert.equal(out.facets.personRole, "director");
  });

  test("does not replace person with unrelated LLM name", () => {
    const det = parseTapeAgentQueryDeterministic("films with nic cage");
    const out = mergeLlmTapeAgentParse(det, { person: "Meryl Streep" });
    assert.equal(out.facets.person, "nic cage");
  });

  test("does not set facets.title from LLM cleaned_query", () => {
    const det = parseTapeAgentQueryDeterministic("best edition of the third man");
    assert.ok(det.facets.title);
    const out = mergeLlmTapeAgentParse(det, {
      cleaned_query: "wrong title injection",
    });
    assert.equal(out.facets.title, det.facets.title);
  });

  test("fills residualQuery from cleaned_query only when deterministic residual empty", () => {
    const det: ReturnType<typeof parseTapeAgentQueryDeterministic> = {
      primaryIntent: "search",
      secondaryIntents: [],
      facets: {},
      residualQuery: "",
      rawQuery: "x",
    };
    const out = mergeLlmTapeAgentParse(det, { cleaned_query: "normalized text" });
    assert.equal(out.residualQuery, "normalized text");
  });

  test("does not overwrite non-empty residualQuery with cleaned_query", () => {
    const det = parseTapeAgentQueryDeterministic("hard boiled 4k");
    const out = mergeLlmTapeAgentParse(det, { cleaned_query: "replaced wrongly" });
    assert.equal(out.residualQuery, "hard boiled");
  });

  test("never touches release_date residual with cleaned_query", () => {
    const det = parseTapeAgentQueryDeterministic("when is possession released");
    assert.equal(det.primaryIntent, "release_date");
    const out = mergeLlmTapeAgentParse(det, { cleaned_query: "possession 2099" });
    assert.equal(out.residualQuery, det.residualQuery);
  });
});
