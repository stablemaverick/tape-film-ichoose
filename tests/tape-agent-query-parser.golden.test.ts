import assert from "node:assert/strict";
import { describe, test } from "node:test";
import { parseTapeAgentQueryDeterministic } from "../app/lib/tape-agent-query-parser.server";

function expectSubset(label: string, actual: unknown, expected: Record<string, unknown>) {
  for (const [k, v] of Object.entries(expected)) {
    if (k === "facets" && v && typeof v === "object") {
      expectSubset(`${label}.facets`, (actual as { facets: object }).facets, v as Record<string, unknown>);
      continue;
    }
    assert.deepEqual(
      (actual as Record<string, unknown>)[k],
      v,
      `${label}: field ${k}`,
    );
  }
}

describe("parseTapeAgentQueryDeterministic golden set", () => {
  const cases: {
    query: string;
    expect: Record<string, unknown>;
  }[] = [
    {
      query: "hard boiled 4k",
      expect: {
        primaryIntent: "title_lookup",
        facets: { title: "hard boiled", format: "4k" },
      },
    },
    {
      query: "films with nic cage",
      expect: {
        primaryIntent: "person",
        facets: { person: "nic cage", personRole: "cast" },
      },
    },
    {
      query: "films directed by carpenter",
      expect: {
        primaryIntent: "person",
        facets: { person: "carpenter", personRole: "director" },
      },
    },
    {
      query: "criterion films in stock",
      expect: {
        primaryIntent: "availability",
        secondaryIntents: ["discovery"],
        facets: {
          studio: "criterion",
          availabilityOnly: true,
          availabilityBrowse: true,
        },
      },
    },
    {
      query: "80s horror on blu ray",
      expect: {
        primaryIntent: "discovery",
        facets: { decade: 1980, genre: "Horror", format: "blu-ray" },
      },
    },
    {
      query: "what preorders do you have from Arrow",
      expect: {
        primaryIntent: "preorder",
        facets: { studio: "arrow", preorderOnly: true },
      },
    },
    {
      query: "when is possession released",
      expect: {
        primaryIntent: "release_date",
        facets: { title: "possession", releaseDateOnly: true },
      },
    },
    {
      query: "best edition of the third man",
      expect: {
        primaryIntent: "best_edition",
        facets: { title: "the third man", bestEdition: true },
      },
    },
    {
      query: "best 4k of alien in stock",
      expect: {
        primaryIntent: "best_edition",
        secondaryIntents: ["availability"],
        facets: {
          title: "alien",
          format: "4k",
          bestEdition: true,
          availabilityOnly: true,
        },
      },
    },
    {
      query: "criterion films by kurosawa",
      expect: {
        primaryIntent: "person",
        secondaryIntents: ["discovery"],
        facets: {
          studio: "criterion",
          person: "kurosawa",
          personRole: "director",
        },
      },
    },
    {
      query: "latest radiance titles",
      expect: {
        primaryIntent: "studio_browse",
        facets: { studio: "radiance", latest: true },
      },
    },
    {
      query: "star wars 4k",
      expect: {
        primaryIntent: "franchise",
        facets: { franchise: "star wars", format: "4k" },
      },
    },
    {
      query: "films from 1999",
      expect: {
        primaryIntent: "discovery",
        facets: { exactYear: 1999 },
      },
    },
    {
      query: "90s thrillers",
      expect: {
        primaryIntent: "discovery",
        facets: { decade: 1990, genre: "Thriller" },
      },
    },
    {
      query: "when does dune part two come out",
      expect: {
        primaryIntent: "release_date",
        facets: { releaseDateOnly: true },
      },
    },
    {
      query: "what preorders for alien",
      expect: {
        primaryIntent: "preorder",
        facets: { preorderOnly: true },
      },
    },
    {
      query: "is the thing available",
      expect: {
        primaryIntent: "availability",
        facets: { title: "the thing", availabilityOnly: true },
      },
    },
  ];

  for (const { query, expect: exp } of cases) {
    test(JSON.stringify(query), () => {
      const actual = parseTapeAgentQueryDeterministic(query);
      expectSubset(query, actual, exp);
    });
  }
});
