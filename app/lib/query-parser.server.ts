import OpenAI from "openai";

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

export type ParsedCustomerQuery = {
  intent: "search" | "availability" | "preorder" | "person" | "best_edition";
  cleaned_query: string;
  franchise: string | null;
  person: string | null;
  studio: string | null;
  genre: string | null;
  year: number | null;
  decade: number | null;
  format: "4k" | "blu-ray" | "dvd" | null;
  availability_only: boolean;
  preorder_only: boolean;
  best_edition: boolean;
};

export async function parseQueryWithLLM(
  message: string,
): Promise<ParsedCustomerQuery> {
  const response = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "developer",
        content:
          [
            "You are a query parser for a film retail search assistant.",
            "Return only structured JSON matching the schema.",
            "Do not invent availability, prices, or search results.",
            "Infer likely franchise names where obvious.",
            "Extract a cleaned_query that is best for a catalog search.",
            "Correct likely misspellings of person names who are actors, actresses or directors, film titles, studios, and franchises when the intended meaning is clear.",
            "For example, 'cristian slater' should become 'Christian Slater' and 'tarentino' should become 'Tarantino'.",
            "If correcting a misspelling, use the corrected value in cleaned_query and in the structured field such as person, franchise, studio, or film title.",
            "Do not rewrite a query into a different film, person, or franchise unless the correction is very likely.",
            "If the intended correction is not reasonably clear, leave the original wording unchanged.",
            "Do not set availability_only to true unless the user explicitly asks for availability using phrases like 'in stock', 'available', 'what do you have', or 'coming soon'.",
            "Simple franchise or film queries like 'star wars films' should be treated as a normal search, not availability.",
            "Prefer conservative corrections: only correct when the intended film person, title, studio, or franchise is highly likely.",
            "For franchise questions like 'what star wars films do you have?', set franchise and use a cleaned_query that is just the franchise name.",
            "For availability-style browse questions like 'what star wars films do you have?' or 'what criterion films do you have?', set availability_only to true.",
            "For collection-style queries like '80s hong kong action', extract genre and decade separately, and keep cleaned_query minimal. Prefer 'hong kong action' or 'action' rather than repeating the full raw query.",
            "For studio + genre queries like 'criterion horror', set studio and genre, and cleaned_query should usually be the studio name only if no specific film title is present.",
            "For person queries like 'films with tom cruise', set person and use cleaned_query as just the person name.",
            "Never infer format from decade or year references.",
            "Do not set format unless the user explicitly mentions 4k, uhd, blu-ray, bluray, blu ray, or dvd.",
            "'80s' or '1980s' refers to decade, not format.",
            "For collection-style queries like '80s hong kong action', set decade to 1980, genre to action, and cleaned_query to 'hong kong action'. Leave format null unless explicitly stated.",
            "'latest from criterion' is a browse query. Set studio to Criterion and cleaned_query to 'criterion'.",
          ].join(" "),
      },
      {
        role: "user",
        content: message,
      },
    ],
    response_format: {
      type: "json_schema",
      json_schema: {
        name: "parsed_customer_query",
        strict: true,
        schema: {
          type: "object",
          additionalProperties: false,
          properties: {
            intent: {
              type: "string",
              enum: ["search", "availability", "preorder", "person", "best_edition"],
            },
            cleaned_query: { type: "string" },
            franchise: { type: ["string", "null"] },
            person: { type: ["string", "null"] },
            studio: { type: ["string", "null"] },
            genre: { type: ["string", "null"] },
            year: { type: ["number", "null"] },
            decade: { type: ["number", "null"] },
            format: {
              type: ["string", "null"],
              enum: ["4k", "blu-ray", "dvd", null],
            },
            availability_only: { type: "boolean" },
            preorder_only: { type: "boolean" },
            best_edition: { type: "boolean" },
          },
          required: [
            "intent",
            "cleaned_query",
            "franchise",
            "person",
            "studio",
            "genre",
            "year",
            "decade",
            "format",
            "availability_only",
            "preorder_only",
            "best_edition",
          ],
        },
      },
    },
    temperature: 0,
  });

  const content = response.choices[0]?.message?.content;

  if (!content) {
    throw new Error("No parser output returned from OpenAI.");
  }

  return JSON.parse(content) as ParsedCustomerQuery;
}
