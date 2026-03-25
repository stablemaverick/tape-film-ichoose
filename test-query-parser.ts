import "dotenv/config";
import { parseQueryWithLLM } from "./app/lib/query-parser.server.ts";

async function run() {
  const queries = [
    "what star wars films do you have?",
    "best version of suspiria",
    "latest from criterion",
    "80s hong kong action",
    "films with tom cruise",
    "criterion horror",
  ];

  for (const query of queries) {
    try {
      const parsed = await parseQueryWithLLM(query);

      console.log("\n----------------------------------------");
      console.log("QUERY:");
      console.log(query);
      console.log("\nPARSED:");
      console.dir(parsed, { depth: null, colors: true });
    } catch (error) {
      console.log("\n----------------------------------------");
      console.log("QUERY:");
      console.log(query);
      console.log("\nERROR:");
      console.error(error);
    }
  }
}

run();
