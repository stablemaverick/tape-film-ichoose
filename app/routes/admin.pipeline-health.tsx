import type { LoaderFunctionArgs } from "react-router";
import { useLoaderData } from "react-router";

import { PipelineHealthDashboard } from "../components/PipelineHealthDashboard";
import { loadPipelineHealthData } from "../lib/pipeline-health.server";

export async function loader({ request }: LoaderFunctionArgs) {
  const url = new URL(request.url);
  const key = url.searchParams.get("key") ?? "";
  const expected = process.env.PIPELINE_HEALTH_KEY ?? "";

  if (!expected) {
    throw new Response(
      "PIPELINE_HEALTH_KEY is not set. Add it to .env (same value you pass as ?key=).",
      { status: 503, headers: { "Content-Type": "text/plain; charset=utf-8" } },
    );
  }
  if (key !== expected) {
    throw new Response("Unauthorized", {
      status: 401,
      headers: { "Content-Type": "text/plain; charset=utf-8" },
    });
  }

  return await loadPipelineHealthData();
}

export default function AdminPipelineHealth() {
  const d = useLoaderData<typeof loader>();

  return (
    <PipelineHealthDashboard
      d={d}
      intro={
        <p style={{ color: "#444", fontSize: 14 }}>
          Operational dashboard: latest data from <code>pipeline_runs</code> and{" "}
          <code>catalog_health_snapshots</code>. Does not replace normal monitoring or scheduled
          syncs.
        </p>
      }
    />
  );
}
