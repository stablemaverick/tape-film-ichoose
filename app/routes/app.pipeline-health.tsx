import type { LoaderFunctionArgs } from "react-router";
import { useLoaderData } from "react-router";

import { PipelineHealthDashboard } from "../components/PipelineHealthDashboard";
import { loadPipelineHealthData } from "../lib/pipeline-health.server";
import { authenticate } from "../shopify.server";

export const loader = async ({ request }: LoaderFunctionArgs) => {
  await authenticate.admin(request);
  return await loadPipelineHealthData();
};

export default function AppPipelineHealth() {
  const d = useLoaderData<typeof loader>();

  return (
    <s-page heading="Pipeline health">
      <s-section heading="Observability">
        <PipelineHealthDashboard
          d={d}
          showTitle={false}
          intro={
            <p style={{ color: "#444", fontSize: 14 }}>
              Same view as <code>/admin/pipeline-health?key=…</code>, using your Shopify session.
              Data from <code>pipeline_runs</code> + <code>catalog_health_snapshots</code>.
            </p>
          }
        />
      </s-section>
    </s-page>
  );
}
