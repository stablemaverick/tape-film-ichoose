import type { CSSProperties, ReactNode } from "react";

import type {
  CatalogHealthSnapshotRow,
  HealthAlert,
  PipelineHealthLoaderData,
  PipelineRunRow,
} from "../lib/pipeline-health.server";

const th: CSSProperties = {
  padding: "8px 10px",
  borderBottom: "2px solid #d1d5db",
};
const td: CSSProperties = { padding: "8px 10px", verticalAlign: "top" };

function fmtNum(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number" && Number.isFinite(v)) return String(v);
  return String(v);
}

function fmtPct(v: unknown): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number" && Number.isFinite(v)) return `${v.toFixed(1)}%`;
  return String(v);
}

function runTimeUtc(r: PipelineRunRow): string {
  return (
    r.recorded_at ||
    r.created_at ||
    r.ended_at ||
    r.started_at ||
    "—"
  );
}

function alertColor(level?: string): string {
  if (level === "CRITICAL") return "#991b1b";
  if (level === "WARNING") return "#92400e";
  return "#374151";
}

function AlertsList({ alerts }: { alerts: HealthAlert[] }) {
  if (!alerts.length) {
    return <p style={{ color: "#166534" }}>No active alerts on latest snapshot.</p>;
  }
  return (
    <ul style={{ margin: 0, paddingLeft: 20 }}>
      {alerts.map((a, i) => (
        <li key={i} style={{ marginBottom: 8, color: alertColor(a.level) }}>
          <strong>{a.level ?? "INFO"}</strong>
          {a.metric ? ` · ${a.metric}` : ""}: {a.message ?? "—"}
        </li>
      ))}
    </ul>
  );
}

function MetricsSummary({ m }: { m: Record<string, unknown> }) {
  const cov = (m.coverage as Record<string, unknown> | undefined) ?? {};
  const lnk = (m.linkage as Record<string, unknown> | undefined) ?? {};
  const com = (m.commercial as Record<string, unknown> | undefined) ?? {};
  const exc = (m.exceptions as Record<string, unknown> | undefined) ?? {};

  const grid: ReactNode = (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))",
        gap: 12,
        fontSize: 13,
      }}
    >
      <div style={kpiBox}>
        <div style={kpiLabel}>Active catalog rows</div>
        <div style={kpiVal}>{fmtNum(cov.total_catalog_items_active)}</div>
      </div>
      <div style={kpiBox}>
        <div style={kpiLabel}>Films</div>
        <div style={kpiVal}>{fmtNum(cov.total_films)}</div>
      </div>
      <div style={kpiBox}>
        <div style={kpiLabel}>Film link % (film-classified)</div>
        <div style={kpiVal}>{fmtPct(lnk.film_link_pct)}</div>
      </div>
      <div style={kpiBox}>
        <div style={kpiLabel}>TMDB match rate %</div>
        <div style={kpiVal}>{fmtPct(lnk.tmdb_match_rate_pct)}</div>
      </div>
      <div style={kpiBox}>
        <div style={kpiLabel}>Missing sale price %</div>
        <div style={kpiVal}>{fmtPct(com.missing_sale_price_pct)}</div>
      </div>
      <div style={kpiBox}>
        <div style={kpiLabel}>Null barcode rows</div>
        <div style={kpiVal}>{fmtNum(exc.null_barcode_rows)}</div>
      </div>
      <div style={kpiBox}>
        <div style={kpiLabel}>Duplicate films (tmdb_id)</div>
        <div style={kpiVal}>{fmtNum(exc.duplicate_films_by_tmdb_id)}</div>
      </div>
    </div>
  );

  return grid;
}

const kpiBox: CSSProperties = {
  border: "1px solid #e5e7eb",
  borderRadius: 8,
  padding: "10px 12px",
  background: "#fafafa",
};
const kpiLabel: CSSProperties = { fontSize: 11, color: "#6b7280", marginBottom: 4 };
const kpiVal: CSSProperties = { fontWeight: 600, fontSize: 15 };

export function PipelineHealthDashboard({
  d,
  intro,
  showTitle = true,
}: {
  d: PipelineHealthLoaderData;
  intro?: ReactNode;
  showTitle?: boolean;
}) {
  const alerts = d.latestHealth?.alerts ?? [];
  const metrics =
    d.latestHealth?.metrics && typeof d.latestHealth.metrics === "object"
      ? d.latestHealth.metrics
      : null;

  return (
    <div
      style={{
        fontFamily: "system-ui, sans-serif",
        padding: showTitle ? 24 : 0,
        maxWidth: 1200,
      }}
    >
      {showTitle ? <h1 style={{ marginTop: 0 }}>Pipeline health</h1> : null}
      {intro}

      <p style={{ fontSize: 13 }}>
        <strong>Source:</strong>{" "}
        <code>{d.source === "supabase" ? "Supabase (pipeline_runs + catalog_health_snapshots)" : "Local file (fallback)"}</code>
      </p>
      {d.banner ? (
        <p
          style={{
            background: "#fef3c7",
            border: "1px solid #fcd34d",
            padding: 10,
            borderRadius: 8,
            fontSize: 13,
          }}
        >
          {d.banner}
        </p>
      ) : null}

      {d.source === "file" && d.historyPath ? (
        <p style={{ fontSize: 13 }}>
          <strong>File:</strong> <code>{d.historyPath}</code>
          <br />
          <strong>Schema:</strong> {d.schemaVersion ?? "—"}
          <br />
          <strong>File mtime:</strong> {d.fileMtime ?? "—"}
        </p>
      ) : null}

      {d.readError ? (
        <p style={{ color: "#b91c1c", fontWeight: 600 }}>Read error: {d.readError}</p>
      ) : null}

      <section style={{ marginTop: 28 }}>
        <h2 style={{ fontSize: 18, marginBottom: 12 }}>Current catalog health</h2>
        {d.source === "file" && !d.latestHealth ? (
          <p style={{ color: "#6b7280" }}>
            Full health metrics are only stored in <code>catalog_health_snapshots</code> when using
            Supabase. File fallback shows run history only.
          </p>
        ) : !d.latestHealth ? (
          <div
            style={{
              border: "1px dashed #d1d5db",
              borderRadius: 8,
              padding: 20,
              color: "#6b7280",
            }}
          >
            <strong>No health snapshot yet.</strong> After the next catalog or stock sync,{" "}
            <code>append_pipeline_run_history</code> will insert into{" "}
            <code>catalog_health_snapshots</code>.
          </div>
        ) : (
          <>
            <p style={{ fontSize: 13 }}>
              <strong>Snapshot</strong>{" "}
              <code>{d.latestHealth.generated_at ?? d.latestHealth.created_at ?? "—"}</code>
              {" · "}
              <strong>exit_code</strong>{" "}
              <code>{fmtNum(d.latestHealth.exit_code)}</code> (0 ok, 1 warn, 2 critical)
            </p>
            <h3 style={{ fontSize: 15, marginTop: 16 }}>Alerts</h3>
            <AlertsList alerts={alerts} />
            {metrics ? (
              <>
                <h3 style={{ fontSize: 15, marginTop: 20 }}>Key metrics</h3>
                <MetricsSummary m={metrics} />
              </>
            ) : null}
          </>
        )}
      </section>

      <section style={{ marginTop: 32 }}>
        <h2 style={{ fontSize: 18, marginBottom: 12 }}>Recent pipeline runs</h2>
        {d.runs.length === 0 ? (
          <div
            style={{
              border: "1px dashed #d1d5db",
              borderRadius: 8,
              padding: 20,
              color: "#6b7280",
            }}
          >
            <strong>No runs in database yet.</strong> Apply the observability migration and run a
            sync so rows are inserted into <code>pipeline_runs</code>.
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table
              style={{
                borderCollapse: "collapse",
                fontSize: 13,
                width: "100%",
              }}
            >
              <thead>
                <tr style={{ background: "#f3f4f6", textAlign: "left" }}>
                  <th style={th}>Time (UTC)</th>
                  <th style={th}>Type</th>
                  <th style={th}>Done</th>
                  <th style={th}>Duration s</th>
                  <th style={th}>Ins</th>
                  <th style={th}>Upd</th>
                  <th style={th}>Fail</th>
                  <th style={th}>Health</th>
                  <th style={th}>TMDB %</th>
                  <th style={th}>Film %</th>
                  <th style={th}>Catalog rows</th>
                </tr>
              </thead>
              <tbody>
                {d.runs.map((r, i) => (
                  <tr key={r.id ?? i} style={{ borderBottom: "1px solid #e5e7eb" }}>
                    <td style={td}>{runTimeUtc(r)}</td>
                    <td style={td}>{r.pipeline_type ?? "—"}</td>
                    <td style={td}>{r.completed ? "yes" : "no"}</td>
                    <td style={td}>{fmtNum(r.duration_seconds)}</td>
                    <td style={td}>{fmtNum(r.inserts)}</td>
                    <td style={td}>{fmtNum(r.updates)}</td>
                    <td style={td}>{fmtNum(r.failures)}</td>
                    <td style={td}>{fmtNum(r.health_exit_code)}</td>
                    <td style={td}>{fmtPct(r.tmdb_matched_pct)}</td>
                    <td style={td}>{fmtPct(r.film_linked_pct)}</td>
                    <td style={td}>{fmtNum(r.catalog_rows_active)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <p style={{ marginTop: 24, fontSize: 12, color: "#9ca3af" }}>
        Read-only. Does not run imports, syncs, or writes. Operational source: newest rows in
        Supabase.
      </p>
    </div>
  );
}
