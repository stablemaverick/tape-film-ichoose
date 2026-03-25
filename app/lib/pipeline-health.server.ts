import * as fs from "node:fs/promises";
import * as path from "node:path";

import { supabase } from "./supabase.server";

export type PipelineHealthSource = "supabase" | "file";

export type PipelineRunRow = {
  id?: string;
  created_at?: string | null;
  recorded_at?: string | null;
  pipeline_type?: string | null;
  log_file?: string | null;
  started_at?: string | null;
  ended_at?: string | null;
  duration_seconds?: number | null;
  completed?: boolean | null;
  inserts?: number | null;
  updates?: number | null;
  failures?: number | null;
  lock_encountered?: boolean | null;
  health_exit_code?: number | null;
  tmdb_matched_pct?: number | null;
  film_linked_pct?: number | null;
  catalog_rows_active?: number | null;
  missing_sale_price_pct?: number | null;
  null_barcode_rows?: number | null;
  duplicate_films?: number | null;
};

export type HealthAlert = {
  level?: string;
  metric?: string;
  message?: string;
  value?: string;
  threshold?: string;
};

export type CatalogHealthSnapshotRow = {
  id?: string;
  created_at?: string;
  pipeline_run_id?: string | null;
  generated_at?: string | null;
  exit_code?: number | null;
  metrics?: Record<string, unknown>;
  alerts?: HealthAlert[];
};

export type PipelineHealthLoaderData = {
  source: PipelineHealthSource;
  /** e.g. file fallback notice */
  banner?: string;
  runs: PipelineRunRow[];
  runsShown: number;
  latestHealth: CatalogHealthSnapshotRow | null;
  readError?: string | null;
  /** File fallback only */
  historyPath?: string | null;
  fileMtime?: string | null;
  schemaVersion?: number | null;
};

const MAX_RUNS = 50;

function pipelineHistoryFilePath(): string {
  const override = process.env.PIPELINE_HISTORY_FILE?.trim();
  if (override) {
    return path.isAbsolute(override)
      ? override
      : path.join(process.cwd(), override);
  }
  return path.join(process.cwd(), "logs", "pipeline_run_history.json");
}

function mapJsonHistoryRun(r: Record<string, unknown>): PipelineRunRow {
  return {
    recorded_at: typeof r.timestamp === "string" ? r.timestamp : null,
    pipeline_type: (r.pipeline_type as string) ?? null,
    log_file: (r.log_file as string) ?? null,
    started_at: (r.started_at as string) ?? null,
    ended_at: (r.ended_at as string) ?? null,
    duration_seconds:
      typeof r.duration_seconds === "number" ? r.duration_seconds : null,
    completed: typeof r.completed === "boolean" ? r.completed : null,
    inserts: typeof r.inserts === "number" ? r.inserts : null,
    updates: typeof r.updates === "number" ? r.updates : null,
    failures: typeof r.failures === "number" ? r.failures : null,
    lock_encountered:
      typeof r.lock_encountered === "boolean" ? r.lock_encountered : null,
    health_exit_code:
      typeof r.health_exit_code === "number" ? r.health_exit_code : null,
    tmdb_matched_pct:
      typeof r.tmdb_matched_pct === "number" ? r.tmdb_matched_pct : null,
    film_linked_pct:
      typeof r.film_linked_pct === "number" ? r.film_linked_pct : null,
    catalog_rows_active:
      typeof r.catalog_rows_active === "number" ? r.catalog_rows_active : null,
    missing_sale_price_pct:
      typeof r.missing_sale_price_pct === "number"
        ? r.missing_sale_price_pct
        : null,
    null_barcode_rows:
      typeof r.null_barcode_rows === "number" ? r.null_barcode_rows : null,
    duplicate_films:
      typeof r.duplicate_films === "number" ? r.duplicate_films : null,
  };
}

async function loadFromFileFallback(): Promise<PipelineHealthLoaderData> {
  const historyPath = pipelineHistoryFilePath();
  let runs: PipelineRunRow[] = [];
  let schemaVersion: number | null = null;
  let fileMtime: string | null = null;
  let readError: string | null = null;

  try {
    const stat = await fs.stat(historyPath);
    fileMtime = stat.mtime.toISOString();
    const raw = await fs.readFile(historyPath, "utf-8");
    const parsed = JSON.parse(raw) as {
      schema_version?: number;
      runs?: Record<string, unknown>[];
    };
    schemaVersion =
      typeof parsed.schema_version === "number" ? parsed.schema_version : null;
    const list = Array.isArray(parsed.runs) ? parsed.runs : [];
    runs = [...list].reverse().slice(0, MAX_RUNS).map(mapJsonHistoryRun);
  } catch (e) {
    readError = e instanceof Error ? e.message : String(e);
  }

  return {
    source: "file",
    banner:
      "Showing local pipeline_run_history.json (PIPELINE_HEALTH_FILE_FALLBACK=1). Prefer Supabase tables on Render.",
    runs,
    runsShown: runs.length,
    latestHealth: null,
    readError,
    historyPath,
    fileMtime,
    schemaVersion,
  };
}

/**
 * Primary: pipeline_runs + latest catalog_health_snapshots.
 * Fallback: local JSON only when PIPELINE_HEALTH_FILE_FALLBACK=1 and DB fails or is empty+error — actually user said empty = empty state, not fallback.
 * Fallback only on DB query error when env set.
 */
export async function loadPipelineHealthData(): Promise<PipelineHealthLoaderData> {
  const fileFallback = process.env.PIPELINE_HEALTH_FILE_FALLBACK === "1";

  try {
    const { data: runRows, error: runsError } = await supabase
      .from("pipeline_runs")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(MAX_RUNS);

    if (runsError) {
      throw new Error(runsError.message);
    }

    const { data: snapList, error: snapError } = await supabase
      .from("catalog_health_snapshots")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(1);

    if (snapError) {
      throw new Error(snapError.message);
    }

    const runs = (runRows ?? []) as PipelineRunRow[];
    const latestHealth =
      snapList && snapList.length > 0
        ? (snapList[0] as CatalogHealthSnapshotRow)
        : null;

    return {
      source: "supabase",
      runs,
      runsShown: runs.length,
      latestHealth,
      readError: null,
    };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (fileFallback) {
      const fb = await loadFromFileFallback();
      return {
        ...fb,
        banner:
          `${fb.banner ?? ""} DB error (fallback): ${msg}`.trim(),
      };
    }
    throw new Response(
      `Pipeline health: Supabase read failed (${msg}). ` +
        `Apply supabase/migrations/20260323120000_pipeline_observability.sql or set PIPELINE_HEALTH_FILE_FALLBACK=1 for local JSON.`,
      { status: 503, headers: { "Content-Type": "text/plain; charset=utf-8" } },
    );
  }
}
