/**
 * Ordering Agent V1 — customer-safe Film ordering API.
 *
 * POST /api/ordering-agent
 * Auth: PIPELINE_HEALTH_KEY (internal) until App Proxy / storefront launch.
 *
 * Authoritative path (Python):
 *   search_inventory → StockAvailability → CommerceOffer → public serializer
 *
 * Legacy /api/agent-query catalogue stock/price is NOT used here.
 */

import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { randomUUID } from "node:crypto";

function authorize(request: Request): void {
  const url = new URL(request.url);
  const key =
    url.searchParams.get("key") ??
    (request.headers.get("authorization") || "").replace(/^Bearer\s+/i, "").trim();
  const expected = process.env.PIPELINE_HEALTH_KEY ?? "";
  if (!expected) {
    throw new Response("PIPELINE_HEALTH_KEY is not set", { status: 503 });
  }
  if (key !== expected) {
    throw new Response("Unauthorized", { status: 401 });
  }
}

function runAgent(args: string[]): Response {
  const root = process.cwd();
  const script = path.join(root, "scripts/ordering/run_ordering_agent.py");
  const python = path.join(root, "venv/bin/python");
  const envFile = process.env.ORDERING_AGENT_ENV_FILE || process.env.STOCK_AVAILABILITY_ENV_FILE || ".env";
  const result = spawnSync(python, [script, "--env-file", envFile, ...args], {
    cwd: root,
    encoding: "utf-8",
    maxBuffer: 4 * 1024 * 1024,
  });
  if (result.error) {
    return Response.json(
      { error: "AGENT_ERROR", message: String(result.error) },
      { status: 500 },
    );
  }
  const stdout = (result.stdout || "").trim();
  let body: Record<string, unknown> = {};
  try {
    body = stdout ? (JSON.parse(stdout) as Record<string, unknown>) : {};
  } catch {
    return Response.json(
      { error: "BAD_CLI_JSON", message: stdout.slice(0, 500) },
      { status: 500 },
    );
  }

  // Never forward Python stderr stack traces to clients
  const type = String(body.type || "");
  const status =
    type === "error"
      ? body.error === "RELEASE_NOT_FOUND"
        ? 404
        : 400
      : 200;

  // Strip observability from browser-facing default; include when debug=1
  return Response.json(body, { status });
}

export async function loader(_args: LoaderFunctionArgs) {
  return Response.json({
    service: "ordering-agent-v1",
    methods: ["POST"],
    auth: "PIPELINE_HEALTH_KEY",
  });
}

export async function action({ request }: ActionFunctionArgs) {
  authorize(request);
  if (request.method !== "POST") {
    return Response.json({ error: "METHOD_NOT_ALLOWED" }, { status: 405 });
  }

  let payload: {
    message?: string;
    conversation_id?: string;
    intent?: Record<string, unknown>;
  } = {};
  try {
    payload = (await request.json()) as typeof payload;
  } catch {
    return Response.json({ error: "INVALID_JSON" }, { status: 400 });
  }

  const message = String(payload.message || "").trim();
  if (!message) {
    return Response.json(
      { error: "INVALID_IDENTIFIER", message: "message is required" },
      { status: 400 },
    );
  }

  const conversationId = String(payload.conversation_id || randomUUID());
  const args = ["--message", message, "--conversation-id", conversationId];
  if (payload.intent && typeof payload.intent === "object") {
    args.push("--intent-json", JSON.stringify(payload.intent));
  }
  return runAgent(args);
}
