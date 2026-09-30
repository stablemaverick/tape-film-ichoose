import type { ActionFunctionArgs, LoaderFunctionArgs } from "react-router";
import { spawnSync } from "node:child_process";
import path from "node:path";

/**
 * Internal Stock Availability V1 endpoint.
 * Auth: PIPELINE_HEALTH_KEY via ?key= or Authorization: Bearer <key>
 * Costs are included only when include_costs=1 (default for internal use).
 * Never expose this route publicly without auth.
 */

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

function runCli(args: string[]): Response {
  const root = process.cwd();
  const script = path.join(root, "scripts/inventory/get_stock_availability.py");
  const python = path.join(root, "venv/bin/python");
  const envFile = process.env.STOCK_AVAILABILITY_ENV_FILE || ".env";
  const result = spawnSync(python, [script, "--env-file", envFile, ...args], {
    cwd: root,
    encoding: "utf-8",
    maxBuffer: 4 * 1024 * 1024,
  });
  if (result.error) {
    return Response.json(
      { error: "CLI_FAILED", message: String(result.error) },
      { status: 500 },
    );
  }
  const stdout = (result.stdout || "").trim();
  let body: unknown = {};
  try {
    body = stdout ? JSON.parse(stdout) : {};
  } catch {
    return Response.json(
      { error: "BAD_CLI_JSON", message: stdout.slice(0, 500) },
      { status: 500 },
    );
  }
  const status =
    result.status === 0
      ? 200
      : typeof body === "object" &&
          body &&
          "error" in body &&
          (body as { error?: string }).error === "RELEASE_NOT_FOUND"
        ? 404
        : result.status === 1
          ? 400
          : 500;
  return Response.json(body, { status });
}

export async function loader({ request }: LoaderFunctionArgs) {
  authorize(request);
  const url = new URL(request.url);
  const op = url.searchParams.get("op") || "get";

  if (op === "search") {
    const q = url.searchParams.get("q") || "";
    return runCli(["search", q, "--limit", url.searchParams.get("limit") || "20"]);
  }
  if (op === "history") {
    const id = url.searchParams.get("release_variant_id") || "";
    return runCli([
      "history",
      "--release-variant-id",
      id,
      "--limit",
      url.searchParams.get("limit") || "50",
    ]);
  }

  const args = ["get"];
  const rv = url.searchParams.get("release_variant_id");
  const barcode = url.searchParams.get("barcode");
  const shopify = url.searchParams.get("shopify_variant_id");
  const supplierId = url.searchParams.get("supplier_id");
  const supplierSku = url.searchParams.get("supplier_sku");
  if (rv) args.push("--release-variant-id", rv);
  if (barcode) args.push("--barcode", barcode);
  if (shopify) args.push("--shopify-variant-id", shopify);
  if (supplierId) args.push("--supplier-id", supplierId);
  if (supplierSku) args.push("--supplier-sku", supplierSku);
  if (url.searchParams.get("include_costs") === "0") args.push("--hide-costs");
  return runCli(args);
}

export async function action({ request }: ActionFunctionArgs) {
  authorize(request);
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const op = String(body.op || "get");
  if (op === "search") {
    return runCli(["search", String(body.query || ""), "--limit", String(body.limit || 20)]);
  }
  if (op === "history") {
    return runCli([
      "history",
      "--release-variant-id",
      String(body.release_variant_id || ""),
      "--limit",
      String(body.limit || 50),
    ]);
  }
  const args = ["get"];
  if (body.release_variant_id) args.push("--release-variant-id", String(body.release_variant_id));
  if (body.barcode) args.push("--barcode", String(body.barcode));
  if (body.shopify_variant_id) args.push("--shopify-variant-id", String(body.shopify_variant_id));
  if (body.supplier_id) args.push("--supplier-id", String(body.supplier_id));
  if (body.supplier_sku) args.push("--supplier-sku", String(body.supplier_sku));
  if (body.include_costs === false || body.include_costs === 0) args.push("--hide-costs");
  return runCli(args);
}
