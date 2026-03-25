import { useState } from "react";

function getAvailabilityMeta(status: string | null | undefined, supplierStock?: number) {
  if (status === "store_stock") {
    return {
      label: "In Stock",
      bg: "#dcfce7",
      color: "#166534",
      border: "#bbf7d0",
    };
  }

  if (status === "supplier_stock") {
    return {
      label:
        supplierStock && supplierStock > 0
          ? `Available to Order (${supplierStock})`
          : "Available to Order",
      bg: "#fef3c7",
      color: "#92400e",
      border: "#fde68a",
    };
  }

  if (status === "preorder") {
    return {
      label: "Pre-Order",
      bg: "#dbeafe",
      color: "#1d4ed8",
      border: "#bfdbfe",
    };
  }

  if (status === "supplier_out" || status === "store_out") {
    return {
      label: "Out of Stock",
      bg: "#fee2e2",
      color: "#991b1b",
      border: "#fecaca",
    };
  }

  return {
    label: "Unknown",
    bg: "#f3f4f6",
    color: "#374151",
    border: "#e5e7eb",
  };
}

function AvailabilityBadge({
  status,
  supplierStock,
}: {
  status: string | null | undefined;
  supplierStock?: number;
}) {
  const meta = getAvailabilityMeta(status, supplierStock);

  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 8,
        padding: "6px 10px",
        borderRadius: 999,
        backgroundColor: meta.bg,
        color: meta.color,
        border: `1px solid ${meta.border}`,
        fontSize: 13,
        fontWeight: 600,
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          backgroundColor: meta.color,
          display: "inline-block",
        }}
      />
      {meta.label}
    </span>
  );
}

function ResultCard({
  opt,
  onAdd,
  creatingDraftId,
  showReason = false,
}: {
  opt: any;
  onAdd: (opt: any) => void;
  creatingDraftId: string | null;
  showReason?: boolean;
}) {
  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        padding: 16,
        marginBottom: 16,
        borderRadius: 12,
        background: "#fff",
        boxShadow: "0 1px 2px rgba(0,0,0,0.04)",
      }}
    >
      <div>
        <strong>{opt.filmTitle}</strong>
      </div>

      <div>{opt.title}</div>

      <div>Director: {opt.director || "—"}</div>

      <div>
        {opt.format} — {opt.studio}
      </div>

      <div>Barcode: {opt.barcode || "—"}</div>

      <div>Release date: {opt.mediaReleaseDate || "—"}</div>

      <div style={{ marginTop: 8 }}>
        <AvailabilityBadge
          status={opt.availability}
          supplierStock={opt.supplierStock}
        />
      </div>

      <div style={{ marginTop: 8 }}>
        <strong>${opt.price}</strong>
      </div>

      {showReason && opt.recommendationReason && (
        <div
          style={{
            marginTop: 10,
            padding: 10,
            background: "#f9fafb",
            borderRadius: 8,
            fontSize: 14,
          }}
        >
          {opt.recommendationReason}
        </div>
      )}

      <div style={{ marginTop: 10 }}>
        <button
          onClick={() => onAdd(opt)}
          disabled={creatingDraftId === opt.id}
        >
          {creatingDraftId === opt.id ? "Creating..." : "Add to Draft Order"}
        </button>
      </div>
    </div>
  );
}

export default function Index() {
  const [message, setMessage] = useState("");
  const [response, setResponse] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [email, setEmail] = useState("");
  const [draftResult, setDraftResult] = useState<any>(null);
  const [creatingDraftId, setCreatingDraftId] = useState<string | null>(null);

  async function sendMessage() {
    if (!message.trim()) return;

    setLoading(true);
    setDraftResult(null);

    const res = await fetch("/api/agent-query", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ message }),
    });

    const data = await res.json();
    setResponse(data);
    setLoading(false);
  }

  async function addToDraftOrder(opt: any) {
    if (!email.trim()) {
      alert("Please enter a customer email first.");
      return;
    }

    setCreatingDraftId(opt.id);
    setDraftResult(null);

    const isShopify = opt.sourceType === "shopify" && !!opt.shopifyVariantId;

    const lineItem = isShopify
      ? {
          source: "shopify",
          variantId: opt.shopifyVariantId,
          quantity: 1,
        }
      : {
          source: "catalog",
          title: opt.filmTitle,
          quantity: 1,
          costGbp: Number(opt.costGbp || 0),
          catalogItemId: opt.catalogItemId,
          filmId: opt.filmId,
          productCode: opt.productCode,
          barcode: opt.barcode,
          director: opt.director,
          studio: opt.studio,
          format: opt.format,
          filmReleased: opt.filmReleased,
          mediaReleaseDate: opt.mediaReleaseDate,
        };

    const res = await fetch("/api/create-draft-order", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        email,
        lineItems: [lineItem],
      }),
    });

    const data = await res.json();
    setDraftResult(data);
    setCreatingDraftId(null);
  }

  return (
    <div style={{ padding: 40, fontFamily: "system-ui", maxWidth: 900 }}>
      <h1>Tape! AI Assistant</h1>

      <div style={{ marginBottom: 16 }}>
        <label style={{ display: "block", marginBottom: 6 }}>Customer Email</label>
        <input
          style={{ width: 400, padding: 10 }}
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          placeholder="customer@email.com"
        />
      </div>

      <div style={{ marginBottom: 20 }}>
        <input
          style={{ width: 400, padding: 10 }}
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          placeholder="Search for a film..."
        />

        <button
          style={{ marginLeft: 10, padding: "10px 20px" }}
          onClick={sendMessage}
        >
          Search
        </button>
      </div>

      {loading && <div>Searching...</div>}

      {response && (
        <div style={{ marginTop: 30 }}>
          <p>{response.reply}</p>
          
          {response.upsell && (
            <p style={{ marginTop: 8, color: "#374151" }}>{response.upsell}</p>
          )}
          
          {response.wishlistPrompt && (
            <p style={{ marginTop: 8, color: "#6b7280" }}>{response.wishlistPrompt}</p>
          )}

          {response.recommendedOption && (
            <div style={{ marginBottom: 28 }}>
              <h2 style={{ marginBottom: 12 }}>Recommended</h2>
              <ResultCard
                opt={response.recommendedOption}
                onAdd={addToDraftOrder}
                creatingDraftId={creatingDraftId}
                showReason
              />
            </div>
          )}

          {response.alternativeOptions?.length > 0 && (
            <div>
              <h2 style={{ marginBottom: 12 }}>Alternatives</h2>
              {response.alternativeOptions.map((opt: any) => (
                <ResultCard
                  key={opt.id}
                  opt={opt}
                  onAdd={addToDraftOrder}
                  creatingDraftId={creatingDraftId}
                />
              ))}
            </div>
          )}

          {!response.recommendedOption && response.options?.length > 0 && (
            <div>
              <h2 style={{ marginBottom: 12 }}>Results</h2>
              {response.options.map((opt: any) => (
                <ResultCard
                  key={opt.id}
                  opt={opt}
                  onAdd={addToDraftOrder}
                  creatingDraftId={creatingDraftId}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {draftResult?.draftOrder && (
        <div
          style={{
            marginTop: 30,
            padding: 15,
            border: "1px solid #ccc",
            borderRadius: 6,
          }}
        >
          <div>
            <strong>Draft order created</strong>
          </div>
          <div>Name: {draftResult.draftOrder.name}</div>
          <div>Status: {draftResult.draftOrder.status}</div>
          <div style={{ marginTop: 8 }}>
            <a
              href={draftResult.draftOrder.invoiceUrl}
              target="_blank"
              rel="noreferrer"
            >
              Open invoice
            </a>
          </div>
        </div>
      )}
    </div>
  );
}