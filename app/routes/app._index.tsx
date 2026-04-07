import { useEffect, useState } from "react";

type IntentMode =
  | "all"
  | "new_releases"
  | "film_title"
  | "director"
  | "label_studio"
  | "in_stock"
  | "preorders"
  | "best_edition";

const INTENT_MODE_CHIPS: Array<{ id: IntentMode; label: string; placeholder: string }> = [
  { id: "all", label: "All", placeholder: "Search films, directors, labels, editions..." },
  { id: "new_releases", label: "New Releases", placeholder: "Try: new releases this month or latest arrivals" },
  { id: "film_title", label: "Film Title", placeholder: "Enter film title (optionally add year)" },
  { id: "director", label: "Director", placeholder: "Search by director name" },
  { id: "label_studio", label: "Label / Studio", placeholder: "Search by label/studio (e.g. Criterion)" },
  { id: "in_stock", label: "In Stock", placeholder: "Find in-stock titles (optionally add title/director)" },
  { id: "preorders", label: "Upcoming Releases", placeholder: "Find upcoming/future release titles" },
  { id: "best_edition", label: "Best Edition", placeholder: "Best edition for [film title]" },
];

const BLANK_QUERY_ALLOWED_MODES = new Set<IntentMode>([
  "new_releases",
  "in_stock",
  "preorders",
]);

function placeholderForMode(mode: IntentMode): string {
  return INTENT_MODE_CHIPS.find((m) => m.id === mode)?.placeholder || INTENT_MODE_CHIPS[0].placeholder;
}

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
  onAddWishlist,
  creatingDraftId,
  wishlistBusy,
  emailReady,
  showReason = false,
}: {
  opt: any;
  onAdd: (opt: any) => void;
  onAddWishlist?: (opt: any) => void;
  creatingDraftId: string | null;
  wishlistBusy?: boolean;
  emailReady?: boolean;
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

      <div style={{ marginTop: 10, display: "flex", flexWrap: "wrap", gap: 8 }}>
        <button
          onClick={() => onAdd(opt)}
          disabled={creatingDraftId === opt.id}
        >
          {creatingDraftId === opt.id ? "Creating..." : "Add to Draft Order"}
        </button>
        {onAddWishlist && (
          <button
            type="button"
            onClick={() => onAddWishlist(opt)}
            disabled={!emailReady || wishlistBusy}
            title={
              emailReady
                ? "Save to customer wishlist"
                : "Enter customer email first"
            }
          >
            {wishlistBusy ? "Adding…" : "Add to Wishlist"}
          </button>
        )}
      </div>
    </div>
  );
}

export default function Index() {
  const [message, setMessage] = useState("");
  const [intentMode, setIntentMode] = useState<IntentMode>("all");
  const [response, setResponse] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [email, setEmail] = useState("");
  const [draftResult, setDraftResult] = useState<any>(null);
  const [creatingDraftId, setCreatingDraftId] = useState<string | null>(null);
  const [wishlistItems, setWishlistItems] = useState<any[]>([]);
  const [wishlistLoading, setWishlistLoading] = useState(false);
  const [wishlistBusyId, setWishlistBusyId] = useState<string | null>(null);
  const [removingWishlistId, setRemovingWishlistId] = useState<string | null>(
    null,
  );

  const emailReady = Boolean(email.trim());

  useEffect(() => {
    async function loadWishlist() {
      if (!email.trim()) {
        setWishlistItems([]);
        return;
      }
      setWishlistLoading(true);
      try {
        const res = await fetch(
          `/api/wishlist?email=${encodeURIComponent(email.trim())}`,
        );
        const data = await res.json();
        if (data.ok && Array.isArray(data.items)) {
          setWishlistItems(data.items);
        } else {
          setWishlistItems([]);
        }
      } catch {
        setWishlistItems([]);
      } finally {
        setWishlistLoading(false);
      }
    }
    void loadWishlist();
  }, [email]);

  async function addToWishlist(opt: any) {
    if (!email.trim()) {
      alert("Please enter a customer email first.");
      return;
    }
    const catalogItemId = opt.catalogItemId ?? opt.id;
    if (!catalogItemId) {
      alert("This result has no catalog id — cannot add to wishlist.");
      return;
    }
    const titleSnapshot =
      [opt.filmTitle, opt.title].filter(Boolean).join(" — ").trim() ||
      String(opt.title || opt.filmTitle || "Wishlist item").trim();
    setWishlistBusyId(String(opt.id ?? catalogItemId));
    try {
      const res = await fetch("/api/wishlist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: email.trim(),
          catalogItemId: String(catalogItemId),
          filmId: opt.filmId ?? null,
          shopifyVariantId: opt.shopifyVariantId ?? null,
          title: titleSnapshot,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        alert(data.error || "Could not add to wishlist");
        return;
      }
      const listRes = await fetch(
        `/api/wishlist?email=${encodeURIComponent(email.trim())}`,
      );
      const listData = await listRes.json();
      if (listData.ok && Array.isArray(listData.items)) {
        setWishlistItems(listData.items);
      }
    } finally {
      setWishlistBusyId(null);
    }
  }

  async function removeWishlistItem(wishlistItemId: string) {
    if (!email.trim()) return;
    setRemovingWishlistId(wishlistItemId);
    try {
      const res = await fetch("/api/wishlist", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: email.trim(),
          wishlistItemId,
        }),
      });
      const data = await res.json();
      if (!res.ok) {
        alert(data.error || "Could not remove");
        return;
      }
      setWishlistItems((prev) => prev.filter((w) => w.id !== wishlistItemId));
    } finally {
      setRemovingWishlistId(null);
    }
  }

  async function sendMessage() {
    const trimmed = message.trim();
    const canSubmitBlank = BLANK_QUERY_ALLOWED_MODES.has(intentMode);
    if (!trimmed && !canSubmitBlank) {
      return;
    }

    setLoading(true);
    setDraftResult(null);

    const res = await fetch("/api/agent-query", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ message, intentMode }),
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

      {emailReady && (
        <div
          style={{
            marginBottom: 20,
            padding: 12,
            border: "1px solid #e5e7eb",
            borderRadius: 8,
            background: "#fafafa",
          }}
        >
          <strong>Wishlist (this email)</strong>
          {wishlistLoading && (
            <div style={{ marginTop: 8, color: "#6b7280" }}>Loading…</div>
          )}
          {!wishlistLoading && wishlistItems.length === 0 && (
            <div style={{ marginTop: 8, color: "#6b7280" }}>
              No saved items yet.
            </div>
          )}
          {!wishlistLoading &&
            wishlistItems.map((w: any) => {
              const c = w.commercial;
              const release =
                c?.mediaReleaseDate || c?.filmReleased || null;
              return (
                <div
                  key={w.id}
                  style={{
                    marginTop: 12,
                    paddingBottom: 12,
                    borderBottom: "1px solid #e5e7eb",
                    display: "flex",
                    alignItems: "flex-start",
                    gap: 12,
                    flexWrap: "wrap",
                  }}
                >
                  <div style={{ flex: 1, minWidth: 220 }}>
                    <div style={{ fontWeight: 600 }}>
                      {c?.displayTitle || w.title_snapshot}
                    </div>
                    {c?.priceLabel != null && (
                      <div style={{ marginTop: 4, color: "#111827" }}>
                        {c.priceLabel}
                        {c.currency && c.currency !== "GBP" ? ` (${c.currency})` : null}
                      </div>
                    )}
                    {c?.catalogFound && c?.priceLabel == null && (
                      <div style={{ marginTop: 4, color: "#6b7280", fontSize: 13 }}>
                        Price not set
                      </div>
                    )}
                    <div style={{ marginTop: 4, color: "#374151", fontSize: 14 }}>
                      {c?.availabilityLabel || "—"}
                    </div>
                    <div style={{ marginTop: 2, color: "#6b7280", fontSize: 13 }}>
                      {c?.sourceChannel || "—"}
                      {c?.format ? ` · ${c.format}` : ""}
                      {c?.availabilityStatus
                        ? ` · ${c.availabilityStatus}`
                        : ""}
                    </div>
                    {release ? (
                      <div style={{ marginTop: 2, color: "#6b7280", fontSize: 13 }}>
                        {c?.mediaReleaseDate
                          ? `Media release: ${c.mediaReleaseDate}`
                          : `Film release: ${c.filmReleased}`}
                      </div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    disabled={removingWishlistId === w.id}
                    onClick={() => removeWishlistItem(w.id)}
                  >
                    {removingWishlistId === w.id ? "Removing…" : "Remove"}
                  </button>
                </div>
              );
            })}
        </div>
      )}

      <div style={{ marginBottom: 20 }}>
        <div style={{ marginBottom: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          {INTENT_MODE_CHIPS.map((mode) => {
            const active = intentMode === mode.id;
            return (
              <button
                key={mode.id}
                type="button"
                onClick={() => setIntentMode(mode.id)}
                style={{
                  padding: "7px 10px",
                  borderRadius: 999,
                  border: active ? "1px solid #2563eb" : "1px solid #d1d5db",
                  background: active ? "#eff6ff" : "#fff",
                  color: active ? "#1d4ed8" : "#374151",
                  fontWeight: 600,
                  fontSize: 13,
                  cursor: "pointer",
                }}
              >
                {mode.label}
              </button>
            );
          })}
        </div>
        <input
          style={{ width: 400, padding: 10 }}
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          placeholder={placeholderForMode(intentMode)}
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
          {response.wishlistSuggested && !emailReady && (
            <p style={{ marginTop: 6, color: "#9ca3af", fontSize: 13 }}>
              Enter customer email above to save items to wishlist.
            </p>
          )}

          {response.recommendedOption && (
            <div style={{ marginBottom: 28 }}>
              <h2 style={{ marginBottom: 12 }}>Recommended</h2>
              <ResultCard
                opt={response.recommendedOption}
                onAdd={addToDraftOrder}
                onAddWishlist={addToWishlist}
                creatingDraftId={creatingDraftId}
                wishlistBusy={
                  wishlistBusyId === String(response.recommendedOption?.id)
                }
                emailReady={emailReady}
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
                  onAddWishlist={addToWishlist}
                  creatingDraftId={creatingDraftId}
                  wishlistBusy={wishlistBusyId === String(opt.id)}
                  emailReady={emailReady}
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
                  onAddWishlist={addToWishlist}
                  creatingDraftId={creatingDraftId}
                  wishlistBusy={wishlistBusyId === String(opt.id)}
                  emailReady={emailReady}
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