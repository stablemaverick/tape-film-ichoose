import { useState } from "react";

export default function AppIndex() {
  const [query, setQuery] = useState("videodrome");
  const [result, setResult] = useState<any>(null);
  const [loading, setLoading] = useState(false);

async function runSearch() {
  setLoading(true);
  setResult(null);

  try {
    const res = await fetch("/api/create-draft-order", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        email: "simon@tapestore.com.au",
        variantId: "gid://shopify/ProductVariant/43005166878783",
        quantity: 1,
      }),
    });

    const data = await res.json();
    setResult(data);
  } catch (error) {
    setResult({ error: String(error) });
  } finally {
    setLoading(false);
  }
}
  return (
    <div style={{ padding: "24px", fontFamily: "sans-serif" }}>
      <h1>iChoose</h1>
      <p>Test Shopify product search</p>

      <div style={{ display: "flex", gap: "8px", marginBottom: "16px" }}>
        <input
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search products"
          style={{ padding: "8px", minWidth: "280px" }}
        />
        <button onClick={runSearch} disabled={loading}>
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      <pre
        style={{
          background: "#f6f6f7",
          padding: "16px",
          borderRadius: "8px",
          overflowX: "auto",
          whiteSpace: "pre-wrap",
        }}
      >
        {JSON.stringify(result, null, 2)}
      </pre>
    </div>
  );
}