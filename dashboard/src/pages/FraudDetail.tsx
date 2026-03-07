import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getFraudDetail, getFraudReasoning } from "../api/client";
import AuditTrail from "../components/AuditTrail";
import type { FraudResult } from "../types";

export default function FraudDetail() {
  const { claimId } = useParams<{ claimId: string }>();
  const [result, setResult] = useState<FraudResult | null>(null);
  const [reasoning, setReasoning] = useState<string | null>(null);
  const [reasoningLoading, setReasoningLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!claimId) return;
    getFraudDetail(claimId)
      .then((data) => {
        setResult(data);
        setReasoningLoading(true);
        getFraudReasoning(claimId)
          .then((r) => setReasoning(r.reasoning))
          .catch(() => setReasoning("Unable to generate AI reasoning."))
          .finally(() => setReasoningLoading(false));
      })
      .catch(() => setError("Claim not found."));
  }, [claimId]);

  if (error) return <p className="error">{error}</p>;
  if (!result) return <p>Loading…</p>;

  return (
    <div>
      <Link to="/fraud">← Back to Fraud Overview</Link>
      <h2>Claim {result.claim_id}</h2>

      <h3>Claim Details</h3>
      <dl>
        <dt>Claim ID</dt><dd>{result.claim_id}</dd>
        <dt>Claim Date</dt><dd>{result.claim_date ?? "—"}</dd>
        <dt>Claim Type</dt><dd>{result.claim_type ?? "—"}</dd>
        <dt>Claim Amount</dt><dd>{result.claim_amount != null ? `$${result.claim_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "—"}</dd>
        <dt>Status</dt><dd>{result.status ?? "—"}</dd>
        <dt>Description</dt><dd>{result.description ?? "—"}</dd>
        <dt>Product Category</dt><dd>{result.product_category ?? "—"}</dd>
      </dl>

      <h3>Contract & Product</h3>
      <dl>
        <dt>Contract ID</dt><dd>{result.contract_id}</dd>
        <dt>SKU</dt><dd>{result.sku ?? "—"}</dd>
        <dt>Manufacturer</dt><dd>{result.manufacturer_name ?? "—"}</dd>
      </dl>

      <h3>Fraud Analysis</h3>
      <dl>
        <dt>Fraud Score</dt><dd>{result.fraud_score.toFixed(2)}</dd>
        <dt>Suspected Fraud</dt><dd>{result.is_suspected_fraud ? "Yes" : "No"}</dd>
        <dt>Model Version</dt><dd>{result.model_version}</dd>
        <dt>Scored At</dt><dd>{new Date(result.scored_at).toLocaleString()}</dd>
      </dl>

      <h3>Contributing Factors</h3>
      <ul>
        {result.contributing_factors.map((f, i) => (
          <li key={i}>{f}</li>
        ))}
      </ul>

      <h3>AI Reasoning (Amazon Nova Pro)</h3>
      <div className="ai-reasoning">
        {reasoningLoading && <p>Generating AI analysis…</p>}
        {reasoning && (
          <pre style={{ whiteSpace: "pre-wrap", fontFamily: "inherit", lineHeight: 1.6 }}>
            {reasoning}
          </pre>
        )}
      </div>

      <AuditTrail entityType="claim" entityId={result.claim_id} />
    </div>
  );
}
