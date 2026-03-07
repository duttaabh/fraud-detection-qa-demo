import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getQualityDetail, getQualityReasoning } from "../api/client";
import AuditTrail from "../components/AuditTrail";
import type { QualityResult } from "../types";

export default function QualityDetail() {
  const { manufacturerId } = useParams<{ manufacturerId: string }>();
  const [result, setResult] = useState<QualityResult | null>(null);
  const [reasoning, setReasoning] = useState<string | null>(null);
  const [reasoningLoading, setReasoningLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!manufacturerId) return;
    getQualityDetail(manufacturerId)
      .then((data) => {
        setResult(data);
        setReasoningLoading(true);
        getQualityReasoning(manufacturerId)
          .then((r) => setReasoning(r.reasoning))
          .catch(() => setReasoning("Unable to generate AI reasoning."))
          .finally(() => setReasoningLoading(false));
      })
      .catch(() => setError("Manufacturer not found."));
  }, [manufacturerId]);

  if (error) return <p className="error">{error}</p>;
  if (!result) return <p>Loading…</p>;

  const categories = Object.values(result.product_category_breakdown);
  const skus = result.sku_breakdown ? Object.values(result.sku_breakdown) : [];
  const skusSorted = [...skus].sort((a, b) => b.repair_count - a.repair_count);
  const repairCount = Math.round(result.total_claims * result.repair_claim_rate);

  return (
    <div>
      <Link to="/quality">← Back to Quality Overview</Link>
      <h2>{result.manufacturer_name}</h2>

      <dl>
        <dt>Manufacturer ID</dt><dd>{result.manufacturer_id}</dd>
        <dt>Total Claims</dt><dd>{result.total_claims}</dd>
        <dt>Repair Claims</dt><dd>{repairCount}</dd>
        <dt>Repair Claim Rate</dt><dd>{(result.repair_claim_rate * 100).toFixed(1)}%</dd>
        <dt>Quality Score</dt><dd>{result.quality_score.toFixed(2)}</dd>
        <dt>Quality Concern</dt><dd>{result.is_quality_concern ? "Yes" : "No"}</dd>
        <dt>Model Version</dt><dd>{result.model_version}</dd>
        <dt>Scored At</dt><dd>{new Date(result.scored_at).toLocaleString()}</dd>
      </dl>

      <h3>Product Category Breakdown</h3>
      <table>
        <thead>
          <tr>
            <th>Category</th>
            <th>Claim Count</th>
            <th>Repair Count</th>
            <th>Repair Rate</th>
          </tr>
        </thead>
        <tbody>
          {categories.map((c) => (
            <tr key={c.category}>
              <td>{c.category}</td>
              <td>{c.claim_count}</td>
              <td>{Math.round(c.claim_count * c.repair_rate)}</td>
              <td>{(c.repair_rate * 100).toFixed(1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>

      <h3>AI Reasoning (Amazon Nova Pro)</h3>
      <div className="ai-reasoning">
        {reasoningLoading && <p>Generating AI analysis…</p>}
        {reasoning && (
          <pre style={{ whiteSpace: "pre-wrap", fontFamily: "inherit", lineHeight: 1.6 }}>
            {reasoning}
          </pre>
        )}
      </div>

      <h3>SKU Breakdown</h3>
      <table>
        <thead>
          <tr>
            <th>SKU</th>
            <th>Total Claims</th>
            <th>Repair Count</th>
            <th>Repair Rate</th>
          </tr>
        </thead>
        <tbody>
          {skusSorted.map((s) => (
            <tr key={s.sku}>
              <td>{s.sku}</td>
              <td>{s.claim_count}</td>
              <td>{s.repair_count}</td>
              <td>{(s.repair_rate * 100).toFixed(1)}%</td>
            </tr>
          ))}
          {skusSorted.length === 0 && (
            <tr>
              <td colSpan={4}>No SKU data available.</td>
            </tr>
          )}
        </tbody>
      </table>

      <AuditTrail entityType="manufacturer" entityId={result.manufacturer_id} />
    </div>
  );
}
