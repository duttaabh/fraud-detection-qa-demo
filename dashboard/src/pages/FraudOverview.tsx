import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getFlaggedFraud, getFraudScoreBounds, fraudExportUrl } from "../api/client";
import ExportButton from "../components/ExportButton";
import Filters from "../components/Filters";
import Pagination from "../components/Pagination";
import type { Filters as FilterValues, FraudResult } from "../types";

export default function FraudOverview() {
  const [items, setItems] = useState<FraudResult[]>([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [filters, setFilters] = useState<FilterValues>({});
  const [scoreBounds, setScoreBounds] = useState<{ min: number; max: number } | null>(null);
  const [loading, setLoading] = useState(false);

  /** Turn raw feature names into readable fraud reasons. */
  const formatReason = (r: FraudResult): string => {
    if (r.fraud_score < 0.4) return "Within normal range";
    if (r.fraud_score < 0.7) return "Slightly elevated — minor deviations";

    const labels: Record<string, string> = {
      claim_amount: "High claim amount",
      days_between_contract_start_and_claim: "Claim filed unusually fast",
      manufacturer_claim_frequency: "Manufacturer has many claims",
      claim_type_repair: "Repair-type claim",
      claim_type_replacement: "Replacement-type claim",
      claim_type_refund: "Refund-type claim",
      claim_type_unknown: "Unknown claim type",
      product_category_electronics: "Electronics product",
      product_category_appliances: "Appliances product",
      product_category_automotive: "Automotive product",
      product_category_furniture: "Furniture product",
      product_category_unknown: "Unknown product category",
    };
    const reasons = r.contributing_factors.map((f) => labels[f] ?? f);
    return "Suspected fraud — " + reasons.join("; ");
  };

  /** Translate reason filter into score ranges for the backend. */
  const translateFilters = (f: FilterValues): FilterValues => {
    const out = { ...f };
    if (out.reason) {
      if (out.reason === "Within normal range") {
        out.score_max = out.score_max ?? 0.39;
        if (out.score_max > 0.39) out.score_max = 0.39;
      } else if (out.reason === "Slightly elevated — minor deviations") {
        out.score_min = Math.max(out.score_min ?? 0.4, 0.4);
        out.score_max = out.score_max != null ? Math.min(out.score_max, 0.69) : 0.69;
      } else if (out.reason === "Suspected fraud") {
        out.score_min = Math.max(out.score_min ?? 0.7, 0.7);
      }
      delete out.reason;
    }
    return out;
  };

  // Fetch score bounds from full dataset
  useEffect(() => {
    getFraudScoreBounds().then((bounds) => {
      setScoreBounds(bounds);
    });
  }, []);

  useEffect(() => {
    const apiFilters = translateFilters(filters);
    setLoading(true);
    getFlaggedFraud(apiFilters, page).then((r) => {
      setItems(r.items);
      setTotalPages(r.total_pages);
    }).finally(() => setLoading(false));
  }, [filters, page]);

  const applyFilters = (f: FilterValues) => {
    setFilters(f);
    setPage(1);
  };

  return (
    <div>
      <h2>Suspected Fraud Claims</h2>
      <Filters onApply={applyFilters} showScoreFilter scoreLabel="Fraud Score" scoreMinBound={scoreBounds?.min} scoreMaxBound={scoreBounds?.max} reasonOptions={["Within normal range", "Slightly elevated — minor deviations", "Suspected fraud"]} />
      <ExportButton href={fraudExportUrl(translateFilters(filters))} />

      <div className="table-container">
        {loading && <div className="loading-overlay"><div className="spinner" />Loading...</div>}
        <table>
        <thead>
          <tr>
            <th>Claim ID</th>
            <th>Manufacturer</th>
            <th>SKU</th>
            <th>Claim Type</th>
            <th>Amount</th>
            <th>Fraud Score</th>
            <th>Reason</th>
            <th>Scored At</th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => (
            <tr key={r.claim_id}>
              <td>
                <Link to={`/fraud/${r.claim_id}`}>{r.claim_id}</Link>
              </td>
              <td>{r.manufacturer_name ?? "—"}</td>
              <td>{r.sku ?? "—"}</td>
              <td>{r.claim_type ?? "—"}</td>
              <td>{r.claim_amount != null ? `$${r.claim_amount.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "—"}</td>
              <td>{r.fraud_score.toFixed(2)}</td>
              <td>{formatReason(r)}</td>
              <td>{new Date(r.scored_at).toLocaleDateString()}</td>
            </tr>
          ))}
          {items.length === 0 && (
            <tr>
              <td colSpan={8}>No flagged claims.</td>
            </tr>
          )}
        </tbody>
      </table>
      </div>

      <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
    </div>
  );
}
