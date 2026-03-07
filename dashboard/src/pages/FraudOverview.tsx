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

  // Fetch score bounds from full dataset
  useEffect(() => {
    getFraudScoreBounds().then((bounds) => {
      setScoreBounds(bounds);
    });
  }, []);

  useEffect(() => {
    getFlaggedFraud(filters, page).then((r) => {
      let filtered = r.items;
      if (filters.score_min !== undefined) {
        filtered = filtered.filter((i) => i.fraud_score >= filters.score_min!);
      }
      if (filters.score_max !== undefined) {
        filtered = filtered.filter((i) => i.fraud_score <= filters.score_max!);
      }
      setItems(filtered);
      setTotalPages(r.total_pages);
    });
  }, [filters, page]);

  const applyFilters = (f: FilterValues) => {
    setFilters(f);
    setPage(1);
  };

  return (
    <div>
      <h2>Suspected Fraud Claims</h2>
      <Filters onApply={applyFilters} showScoreFilter scoreLabel="Fraud Score" scoreMinBound={scoreBounds?.min} scoreMaxBound={scoreBounds?.max} />
      <ExportButton href={fraudExportUrl(filters)} />

      <table>
        <thead>
          <tr>
            <th>Claim ID</th>
            <th>Manufacturer</th>
            <th>SKU</th>
            <th>Claim Type</th>
            <th>Amount</th>
            <th>Fraud Score</th>
            <th>Factors</th>
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
              <td>{r.contributing_factors.join(", ")}</td>
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

      <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
    </div>
  );
}
