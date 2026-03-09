import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getFlaggedQuality, getQualityScoreBounds, qualityExportUrl } from "../api/client";
import ExportButton from "../components/ExportButton";
import Filters from "../components/Filters";
import Pagination from "../components/Pagination";
import type { Filters as FilterValues, QualityResult } from "../types";

export default function QualityOverview() {
  const [items, setItems] = useState<QualityResult[]>([]);
  const [page, setPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [filters, setFilters] = useState<FilterValues>({});
  const [scoreBounds, setScoreBounds] = useState<{ min: number; max: number } | null>(null);
  const [loading, setLoading] = useState(false);

  // Fetch score bounds from full dataset
  useEffect(() => {
    getQualityScoreBounds().then((bounds) => {
      setScoreBounds(bounds);
    });
  }, []);

  /** Translate reason filter into score ranges for the backend. */
  const translateFilters = (f: FilterValues): FilterValues => {
    const out = { ...f };
    if (out.reason) {
      if (out.reason === "Within normal range") {
        out.score_max = out.score_max != null ? Math.min(out.score_max, 0.99) : 0.99;
      } else if (out.reason === "Slightly elevated repair rate") {
        out.score_min = Math.max(out.score_min ?? 1.0, 1.0);
        out.score_max = out.score_max != null ? Math.min(out.score_max, 1.99) : 1.99;
      } else if (out.reason === "Quality concern") {
        out.score_min = Math.max(out.score_min ?? 2.0, 2.0);
      }
      delete out.reason;
    }
    return out;
  };

  useEffect(() => {
    const apiFilters = translateFilters(filters);
    setLoading(true);
    getFlaggedQuality(apiFilters, page).then((r) => {
      setItems(r.items);
      setTotalPages(r.total_pages);
    }).finally(() => setLoading(false));
  }, [filters, page]);

  const applyFilters = (f: FilterValues) => {
    setFilters(f);
    setPage(1);
  };

  /** Score-based quality reason, similar to fraud's formatReason. */
  const formatQualityReason = (r: QualityResult, topSkus: { sku: string; repair_rate: number }[]): string => {
    if (r.quality_score < 1.0) return "Within normal range";
    if (r.quality_score < 2.0) return "Slightly elevated repair rate";
    let detail = `${(r.repair_claim_rate * 100).toFixed(0)}% repair rate`;
    if (topSkus.length > 0) {
      detail += ` — top: ${topSkus[0].sku} (${(topSkus[0].repair_rate * 100).toFixed(0)}% repairs)`;
    }
    return "Quality concern — " + detail;
  };

  return (
    <div>
      <h2>Manufacturer Quality Concerns</h2>
      <Filters onApply={applyFilters} showScoreFilter scoreLabel="Quality Score" scoreMinBound={scoreBounds?.min} scoreMaxBound={scoreBounds?.max} reasonOptions={["Within normal range", "Slightly elevated repair rate", "Quality concern"]} />
      <ExportButton href={qualityExportUrl(translateFilters(filters))} />

      <div className="table-container">
        {loading && <div className="loading-overlay"><div className="spinner" />Loading...</div>}
        <table>
        <thead>
          <tr>
            <th>Manufacturer</th>
            <th>Top SKUs (by repairs)</th>
            <th>Repair Claims</th>
            <th>Repair Rate</th>
            <th>Quality Score</th>
            <th>Reason</th>
            <th>Scored At</th>
          </tr>
        </thead>
        <tbody>
          {items.map((r) => {
            const skus = r.sku_breakdown ? Object.values(r.sku_breakdown) : [];
            const topSkus = skus
              .sort((a, b) => b.repair_count - a.repair_count)
              .slice(0, 3);
            return (
              <tr key={r.manufacturer_id}>
                <td>
                  <Link to={`/quality/${r.manufacturer_id}`}>{r.manufacturer_name}</Link>
                </td>
                <td>{topSkus.length > 0 ? topSkus.map((s) => s.sku).join(", ") : "—"}</td>
                <td>{Math.round(r.total_claims * r.repair_claim_rate)}</td>
                <td>{(r.repair_claim_rate * 100).toFixed(1)}%</td>
                <td>{r.quality_score.toFixed(2)}</td>
                <td>{formatQualityReason(r, topSkus)}</td>
                <td>{new Date(r.scored_at).toLocaleDateString()}</td>
              </tr>
            );
          })}
          {items.length === 0 && (
            <tr>
              <td colSpan={7}>No flagged manufacturers.</td>
            </tr>
          )}
        </tbody>
      </table>
      </div>

      <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
    </div>
  );
}
