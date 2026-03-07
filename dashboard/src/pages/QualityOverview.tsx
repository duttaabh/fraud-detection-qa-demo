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

  // Fetch score bounds from full dataset
  useEffect(() => {
    getQualityScoreBounds().then((bounds) => {
      setScoreBounds(bounds);
    });
  }, []);

  useEffect(() => {
    getFlaggedQuality(filters, page).then((r) => {
      let filtered = r.items;
      if (filters.score_min !== undefined) {
        filtered = filtered.filter((i) => i.quality_score >= filters.score_min!);
      }
      if (filters.score_max !== undefined) {
        filtered = filtered.filter((i) => i.quality_score <= filters.score_max!);
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
      <h2>Manufacturer Quality Concerns</h2>
      <Filters onApply={applyFilters} showScoreFilter scoreLabel="Quality Score" scoreMinBound={scoreBounds?.min} scoreMaxBound={scoreBounds?.max} />
      <ExportButton href={qualityExportUrl(filters)} />

      <table>
        <thead>
          <tr>
            <th>Manufacturer</th>
            <th>Top SKUs (by repairs)</th>
            <th>Repair Claims</th>
            <th>Repair Rate</th>
            <th>Quality Score</th>
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
                <td>{new Date(r.scored_at).toLocaleDateString()}</td>
              </tr>
            );
          })}
          {items.length === 0 && (
            <tr>
              <td colSpan={6}>No flagged manufacturers.</td>
            </tr>
          )}
        </tbody>
      </table>

      <Pagination page={page} totalPages={totalPages} onPageChange={setPage} />
    </div>
  );
}
