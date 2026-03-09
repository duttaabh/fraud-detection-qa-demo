import { useState } from "react";
import type { Filters as FilterValues } from "../types";

interface Props {
  onApply: (f: FilterValues) => void;
  showScoreFilter?: boolean;
  scoreLabel?: string;
  scoreMinBound?: number;
  scoreMaxBound?: number;
  reasonOptions?: string[];
}

export default function Filters({ onApply, showScoreFilter = false, scoreLabel = "Score", scoreMinBound, scoreMaxBound, reasonOptions }: Props) {
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [manufacturer, setManufacturer] = useState("");
  const [category, setCategory] = useState("");
  const [scoreMin, setScoreMin] = useState("");
  const [scoreMax, setScoreMax] = useState("");
  const [reason, setReason] = useState("");

  const apply = () =>
    onApply({
      date_from: dateFrom || undefined,
      date_to: dateTo || undefined,
      manufacturer: manufacturer || undefined,
      category: category || undefined,
      score_min: scoreMin ? parseFloat(scoreMin) : undefined,
      score_max: scoreMax ? parseFloat(scoreMax) : undefined,
      reason: reason || undefined,
    });

  const clear = () => {
    setDateFrom("");
    setDateTo("");
    setManufacturer("");
    setCategory("");
    setScoreMin("");
    setScoreMax("");
    setReason("");
    onApply({});
  };

  return (
    <div className="filters">
      <label>
        From
        <input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
      </label>
      <label>
        To
        <input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
      </label>
      <label>
        Manufacturer
        <input type="text" value={manufacturer} onChange={(e) => setManufacturer(e.target.value)} placeholder="e.g. Acme Corp" />
      </label>
      <label>
        Category
        <input type="text" value={category} onChange={(e) => setCategory(e.target.value)} placeholder="e.g. electronics" />
      </label>
      {showScoreFilter && (
        <>
          <label>
            {scoreLabel} Min
            <input type="number" step="0.01" value={scoreMin} onChange={(e) => setScoreMin(e.target.value)} placeholder={scoreMinBound != null ? scoreMinBound.toFixed(2) : "0.00"} />
          </label>
          <label>
            {scoreLabel} Max
            <input type="number" step="0.01" value={scoreMax} onChange={(e) => setScoreMax(e.target.value)} placeholder={scoreMaxBound != null ? scoreMaxBound.toFixed(2) : "1.00"} />
          </label>
        </>
      )}
      {reasonOptions && reasonOptions.length > 0 && (
        <label>
          Reason
          <select value={reason} onChange={(e) => setReason(e.target.value)}>
            <option value="">All</option>
            {reasonOptions.map((opt) => (
              <option key={opt} value={opt}>{opt}</option>
            ))}
          </select>
        </label>
      )}
      <button onClick={apply}>Apply</button>
      <button onClick={clear} className="secondary">Clear</button>
    </div>
  );
}
