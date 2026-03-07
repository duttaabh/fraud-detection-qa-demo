import { useEffect, useState } from "react";
import { getAuditLogs } from "../api/client";
import type { AuditLogEntry } from "../types";

interface Props {
  entityType: string;
  entityId: string;
}

export default function AuditTrail({ entityType, entityId }: Props) {
  const [logs, setLogs] = useState<AuditLogEntry[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    getAuditLogs({ entity_type: entityType, entity_id: entityId })
      .then(setLogs)
      .finally(() => setLoading(false));
  }, [open, entityType, entityId]);

  return (
    <section className="audit-trail">
      <button onClick={() => setOpen(!open)} className="toggle-btn">
        {open ? "▾" : "▸"} Audit Trail ({open ? logs.length : "…"})
      </button>
      {open && (
        <div className="audit-list">
          {loading && <p>Loading…</p>}
          {!loading && logs.length === 0 && <p>No audit entries.</p>}
          {logs.map((entry, i) => (
            <div key={i} className="audit-entry">
              <span className="audit-type">{entry.event_type}</span>
              <time>{new Date(entry.created_at).toLocaleString()}</time>
              {entry.score != null && <span>Score: {entry.score}</span>}
              <pre>{JSON.stringify(entry.details, null, 2)}</pre>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
