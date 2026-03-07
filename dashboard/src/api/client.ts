import axios from "axios";
import type {
  AuditLogEntry,
  Filters,
  FraudResult,
  PaginatedResponse,
  QualityResult,
} from "../types";

const api = axios.create({ baseURL: "/api/v1" });

function filterParams(f?: Filters): Record<string, string> {
  const p: Record<string, string> = {};
  if (f?.date_from) p.date_from = f.date_from;
  if (f?.date_to) p.date_to = f.date_to;
  if (f?.manufacturer) p.manufacturer = f.manufacturer;
  if (f?.category) p.category = f.category;
  return p;
}

/* ---- Fraud ---- */

export async function getFlaggedFraud(
  filters?: Filters,
  page = 1,
  pageSize = 50
): Promise<PaginatedResponse<FraudResult>> {
  const { data } = await api.get<PaginatedResponse<FraudResult>>(
    "/fraud/flagged",
    { params: { ...filterParams(filters), page, page_size: pageSize } }
  );
  return data;
}

export async function getFraudDetail(
  claimId: string
): Promise<FraudResult> {
  const { data } = await api.get<FraudResult>(`/fraud/claims/${claimId}`);
  return data;
}

export async function getFraudScoreBounds(): Promise<{ min: number; max: number }> {
  const { data } = await api.get<{ min: number; max: number }>("/fraud/score-bounds");
  return data;
}

export function fraudExportUrl(filters?: Filters): string {
  const qs = new URLSearchParams(filterParams(filters)).toString();
  return `/api/v1/fraud/export${qs ? `?${qs}` : ""}`;
}

/* ---- Quality ---- */

export async function getFlaggedQuality(
  filters?: Filters,
  page = 1,
  pageSize = 50
): Promise<PaginatedResponse<QualityResult>> {
  const { data } = await api.get<PaginatedResponse<QualityResult>>(
    "/quality/flagged",
    { params: { ...filterParams(filters), page, page_size: pageSize } }
  );
  return data;
}

export async function getQualityDetail(
  manufacturerId: string
): Promise<QualityResult> {
  const { data } = await api.get<QualityResult>(
    `/quality/manufacturers/${manufacturerId}`
  );
  return data;
}

export async function getQualityScoreBounds(): Promise<{ min: number; max: number }> {
  const { data } = await api.get<{ min: number; max: number }>("/quality/score-bounds");
  return data;
}

export function qualityExportUrl(filters?: Filters): string {
  const qs = new URLSearchParams(filterParams(filters)).toString();
  return `/api/v1/quality/export${qs ? `?${qs}` : ""}`;
}

/* ---- Audit ---- */

export async function getAuditLogs(params?: {
  entity_type?: string;
  entity_id?: string;
  date_from?: string;
  date_to?: string;
}): Promise<AuditLogEntry[]> {
  const { data } = await api.get<AuditLogEntry[]>("/audit/logs", {
    params,
  });
  return data;
}

/* ---- Quality Reasoning ---- */

export async function getQualityReasoning(
  manufacturerId: string
): Promise<{ manufacturer_id: string; reasoning: string }> {
  const { data } = await api.get<{ manufacturer_id: string; reasoning: string }>(
    `/quality/manufacturers/${manufacturerId}/reasoning`
  );
  return data;
}

/* ---- Fraud Reasoning ---- */

export async function getFraudReasoning(
  claimId: string
): Promise<{ claim_id: string; reasoning: string }> {
  const { data } = await api.get<{ claim_id: string; reasoning: string }>(
    `/fraud/claims/${claimId}/reasoning`
  );
  return data;
}
