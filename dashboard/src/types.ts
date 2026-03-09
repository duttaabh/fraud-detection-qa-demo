/** Fraud result from the backend. */
export interface FraudResult {
  claim_id: string;
  contract_id: string;
  sku: string | null;
  manufacturer_name: string | null;
  fraud_score: number;
  is_suspected_fraud: boolean;
  contributing_factors: string[];
  model_version: string;
  scored_at: string;
  batch_run_id?: string;
  claim_amount: number | null;
  claim_type: string | null;
  claim_date: string | null;
  product_category: string | null;
  status: string | null;
  description: string | null;
}

/** Per-category stats inside a quality result. */
export interface CategoryStats {
  category: string;
  claim_count: number;
  repair_rate: number;
}

/** Per-SKU stats inside a quality result. */
export interface SkuStats {
  sku: string;
  claim_count: number;
  repair_count: number;
  repair_rate: number;
}

/** Manufacturer quality result from the backend. */
export interface QualityResult {
  manufacturer_id: string;
  manufacturer_name: string;
  total_claims: number;
  repair_claim_rate: number;
  quality_score: number;
  is_quality_concern: boolean;
  product_category_breakdown: Record<string, CategoryStats>;
  sku_breakdown: Record<string, SkuStats>;
  model_version: string;
  scored_at: string;
  batch_run_id?: string;
}

/** Audit log entry. */
export interface AuditLogEntry {
  event_type: string;
  entity_type: string;
  entity_id: string;
  model_version: string | null;
  score: number | null;
  details: Record<string, unknown>;
  created_at: string;
}

/** Paginated response wrapper. */
export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

/** Common filter params used across fraud/quality endpoints. */
export interface Filters {
  date_from?: string;
  date_to?: string;
  manufacturer?: string;
  category?: string;
  score_min?: number;
  score_max?: number;
  reason?: string;
}
