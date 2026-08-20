export interface Company {
  company_id: string
  company_name: string
  currency: string
  unit: number
}

export interface Country {
  country_code: string
  country_name: string
  companies: Company[]
}

export interface Region {
  region_code: string
  region_name: string
  countries: Country[]
}

export interface FileRef {
  filename: string
  gridfs_id: string
}

export interface Source {
  id: string
  company_id: string
  region_code: string
  country_code: string
  report_year: number
  annual_report: FileRef | null
  bref_template: FileRef | null
  uploaded_at: string
}

export interface ExtractionRow {
  label: string
  parent?: string
  [yearKey: string]: string | number | undefined
}

export interface StatementData {
  rows: ExtractionRow[]
  year_headers: string[]
  pages: number[]
  unit_scale: string | null
  fields?: Record<string, Record<string, number>>
}

export interface ExtractionResult {
  id: string
  source_id: string
  company_id: string
  report_year: number
  income_statement: StatementData | null
  balance_sheet: StatementData | null
  cash_flow: StatementData | null
  notes: Record<string, unknown> | null
  extracted_at: string
}

export interface FieldMapping {
  previous_year: number | null
  current_year: number | null
  source_field: string | null
  match_method: string
  confidence: number
  reason?: string
  indent_level?: number
}

export interface MappingResult {
  id: string
  source_id: string
  company_id: string
  region_code: string
  report_year: number
  income_statement: Record<string, FieldMapping> | null
  balance_sheet: Record<string, FieldMapping> | null
  cash_flow: Record<string, FieldMapping> | null
  mapped_at: string
}
