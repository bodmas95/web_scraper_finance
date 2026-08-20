import axios from 'axios'
import type { Region, Source, ExtractionResult, MappingResult } from '@/types'

const api = axios.create({
  baseURL: '/api',
})

export async function fetchRegions(): Promise<Region[]> {
  const { data } = await api.get('/regions')
  return data
}

export async function addCompany(
  regionCode: string,
  countryCode: string,
  companyName: string,
  currency: string = 'USD',
  unit: number = 1000
) {
  const { data } = await api.post(
    `/regions/${regionCode}/countries/${countryCode}/companies`,
    { company_name: companyName, currency, unit }
  )
  return data
}

export async function uploadAnnualReport(
  file: File,
  companyId: string,
  regionCode: string,
  countryCode: string,
  reportYear: number
): Promise<{ source_id: string; filename: string }> {
  const formData = new FormData()
  formData.append('file', file)
  formData.append('company_id', companyId)
  formData.append('region_code', regionCode)
  formData.append('country_code', countryCode)
  formData.append('report_year', String(reportYear))
  const { data } = await api.post('/documents/annual-report', formData)
  return data
}

export async function uploadBrefTemplate(
  file: File,
  companyId: string,
  reportYear: number
): Promise<{ source_id: string; filename: string }> {
  const formData = new FormData()
  formData.append('file', file)
  formData.append('company_id', companyId)
  formData.append('report_year', String(reportYear))
  const { data } = await api.post('/documents/bref-template', formData)
  return data
}

export async function fetchDocuments(companyId: string): Promise<Source[]> {
  const { data } = await api.get(`/documents/${companyId}`)
  return data
}

export async function fetchSource(sourceId: string): Promise<Source> {
  const { data } = await api.get(`/documents/source/${sourceId}`)
  return data
}

export async function triggerExtraction(
  sourceId: string
): Promise<{ status: string; extraction: ExtractionResult }> {
  const { data } = await api.post(`/extract/${sourceId}`)
  return data
}

export async function getExtraction(sourceId: string): Promise<ExtractionResult> {
  const { data } = await api.get(`/extract/${sourceId}`)
  return data
}

export async function clearExtraction(sourceId: string): Promise<void> {
  await api.delete(`/extract/${sourceId}`)
}

export async function triggerMapping(
  sourceId: string,
  force: boolean = false
): Promise<{ status: string; mapping: MappingResult }> {
  const { data } = await api.post(`/map/${sourceId}?force=${force}`)
  return data
}

export async function getMapping(sourceId: string): Promise<MappingResult> {
  const { data } = await api.get(`/map/${sourceId}`)
  return data
}

export async function clearMapping(sourceId: string): Promise<void> {
  await api.delete(`/map/${sourceId}`)
}

export async function exportMapping(sourceId: string): Promise<Blob> {
  const { data } = await api.get(`/map/${sourceId}/export`, {
    responseType: 'blob',
  })
  return data
}

export async function fetchPageImage(
  sourceId: string,
  pageNum: number
): Promise<{ image: string; page_num: number; total_pages: number }> {
  const { data } = await api.get(`/extract/page-image/${sourceId}`, {
    params: { page_num: pageNum },
  })
  return data
}

export async function fetchSummary(
  sourceId: string
): Promise<Record<string, SummaryRow[]>> {
  const { data } = await api.get(`/map/${sourceId}/summary`)
  return data
}

export async function exportSummary(sourceId: string): Promise<Blob> {
  const { data } = await api.get(`/map/${sourceId}/summary/export`, {
    responseType: 'blob',
  })
  return data
}

export interface SummaryRow {
  metric: string
  previous_year: number | null
  current_year: number | null
  derivation: string
  is_calculated?: boolean
  is_highlight?: boolean
}
