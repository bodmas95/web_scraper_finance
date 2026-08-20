import { useCallback, useState, useRef } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import {
  uploadAnnualReport,
  uploadBrefTemplate,
  fetchDocuments,
} from '@/api/client'
import type { Company, Country, Region, Source } from '@/types'
import {
  Upload,
  FileText,
  FileSpreadsheet,
  CheckCircle2,
  Loader2,
} from 'lucide-react'
import { useEffect } from 'react'

interface FileUploadProps {
  region: Region
  country: Country
  company: Company
  onSourceReady: (source: Source) => void
}

export function FileUpload({
  region,
  country,
  company,
  onSourceReady,
}: FileUploadProps) {
  const [sources, setSources] = useState<Source[]>([])
  const [reportYear, setReportYear] = useState<number>(new Date().getFullYear())
  const [uploading, setUploading] = useState<'ar' | 'bref' | null>(null)
  const [activeSource, setActiveSource] = useState<Source | null>(null)

  const onSourceReadyRef = useRef(onSourceReady)
  onSourceReadyRef.current = onSourceReady

  const loadDocuments = useCallback(async () => {
    const docs = await fetchDocuments(company.company_id)
    setSources(docs)
    if (docs.length > 0) {
      const latest = docs[0]
      setActiveSource(latest)
      setReportYear(latest.report_year)
      if (latest.annual_report && latest.bref_template) {
        onSourceReadyRef.current(latest)
      }
    }
  }, [company.company_id])

  useEffect(() => {
    loadDocuments()
  }, [loadDocuments])

  const handleAnnualReportUpload = async (
    e: React.ChangeEvent<HTMLInputElement>
  ) => {
    const file = e.target.files?.[0]
    if (!file) return
    setUploading('ar')
    try {
      const result = await uploadAnnualReport(
        file,
        company.company_id,
        region.region_code,
        country.country_code,
        reportYear
      )
      await loadDocuments()
      setActiveSource((prev) =>
        prev
          ? { ...prev, annual_report: { filename: file.name, gridfs_id: '' }, id: result.source_id }
          : null
      )
    } finally {
      setUploading(null)
    }
  }

  const handleBrefUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !activeSource) return
    setUploading('bref')
    try {
      await uploadBrefTemplate(file, company.company_id, reportYear)
      await loadDocuments()
    } finally {
      setUploading(null)
    }
  }

  const hasAR = activeSource?.annual_report != null
  const hasBREF = activeSource?.bref_template != null

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span>Documents - {company.company_name}</span>
          {sources.length > 0 && (
            <Badge variant="secondary">
              {sources.length} report{sources.length > 1 ? 's' : ''} cached
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          <div className="flex items-end gap-4">
            <div className="space-y-2">
              <Label>Report Year</Label>
              <input
                type="number"
                className="flex h-9 w-28 rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                value={reportYear}
                onChange={(e) => setReportYear(Number(e.target.value))}
              />
            </div>
          </div>

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div className="rounded-lg border-2 border-dashed p-4">
              <div className="flex items-center gap-3">
                <FileText className="h-8 w-8 text-muted-foreground" />
                <div className="flex-1">
                  <p className="text-sm font-medium">Annual Report (PDF)</p>
                  {hasAR ? (
                    <div className="flex items-center gap-1 text-xs text-green-600">
                      <CheckCircle2 className="h-3 w-3" />
                      {activeSource.annual_report?.filename}
                    </div>
                  ) : (
                    <p className="text-xs text-muted-foreground">
                      No report uploaded
                    </p>
                  )}
                </div>
                <div>
                  <input
                    type="file"
                    accept=".pdf"
                    className="hidden"
                    id="ar-upload"
                    onChange={handleAnnualReportUpload}
                  />
                  <label htmlFor="ar-upload">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={uploading === 'ar'}
                      onClick={() => document.getElementById('ar-upload')?.click()}
                    >
                      {uploading === 'ar' ? (
                        <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                      ) : (
                        <Upload className="mr-1 h-3 w-3" />
                      )}
                      {hasAR ? 'Replace' : 'Upload'}
                    </Button>
                  </label>
                </div>
              </div>
            </div>

            <div className="rounded-lg border-2 border-dashed p-4">
              <div className="flex items-center gap-3">
                <FileSpreadsheet className="h-8 w-8 text-muted-foreground" />
                <div className="flex-1">
                  <p className="text-sm font-medium">BREF Template (Excel)</p>
                  {hasBREF ? (
                    <div className="flex items-center gap-1 text-xs text-green-600">
                      <CheckCircle2 className="h-3 w-3" />
                      {activeSource.bref_template?.filename}
                    </div>
                  ) : (
                    <p className="text-xs text-muted-foreground">
                      {hasAR
                        ? 'Upload BREF template'
                        : 'Upload annual report first'}
                    </p>
                  )}
                </div>
                <div>
                  <input
                    type="file"
                    accept=".xlsx,.xls"
                    className="hidden"
                    id="bref-upload"
                    onChange={handleBrefUpload}
                    disabled={!hasAR}
                  />
                  <label htmlFor="bref-upload">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!hasAR || uploading === 'bref'}
                      onClick={() => document.getElementById('bref-upload')?.click()}
                    >
                      {uploading === 'bref' ? (
                        <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                      ) : (
                        <Upload className="mr-1 h-3 w-3" />
                      )}
                      {hasBREF ? 'Replace' : 'Upload'}
                    </Button>
                  </label>
                </div>
              </div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
