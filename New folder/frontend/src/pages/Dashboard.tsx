import { useState } from 'react'
import { RegionSelector } from '@/components/RegionSelector'
import { FileUpload } from '@/components/FileUpload'
import { ExtractionView } from '@/components/ExtractionView'
import { MappingView } from '@/components/MappingView'
import { SummaryView } from '@/components/SummaryView'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import {
  triggerExtraction,
  triggerMapping,
  clearExtraction,
} from '@/api/client'
import type {
  Region,
  Country,
  Company,
  Source,
  ExtractionResult,
  MappingResult,
} from '@/types'
import { Play, Loader2 } from 'lucide-react'

export function Dashboard() {
  const [selection, setSelection] = useState<{
    region: Region
    country: Country
    company: Company
  } | null>(null)
  const [source, setSource] = useState<Source | null>(null)
  const [extraction, setExtraction] = useState<ExtractionResult | null>(null)
  const [mapping, setMapping] = useState<MappingResult | null>(null)
  const [extracting, setExtracting] = useState(false)
  const [mappingInProgress, setMappingInProgress] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSelectionChange = (sel: {
    region: Region
    country: Country
    company: Company
  }) => {
    setSelection(sel)
    setSource(null)
    setExtraction(null)
    setMapping(null)
  }

  const handleSourceReady = (src: Source) => {
    setSource(src)
  }

  const handleExtract = async () => {
    if (!source) return
    setExtracting(true)
    setError(null)
    try {
      const result = await triggerExtraction(source.id)
      setExtraction(result.extraction as ExtractionResult)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Extraction failed')
    } finally {
      setExtracting(false)
    }
  }

  const handleReExtract = async () => {
    if (!source) return
    setExtracting(true)
    setError(null)
    try {
      await clearExtraction(source.id)
      const result = await triggerExtraction(source.id)
      setExtraction(result.extraction as ExtractionResult)
      setMapping(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Re-extraction failed')
    } finally {
      setExtracting(false)
    }
  }

  const handleMap = async () => {
    if (!source) return
    setMappingInProgress(true)
    setError(null)
    try {
      const result = await triggerMapping(source.id, !!mapping)
      setMapping(result.mapping as MappingResult)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Mapping failed')
    } finally {
      setMappingInProgress(false)
    }
  }

  const handleUpdateExtraction = (updated: ExtractionResult) => {
    setExtraction(updated)
  }

  const hasBrefTemplate = source?.bref_template != null

  return (
    <div className="space-y-6">
      <RegionSelector onSelectionChange={handleSelectionChange} />

      {selection && (
        <FileUpload
          region={selection.region}
          country={selection.country}
          company={selection.company}
          onSourceReady={handleSourceReady}
        />
      )}

      {source && !extraction && (
        <Card>
          <CardContent className="flex items-center justify-between py-4">
            <div>
              <p className="text-sm font-medium">
                Ready to extract financial data
              </p>
              <p className="text-xs text-muted-foreground">
                AI agents will extract income statement, balance sheet, and cash
                flow from the annual report.
              </p>
            </div>
            <Button onClick={handleExtract} disabled={extracting}>
              {extracting ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <Play className="mr-2 h-4 w-4" />
              )}
              {extracting ? 'Extracting...' : 'Start Extraction'}
            </Button>
          </CardContent>
        </Card>
      )}

      {error && (
        <Card className="border-destructive">
          <CardContent className="py-4">
            <p className="text-sm text-destructive">{error}</p>
          </CardContent>
        </Card>
      )}

      {extraction && (
        <ExtractionView
          extraction={extraction}
          onUpdateExtraction={handleUpdateExtraction}
          onStartMapping={handleMap}
          onReExtract={handleReExtract}
          extracting={extracting}
          mappingInProgress={mappingInProgress}
          hasBrefTemplate={hasBrefTemplate}
          hasMapping={!!mapping}
        />
      )}

      {mapping && (
        <MappingView
          mapping={mapping}
          onUpdateMapping={setMapping}
        />
      )}

      {mapping && source && (
        <SummaryView
          sourceId={source.id}
          reportYear={mapping.report_year}
        />
      )}
    </div>
  )
}
