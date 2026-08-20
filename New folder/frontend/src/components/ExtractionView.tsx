import { useState } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { StatementTab } from '@/components/StatementTab'
import type { ExtractionResult, ExtractionRow, StatementData } from '@/types'
import { ArrowRight, Loader2, RotateCcw } from 'lucide-react'

interface ExtractionViewProps {
  extraction: ExtractionResult
  onUpdateExtraction?: (updated: ExtractionResult) => void
  onStartMapping?: () => void
  onReExtract?: () => void
  extracting?: boolean
  mappingInProgress?: boolean
  hasBrefTemplate?: boolean
  hasMapping?: boolean
}

const STATEMENT_TABS = [
  { key: 'income_statement', label: 'Income Statement' },
  { key: 'balance_sheet', label: 'Balance Sheet' },
  { key: 'cash_flow', label: 'Cash Flow' },
  { key: 'notes', label: 'Notes' },
] as const

type TabKey = (typeof STATEMENT_TABS)[number]['key']
type StatementKey = Exclude<TabKey, 'notes'>

function normalizeStatementData(
  raw: StatementData | Record<string, unknown> | null,
  reportYear: number
): StatementData | null {
  if (!raw) return null

  const data = raw as Record<string, unknown>

  if (Array.isArray(data.rows) && data.rows.length > 0) {
    return raw as StatementData
  }

  const fields = data.fields as Record<string, Record<string, number>> | undefined
  if (!fields || Object.keys(fields).length === 0) return null

  const yearSet = new Set<string>()
  const rows: ExtractionRow[] = []

  for (const [label, yearValues] of Object.entries(fields)) {
    if (!yearValues || typeof yearValues !== 'object') continue
    const row: ExtractionRow = { label }
    for (const [year, val] of Object.entries(yearValues)) {
      yearSet.add(String(year))
      row[String(year)] = val
    }
    rows.push(row)
  }

  const yearHeaders = [...yearSet].sort().reverse()
  const pages = (data.pages as number[]) || []

  return {
    rows,
    year_headers: yearHeaders.length > 0 ? yearHeaders : [String(reportYear), String(reportYear - 1)],
    pages,
    unit_scale: (data.unit_scale as string) || null,
  }
}

interface NoteData {
  title?: string
  pages?: number[]
  breakdown?: Record<string, unknown>
  summary?: string
}

interface BreakdownRow {
  label: string
  values: Record<string, number | null>
  isGroup?: boolean
}

function flattenBreakdown(breakdown: Record<string, unknown>): {
  rows: BreakdownRow[]
  years: string[]
} {
  const rows: BreakdownRow[] = []
  const yearSet = new Set<string>()

  for (const [key, val] of Object.entries(breakdown)) {
    if (val == null) continue
    if (typeof val !== 'object') continue

    const entries = Object.entries(val as Record<string, unknown>)
    if (entries.length === 0) continue

    const isYearMap = entries.every(
      ([, v]) => typeof v === 'number' || v === null
    )

    if (isYearMap) {
      const values: Record<string, number | null> = {}
      for (const [year, num] of entries) {
        yearSet.add(year)
        values[year] = typeof num === 'number' ? num : null
      }
      rows.push({ label: key, values })
    } else {
      rows.push({ label: key, values: {}, isGroup: true })
      for (const [subKey, subVal] of entries) {
        if (subVal != null && typeof subVal === 'object') {
          const subEntries = Object.entries(subVal as Record<string, unknown>)
          const isSubYearMap = subEntries.every(
            ([, v]) => typeof v === 'number' || v === null
          )
          if (isSubYearMap) {
            const values: Record<string, number | null> = {}
            for (const [year, num] of subEntries) {
              yearSet.add(year)
              values[year] = typeof num === 'number' ? num : null
            }
            rows.push({ label: subKey, values })
          } else {
            rows.push({ label: subKey, values: {}, isGroup: true })
            for (const [deepKey, deepVal] of subEntries) {
              if (typeof deepVal === 'number') {
                yearSet.add(deepKey)
              } else if (deepVal != null && typeof deepVal === 'object') {
                const values: Record<string, number | null> = {}
                for (const [y, n] of Object.entries(deepVal as Record<string, unknown>)) {
                  yearSet.add(y)
                  values[y] = typeof n === 'number' ? n : null
                }
                rows.push({ label: deepKey, values })
              }
            }
          }
        }
      }
    }
  }

  const years = [...yearSet].sort()
  return { rows, years }
}

function NotesView({ notes }: { notes: Record<string, unknown> | null }) {
  if (!notes || Object.keys(notes).length === 0) {
    return (
      <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
        No notes extracted
      </div>
    )
  }

  return (
    <div className="space-y-4 overflow-auto" style={{ maxHeight: '520px' }}>
      {Object.entries(notes).map(([noteKey, noteVal]) => {
        const note = noteVal as NoteData
        if (!note || typeof note !== 'object') return null

        const { rows: breakdownRows, years } = note.breakdown
          ? flattenBreakdown(note.breakdown)
          : { rows: [], years: [] }

        return (
          <div key={noteKey} className="rounded-lg border p-4">
            <div className="mb-2 flex items-center gap-2">
              <Badge variant="secondary" className="text-xs">
                {noteKey.replace('_', ' ').replace('note', 'Note ')}
              </Badge>
              <span className="text-sm font-medium">{note.title || 'Untitled'}</span>
              {note.pages && note.pages.length > 0 && (
                <span className="text-xs text-muted-foreground">
                  (Pages {note.pages.join(', ')})
                </span>
              )}
            </div>
            {note.summary && (
              <p className="mb-2 text-xs text-muted-foreground">{note.summary}</p>
            )}
            {breakdownRows.length > 0 && (
              <div className="rounded-md border">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      <th className="px-3 py-1.5 text-left font-medium">Item</th>
                      {years.map((y) => (
                        <th key={y} className="px-3 py-1.5 text-right font-medium">
                          {y}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {breakdownRows.map((row, idx) => (
                      <tr
                        key={idx}
                        className={`border-b last:border-0 ${row.isGroup ? 'bg-muted/30' : ''}`}
                      >
                        <td
                          className={`px-3 py-1.5 ${row.isGroup ? 'font-semibold' : ''}`}
                          style={!row.isGroup ? { paddingLeft: '24px' } : undefined}
                        >
                          {row.label}
                        </td>
                        {years.map((y) => (
                          <td key={y} className="px-3 py-1.5 text-right tabular-nums">
                            {row.isGroup
                              ? ''
                              : row.values[y] != null
                                ? row.values[y]!.toLocaleString()
                                : '-'}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

export function ExtractionView({
  extraction,
  onUpdateExtraction,
  onStartMapping,
  onReExtract,
  extracting,
  mappingInProgress,
  hasBrefTemplate,
  hasMapping,
}: ExtractionViewProps) {
  const [activeTab, setActiveTab] = useState<TabKey>('income_statement')

  const getNormalized = (key: StatementKey): StatementData | null => {
    return normalizeStatementData(extraction[key], extraction.report_year)
  }

  const handleUpdateRows = (statementType: string, rows: ExtractionRow[]) => {
    if (!onUpdateExtraction) return
    const updated = { ...extraction }
    const key = statementType as StatementKey
    const current = getNormalized(key)
    if (current) {
      updated[key] = { ...current, rows }
    }
    onUpdateExtraction(updated)
  }

  const getRowCount = (key: StatementKey): number => {
    const stmt = getNormalized(key)
    return stmt?.rows?.length ?? 0
  }

  const getNoteCount = (): number => {
    return extraction.notes ? Object.keys(extraction.notes).length : 0
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span className="flex items-center gap-2">
            Extracted Financial Statements
            <Badge variant="secondary">Year {extraction.report_year}</Badge>
          </span>
          <div className="flex items-center gap-2">
            {onReExtract && (
              <Button
                variant="outline"
                size="sm"
                onClick={onReExtract}
                disabled={extracting || mappingInProgress}
              >
                {extracting ? (
                  <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                ) : (
                  <RotateCcw className="mr-1 h-3 w-3" />
                )}
                {extracting ? 'Extracting...' : 'Re-extract'}
              </Button>
            )}
            {onStartMapping && hasBrefTemplate && (
              <Button
                size="sm"
                onClick={onStartMapping}
                disabled={extracting || mappingInProgress}
              >
                {mappingInProgress ? (
                  <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                ) : (
                  <ArrowRight className="mr-1 h-3 w-3" />
                )}
                {mappingInProgress
                  ? 'Mapping...'
                  : hasMapping
                    ? 'Re-run BREF Mapping'
                    : 'Start BREF Mapping'}
              </Button>
            )}
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex gap-1 rounded-lg border bg-muted/50 p-1">
          {STATEMENT_TABS.map((tab) => {
            const count =
              tab.key === 'notes'
                ? getNoteCount()
                : getRowCount(tab.key as StatementKey)
            return (
              <Button
                key={tab.key}
                variant={activeTab === tab.key ? 'default' : 'ghost'}
                size="sm"
                className="flex-1"
                onClick={() => setActiveTab(tab.key)}
              >
                {tab.label}
                {count > 0 && (
                  <Badge
                    variant={activeTab === tab.key ? 'secondary' : 'outline'}
                    className="ml-1.5"
                  >
                    {count}
                  </Badge>
                )}
              </Button>
            )
          })}
        </div>

        {activeTab === 'notes' ? (
          <NotesView notes={extraction.notes} />
        ) : (
          (() => {
            const data = getNormalized(activeTab as StatementKey)
            return data ? (
              <StatementTab
                statementType={activeTab}
                data={data}
                sourceId={extraction.source_id}
                onUpdateRows={handleUpdateRows}
              />
            ) : (
              <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
                No data extracted for this statement
              </div>
            )
          })()
        )}
      </CardContent>
    </Card>
  )
}
