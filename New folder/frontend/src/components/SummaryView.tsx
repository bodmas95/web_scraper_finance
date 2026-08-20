import { useState, useEffect } from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { fetchSummary, exportSummary } from '@/api/client'
import type { SummaryRow } from '@/api/client'
import { Download, Loader2, BarChart3 } from 'lucide-react'

interface SummaryViewProps {
  sourceId: string
  reportYear: number
}

const SUMMARY_TABS = [
  { key: 'income_statement', label: 'Income Statement' },
  { key: 'balance_sheet', label: 'Balance Sheet' },
  { key: 'cash_flow', label: 'Cash Flow' },
] as const

type SummaryKey = (typeof SUMMARY_TABS)[number]['key']

function formatValue(val: number | null, metric: string): string {
  if (val == null) return '-'
  const isPercent =
    metric.includes('%') ||
    metric.includes('margin') ||
    metric.includes('Margin') ||
    metric.includes('growth') ||
    metric.includes('Growth') ||
    metric.includes('Gearing') ||
    metric.includes('conversion')
  const isRatio =
    metric.includes('coverage') ||
    metric.includes('Leverage') ||
    metric.includes('Coverage')
  if (isPercent) return `${val.toFixed(1)}%`
  if (isRatio) return val.toFixed(1) + 'x'
  return val.toLocaleString()
}

export function SummaryView({ sourceId, reportYear }: SummaryViewProps) {
  const [activeTab, setActiveTab] = useState<SummaryKey>('income_statement')
  const [summaryData, setSummaryData] = useState<Record<string, SummaryRow[]> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchSummary(sourceId)
      .then((data) => {
        if (!cancelled) setSummaryData(data)
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : 'Failed to load summary')
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [sourceId])

  const handleExport = async () => {
    const blob = await exportSummary(sourceId)
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `Financial_Summary_${reportYear}.xlsx`
    a.click()
    URL.revokeObjectURL(url)
  }

  const activeRows = summaryData?.[activeTab] ?? []

  if (loading) {
    return (
      <Card>
        <CardContent className="flex h-40 items-center justify-center">
          <Loader2 className="mr-2 h-5 w-5 animate-spin text-primary" />
          <span className="text-sm text-muted-foreground">Generating summary...</span>
        </CardContent>
      </Card>
    )
  }

  if (error) {
    return (
      <Card className="border-destructive">
        <CardContent className="py-4">
          <p className="text-sm text-destructive">{error}</p>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span className="flex items-center gap-2">
            <BarChart3 className="h-4 w-4 text-primary" />
            Financial Summary
            <Badge variant="secondary">{reportYear - 1} / {reportYear}</Badge>
          </span>
          <Button variant="outline" size="sm" onClick={handleExport}>
            <Download className="mr-1 h-3 w-3" />
            Export Summary
          </Button>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex gap-1 rounded-lg border bg-muted/50 p-1">
          {SUMMARY_TABS.map((tab) => {
            const rows = summaryData?.[tab.key] ?? []
            return (
              <Button
                key={tab.key}
                variant={activeTab === tab.key ? 'default' : 'ghost'}
                size="sm"
                className="flex-1"
                onClick={() => setActiveTab(tab.key)}
              >
                {tab.label}
                {rows.length > 0 && (
                  <Badge
                    variant={activeTab === tab.key ? 'secondary' : 'outline'}
                    className={activeTab === tab.key ? 'ml-1.5 bg-white/20 text-white' : 'ml-1.5'}
                  >
                    {rows.length}
                  </Badge>
                )}
              </Button>
            )
          })}
        </div>

        {activeRows.length > 0 ? (
          <div className="overflow-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="min-w-[280px] bg-card">Metric</TableHead>
                  <TableHead className="bg-card text-right">{reportYear - 1}</TableHead>
                  <TableHead className="bg-card text-right">{reportYear}</TableHead>
                  <TableHead className="bg-card min-w-[250px]">Derivation</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {activeRows.map((row, idx) => {
                  const isCalc = row.is_calculated
                  const isHighlight = row.is_highlight && !isCalc
                  return (
                    <TableRow
                      key={idx}
                      className={
                        isHighlight
                          ? 'bg-accent/40'
                          : isCalc
                            ? 'bg-secondary/40'
                            : ''
                      }
                    >
                      <TableCell className="text-xs">
                        <span
                          className={
                            isHighlight
                              ? 'font-semibold'
                              : isCalc
                                ? 'italic text-primary/80'
                                : 'font-medium'
                          }
                        >
                          {row.metric}
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-xs tabular-nums">
                        {formatValue(row.previous_year, row.metric)}
                      </TableCell>
                      <TableCell className="text-right text-xs tabular-nums font-semibold">
                        {formatValue(row.current_year, row.metric)}
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground font-mono">
                        {row.derivation}
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        ) : (
          <div className="flex h-32 items-center justify-center text-sm text-muted-foreground">
            No summary data for this statement
          </div>
        )}
      </CardContent>
    </Card>
  )
}
