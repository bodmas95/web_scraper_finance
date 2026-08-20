import { useState, useRef, useCallback } from 'react'
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
import type { MappingResult, FieldMapping } from '@/types'
import { exportMapping } from '@/api/client'
import { Download, Pencil, Save, X } from 'lucide-react'

interface MappingViewProps {
  mapping: MappingResult
  onUpdateMapping?: (updated: MappingResult) => void
}

const STATEMENT_TABS = [
  { key: 'income_statement', label: 'Income Statement' },
  { key: 'balance_sheet', label: 'Balance Sheet' },
  { key: 'cash_flow', label: 'Cash Flow' },
] as const

type StatementKey = (typeof STATEMENT_TABS)[number]['key']

function getFieldCode(fieldKey: string): string {
  return fieldKey.split(' | ')[0].trim()
}

function parseFormula(formula: string): Array<{ op: string; code: string }> {
  const clean = formula.replace(/\s/g, '').replace(/[()]/g, '')
  const tokens = clean.split(/([+\-*/])/).filter(Boolean)
  const parsed: Array<{ op: string; code: string }> = []
  if (tokens.length > 0) parsed.push({ op: '+', code: tokens[0] })
  for (let i = 1; i < tokens.length; i += 2) {
    if (i + 1 < tokens.length) {
      parsed.push({ op: tokens[i], code: tokens[i + 1] })
    }
  }
  return parsed
}

function recalculateAll(
  fields: Record<string, FieldMapping>
): Record<string, FieldMapping> {
  const updated = { ...fields }
  for (const k of Object.keys(updated)) {
    updated[k] = { ...updated[k] }
  }

  const maxIter = 5
  for (let iter = 0; iter < maxIter; iter++) {
    let changed = false
    for (const [key, data] of Object.entries(updated)) {
      if (data.match_method !== 'calculated') continue
      const formula = data.source_field
      if (!formula || !formula.match(/[A-Z]/)) continue

      const parts = parseFormula(formula)
      let result = 0
      let foundAny = false
      let valid = true

      for (const { op, code } of parts) {
        let val: number | null = null
        for (const [fk, fd] of Object.entries(updated)) {
          if (getFieldCode(fk) === code) {
            val = fd.current_year
            break
          }
        }
        const num = val ?? 0
        if (val !== null) foundAny = true

        if (op === '+') result += num
        else if (op === '-') result -= num
        else if (op === '*') result *= num
        else if (op === '/') {
          if (num === 0) { valid = false; break }
          result /= num
        }
      }

      const newVal = valid && foundAny ? Math.round(result * 100) / 100 : null
      if (newVal !== data.current_year) {
        updated[key] = { ...updated[key], current_year: newVal }
        changed = true
      }
    }
    if (!changed) break
  }
  return updated
}

function confidenceBadge(confidence: number) {
  if (confidence >= 0.9)
    return <Badge variant="default" className="text-[10px] bg-primary hover:bg-primary/90">High</Badge>
  if (confidence >= 0.7)
    return <Badge variant="secondary" className="text-[10px]">Med</Badge>
  if (confidence > 0)
    return <Badge variant="outline" className="text-[10px]">Low</Badge>
  return <Badge variant="destructive" className="text-[10px]">None</Badge>
}

function methodBadge(method: string) {
  const variants: Record<
    string,
    'default' | 'secondary' | 'outline' | 'destructive'
  > = {
    direct_value_match: 'default',
    alias_exact: 'default',
    alias_fuzzy: 'secondary',
    alias_match: 'secondary',
    llm_mapping: 'outline',
    composite_decomposition: 'outline',
    calculated: 'outline',
    blank_reference: 'outline',
    unmatched: 'destructive',
  }
  const labels: Record<string, string> = {
    direct_value_match: 'Value Match',
    alias_exact: 'Alias',
    alias_fuzzy: 'Fuzzy',
    alias_match: 'Alias',
    llm_mapping: 'LLM',
    composite_decomposition: 'Composite',
    calculated: 'Calculated',
    blank_reference: 'Blank',
    unmatched: 'Unmatched',
  }
  return (
    <Badge
      variant={variants[method] ?? 'outline'}
      className="text-[10px]"
    >
      {labels[method] ?? method.replace(/_/g, ' ')}
    </Badge>
  )
}

function formatNumber(val: number | null): string {
  if (val == null) return '-'
  return val.toLocaleString()
}

function getMetrics(fields: Record<string, FieldMapping> | null) {
  if (!fields) return { total: 0, matched: 0, high: 0, calculated: 0 }
  const entries = Object.values(fields)
  return {
    total: entries.length,
    matched: entries.filter((f) => f.match_method !== 'unmatched' && f.match_method !== 'blank_reference').length,
    high: entries.filter((f) => f.confidence >= 0.9).length,
    calculated: entries.filter((f) => f.match_method === 'calculated').length,
  }
}

function ReasonTooltip({
  data,
  children,
}: {
  data: FieldMapping
  children: React.ReactNode
}) {
  const [show, setShow] = useState(false)
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>()
  const wrapperRef = useRef<HTMLDivElement>(null)
  const tooltipRef = useRef<HTMLDivElement>(null)

  const handleEnter = () => {
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => setShow(true), 300)
  }
  const handleLeave = () => {
    clearTimeout(timeoutRef.current)
    timeoutRef.current = setTimeout(() => setShow(false), 200)
  }

  const hasReason = data.reason && data.match_method !== 'direct_value_match'

  const isComposite = data.match_method === 'composite_decomposition'
  const isCalculated = data.match_method === 'calculated'

  return (
    <div
      ref={wrapperRef}
      className="relative"
      onMouseEnter={handleEnter}
      onMouseLeave={handleLeave}
    >
      {children}
      {show && hasReason && (
        <div
          ref={tooltipRef}
          className="fixed z-[100] max-w-lg rounded-lg border bg-popover p-4 shadow-xl"
          style={{
            left: wrapperRef.current
              ? Math.min(
                  wrapperRef.current.getBoundingClientRect().left,
                  window.innerWidth - 450,
                )
              : 0,
            top: wrapperRef.current
              ? wrapperRef.current.getBoundingClientRect().top - 8
              : 0,
            transform: 'translateY(-100%)',
            minWidth: '320px',
          }}
          onMouseEnter={() => clearTimeout(timeoutRef.current)}
          onMouseLeave={handleLeave}
        >
          <div className="mb-2 flex items-center gap-1.5">
            {methodBadge(data.match_method)}
            {confidenceBadge(data.confidence)}
          </div>
          <p className="text-xs leading-relaxed text-popover-foreground whitespace-pre-wrap">
            {data.reason}
          </p>
          {data.source_field && (isCalculated || isComposite) && (
            <p className="mt-2 text-[10px] font-mono text-muted-foreground border-t pt-2">
              {isCalculated ? 'Formula' : 'Source'}: {data.source_field}
            </p>
          )}
        </div>
      )}
    </div>
  )
}

export function MappingView({ mapping, onUpdateMapping }: MappingViewProps) {
  const [activeTab, setActiveTab] = useState<StatementKey>('income_statement')
  const [editMode, setEditMode] = useState(false)
  const [editedFields, setEditedFields] = useState<Record<string, FieldMapping> | null>(null)

  const handleExport = async () => {
    const blob = await exportMapping(mapping.source_id)
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `BREF_${mapping.company_id}_${mapping.report_year}.xlsx`
    a.click()
    URL.revokeObjectURL(url)
  }

  const rawFields = mapping[activeTab]
  const activeFields = editMode && editedFields ? editedFields : rawFields
  const metrics = getMetrics(rawFields)

  const getFieldCount = (key: StatementKey): number => {
    const stmt = mapping[key]
    return stmt ? Object.keys(stmt).length : 0
  }

  const handleToggleEdit = () => {
    if (!editMode && rawFields) {
      setEditedFields(
        Object.fromEntries(
          Object.entries(rawFields).map(([k, v]) => [k, { ...v }])
        )
      )
      setEditMode(true)
    } else {
      setEditedFields(null)
      setEditMode(false)
    }
  }

  const handleCellEdit = useCallback((fieldKey: string, value: string) => {
    if (!editedFields) return
    const num = parseFloat(value)
    const newVal = !isNaN(num) && value !== '' ? num : null

    const patched = {
      ...editedFields,
      [fieldKey]: {
        ...editedFields[fieldKey],
        current_year: newVal,
      },
    }
    setEditedFields(recalculateAll(patched))
  }, [editedFields])

  const handleSave = () => {
    if (!editedFields || !onUpdateMapping) return
    const updated = { ...mapping, [activeTab]: editedFields }
    onUpdateMapping(updated)
    setEditedFields(null)
    setEditMode(false)
  }

  const handleDiscard = () => {
    setEditedFields(null)
    setEditMode(false)
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between text-base">
          <span className="flex items-center gap-2">
            BREF Mapping Results
            <Badge variant="secondary">{mapping.report_year - 1} / {mapping.report_year}</Badge>
          </span>
          <div className="flex items-center gap-2">
            {editMode && (
              <>
                <Button variant="default" size="sm" onClick={handleSave}>
                  <Save className="mr-1 h-3 w-3" />
                  Save
                </Button>
                <Button variant="outline" size="sm" onClick={handleDiscard}>
                  <X className="mr-1 h-3 w-3" />
                  Discard
                </Button>
              </>
            )}
            <Button
              variant={editMode ? 'secondary' : 'outline'}
              size="sm"
              onClick={handleToggleEdit}
            >
              <Pencil className="mr-1 h-3 w-3" />
              {editMode ? 'Editing' : 'Edit'}
            </Button>
            <Button variant="outline" size="sm" onClick={handleExport}>
              <Download className="mr-1 h-3 w-3" />
              Export
            </Button>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex gap-1 rounded-lg border border-border/60 bg-muted/40 p-1">
          {STATEMENT_TABS.map((tab) => (
            <Button
              key={tab.key}
              variant={activeTab === tab.key ? 'default' : 'ghost'}
              size="sm"
              className={`flex-1 ${
                activeTab === tab.key
                  ? 'bg-primary text-primary-foreground shadow-sm hover:bg-primary/90'
                  : ''
              }`}
              onClick={() => {
                setActiveTab(tab.key)
                setEditMode(false)
                setEditedFields(null)
              }}
            >
              {tab.label}
              {getFieldCount(tab.key) > 0 && (
                <Badge
                  variant={activeTab === tab.key ? 'secondary' : 'outline'}
                  className={`ml-1.5 ${
                    activeTab === tab.key
                      ? 'bg-white/20 text-white border-white/20 hover:bg-white/30'
                      : ''
                  }`}
                >
                  {getFieldCount(tab.key)}
                </Badge>
              )}
            </Button>
          ))}
        </div>

        {activeFields && metrics.total > 0 && (
          <div className="mb-4 grid grid-cols-4 gap-3">
            <div className="rounded-lg border border-border/80 bg-card p-3 text-center shadow-sm">
              <p className="text-2xl font-bold text-foreground">{metrics.total}</p>
              <p className="text-xs text-muted-foreground">Total Fields</p>
            </div>
            <div className="rounded-lg border border-green-200 bg-green-50/50 p-3 text-center shadow-sm">
              <p className="text-2xl font-bold text-green-600">
                {metrics.matched}
              </p>
              <p className="text-xs text-muted-foreground">Matched</p>
            </div>
            <div className="rounded-lg border border-primary/20 bg-accent/50 p-3 text-center shadow-sm">
              <p className="text-2xl font-bold text-primary">
                {metrics.high}
              </p>
              <p className="text-xs text-muted-foreground">High Confidence</p>
            </div>
            <div className="rounded-lg border border-primary/15 bg-secondary/60 p-3 text-center shadow-sm">
              <p className="text-2xl font-bold text-primary/80">
                {metrics.calculated}
              </p>
              <p className="text-xs text-muted-foreground">Calculated</p>
            </div>
          </div>
        )}

        {activeFields && Object.keys(activeFields).length > 0 ? (
          <div className="overflow-auto rounded-md border" style={{ maxHeight: '600px' }}>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="sticky top-0 z-10 min-w-[260px] bg-card">
                    BREF Field
                  </TableHead>
                  <TableHead className="sticky top-0 z-10 bg-card text-right">
                    {mapping.report_year - 1}
                  </TableHead>
                  <TableHead className="sticky top-0 z-10 bg-card text-right">
                    {mapping.report_year}
                  </TableHead>
                  <TableHead className="sticky top-0 z-10 bg-card min-w-[180px]">
                    Source
                  </TableHead>
                  <TableHead className="sticky top-0 z-10 bg-card">Method</TableHead>
                  <TableHead className="sticky top-0 z-10 bg-card">Conf.</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {Object.entries(activeFields).map(([name, data]) => {
                  const indent = data.indent_level ?? 0
                  const isCalc = data.match_method === 'calculated'
                  const isUnmatched = data.match_method === 'unmatched'
                  const isBlank = data.match_method === 'blank_reference'

                  return (
                    <TableRow
                      key={name}
                      className={
                        isCalc
                          ? 'bg-accent/40'
                          : isUnmatched
                            ? 'bg-red-50/30'
                            : isBlank
                              ? 'opacity-50'
                              : ''
                      }
                    >
                      <TableCell className="text-xs">
                        <span
                          className={`${isCalc ? 'font-semibold' : 'font-medium'}`}
                          style={{ paddingLeft: `${indent * 20}px` }}
                        >
                          {indent > 0 && (
                            <span className="mr-1 text-muted-foreground">└</span>
                          )}
                          {name}
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-xs tabular-nums">
                        {formatNumber(data.previous_year)}
                      </TableCell>
                      <TableCell className="text-right text-xs tabular-nums font-semibold">
                        {editMode && !isCalc && data.match_method !== 'blank_reference' ? (
                          <input
                            className="w-24 rounded border border-input bg-transparent px-1 py-0.5 text-right text-xs font-semibold"
                            value={data.current_year ?? ''}
                            onChange={(e) => handleCellEdit(name, e.target.value)}
                          />
                        ) : (
                          formatNumber(data.current_year)
                        )}
                      </TableCell>
                      <TableCell>
                        <ReasonTooltip data={data}>
                          <span
                            className={`max-w-[180px] truncate text-xs block ${
                              data.reason && data.match_method !== 'direct_value_match'
                                ? 'cursor-help border-b border-dashed border-muted-foreground/40 text-foreground'
                                : 'text-muted-foreground'
                            }`}
                          >
                            {data.source_field ?? '-'}
                          </span>
                        </ReasonTooltip>
                      </TableCell>
                      <TableCell>{methodBadge(data.match_method)}</TableCell>
                      <TableCell>{confidenceBadge(data.confidence)}</TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        ) : (
          <div className="flex h-40 items-center justify-center text-sm text-muted-foreground">
            No mapping data for this statement
          </div>
        )}
      </CardContent>
    </Card>
  )
}
