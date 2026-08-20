import { useState, useEffect, useCallback, useRef } from 'react'
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
import { fetchPageImage } from '@/api/client'
import type { StatementData, ExtractionRow } from '@/types'
import {
  ChevronLeft,
  ChevronRight,
  Pencil,
  Save,
  X,
  ZoomIn,
  ZoomOut,
  Loader2,
} from 'lucide-react'

interface StatementTabProps {
  statementType: string
  data: StatementData
  sourceId: string
  onUpdateRows?: (statementType: string, rows: ExtractionRow[]) => void
}

export function StatementTab({
  statementType,
  data,
  sourceId,
  onUpdateRows,
}: StatementTabProps) {
  const allPageNums = data.pages || []
  const [currentPageIdx, setCurrentPageIdx] = useState(0)
  const [pageImage, setPageImage] = useState<string | null>(null)
  const [pageImageLoading, setPageImageLoading] = useState(false)
  const [totalPages, setTotalPages] = useState<number | null>(null)

  const currentPageNum =
    allPageNums.length > 0 ? allPageNums[currentPageIdx] - 1 : null

  const loadPageImage = useCallback(
    async (pageNum: number) => {
      if (!sourceId) return
      setPageImageLoading(true)
      try {
        const result = await fetchPageImage(sourceId, pageNum)
        setPageImage(result.image)
        setTotalPages(result.total_pages)
      } catch {
        setPageImage(null)
      } finally {
        setPageImageLoading(false)
      }
    },
    [sourceId]
  )

  useEffect(() => {
    if (sourceId && currentPageNum != null) {
      loadPageImage(currentPageNum)
    } else {
      setPageImage(null)
    }
  }, [sourceId, currentPageNum, loadPageImage])

  const handlePrevPage = () =>
    setCurrentPageIdx((i) => Math.max(0, i - 1))
  const handleNextPage = () =>
    setCurrentPageIdx((i) => Math.min(allPageNums.length - 1, i + 1))

  const [zoomLevel, setZoomLevel] = useState(1)
  const handleZoomIn = () => setZoomLevel((z) => Math.min(z + 0.25, 3))
  const handleZoomOut = () => setZoomLevel((z) => Math.max(z - 0.25, 0.5))
  const handleImageWheel = (e: React.WheelEvent) => {
    if (e.ctrlKey || e.metaKey) {
      e.preventDefault()
      setZoomLevel((z) => {
        const delta = e.deltaY > 0 ? -0.1 : 0.1
        return Math.min(Math.max(z + delta, 0.5), 3)
      })
    }
  }

  const imageContainerRef = useRef<HTMLDivElement>(null)
  const isDragging = useRef(false)
  const dragStart = useRef({ x: 0, y: 0, scrollLeft: 0, scrollTop: 0 })

  const handleDragStart = (e: React.MouseEvent) => {
    if (zoomLevel <= 1) return
    const el = imageContainerRef.current
    if (!el) return
    isDragging.current = true
    dragStart.current = {
      x: e.clientX,
      y: e.clientY,
      scrollLeft: el.scrollLeft,
      scrollTop: el.scrollTop,
    }
    el.style.cursor = 'grabbing'
    e.preventDefault()
  }

  const handleDragMove = (e: React.MouseEvent) => {
    if (!isDragging.current) return
    const el = imageContainerRef.current
    if (!el) return
    el.scrollLeft =
      dragStart.current.scrollLeft - (e.clientX - dragStart.current.x)
    el.scrollTop =
      dragStart.current.scrollTop - (e.clientY - dragStart.current.y)
  }

  const handleDragEnd = () => {
    isDragging.current = false
    const el = imageContainerRef.current
    if (el) el.style.cursor = ''
  }

  const [editMode, setEditMode] = useState(false)
  const [editedRows, setEditedRows] = useState<ExtractionRow[] | null>(null)

  const yearHeaders = data.year_headers || []
  const rows = editMode && editedRows ? editedRows : data.rows || []

  const handleToggleEdit = () => {
    if (!editMode) {
      setEditedRows([...data.rows.map((r) => ({ ...r }))])
      setEditMode(true)
    } else {
      setEditedRows(null)
      setEditMode(false)
    }
  }

  const handleCellEdit = (
    rowIndex: number,
    key: string,
    value: string
  ) => {
    if (!editedRows) return
    const updated = [...editedRows]
    const num = parseFloat(value)
    updated[rowIndex] = {
      ...updated[rowIndex],
      [key]: !isNaN(num) && value !== '' ? num : value,
    }
    setEditedRows(updated)
  }

  const handleSave = () => {
    if (!editedRows || !onUpdateRows) return
    onUpdateRows(statementType, editedRows)
    setEditedRows(null)
    setEditMode(false)
  }

  const handleDiscard = () => {
    setEditedRows(null)
    setEditMode(false)
  }

  const formatNumber = (val: unknown): string => {
    if (val == null || val === '') return ''
    if (typeof val === 'number') return val.toLocaleString()
    return String(val)
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="secondary">{rows.length} rows</Badge>
        {data.unit_scale && (
          <Badge variant="outline">{data.unit_scale}</Badge>
        )}
        {allPageNums.length > 0 && (
          <Badge variant="outline">
            Page{allPageNums.length > 1 ? 's' : ''}{' '}
            {allPageNums.join(', ')}
          </Badge>
        )}
        {yearHeaders.map((y) => (
          <Badge key={y} variant="secondary">
            {y}
          </Badge>
        ))}

        <div className="ml-auto flex items-center gap-2">
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
        </div>
      </div>

      <div
        className={
          sourceId
            ? 'grid gap-4'
            : ''
        }
        style={sourceId ? { gridTemplateColumns: '55% 45%' } : undefined}
      >
        <div className="min-w-0 overflow-auto rounded-md border" style={{ maxHeight: '520px' }}>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="sticky top-0 z-10 min-w-[250px] bg-card">
                  Label
                </TableHead>
                {yearHeaders.map((y) => (
                  <TableHead
                    key={y}
                    className="sticky top-0 z-10 bg-card text-right"
                  >
                    {y}
                  </TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row, idx) => (
                <TableRow key={idx}>
                  <TableCell className="text-xs">
                    {row.parent ? (
                      <span>
                        <span className="text-muted-foreground">
                          {row.parent} {'>'}{' '}
                        </span>
                        {row.label}
                      </span>
                    ) : (
                      row.label
                    )}
                  </TableCell>
                  {yearHeaders.map((y) => (
                    <TableCell
                      key={y}
                      className="text-right text-xs tabular-nums"
                    >
                      {editMode ? (
                        <input
                          className="w-full rounded border border-input bg-transparent px-1 py-0.5 text-right text-xs"
                          value={row[y] ?? ''}
                          onChange={(e) =>
                            handleCellEdit(idx, y, e.target.value)
                          }
                        />
                      ) : (
                        formatNumber(row[y])
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>

        {sourceId && (
          <div className="flex flex-col overflow-hidden rounded-lg border">
            <div className="flex items-center justify-between border-b bg-muted/50 px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">
                PDF Page{' '}
                {currentPageNum != null ? currentPageNum + 1 : '?'}
                {totalPages != null && ` / ${totalPages}`}
              </span>
              <div className="flex items-center gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0"
                  onClick={handleZoomOut}
                  disabled={zoomLevel <= 0.5}
                >
                  <ZoomOut className="h-3 w-3" />
                </Button>
                <button
                  className="rounded px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground hover:bg-muted"
                  onClick={() => setZoomLevel(1)}
                >
                  {Math.round(zoomLevel * 100)}%
                </button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0"
                  onClick={handleZoomIn}
                  disabled={zoomLevel >= 3}
                >
                  <ZoomIn className="h-3 w-3" />
                </Button>
                {allPageNums.length > 1 && (
                  <>
                    <span className="mx-1 h-4 w-px bg-border" />
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 w-6 p-0"
                      disabled={currentPageIdx <= 0}
                      onClick={handlePrevPage}
                    >
                      <ChevronLeft className="h-3 w-3" />
                    </Button>
                    <span className="text-[10px] text-muted-foreground">
                      {currentPageIdx + 1}/{allPageNums.length}
                    </span>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-6 w-6 p-0"
                      disabled={currentPageIdx >= allPageNums.length - 1}
                      onClick={handleNextPage}
                    >
                      <ChevronRight className="h-3 w-3" />
                    </Button>
                  </>
                )}
              </div>
            </div>
            <div
              ref={imageContainerRef}
              className="flex-1 overflow-auto p-2"
              style={{
                maxHeight: '520px',
                cursor: zoomLevel > 1 ? 'grab' : 'default',
              }}
              onWheel={handleImageWheel}
              onMouseDown={handleDragStart}
              onMouseMove={handleDragMove}
              onMouseUp={handleDragEnd}
              onMouseLeave={handleDragEnd}
            >
              {pageImageLoading ? (
                <div className="flex h-64 items-center justify-center">
                  <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                </div>
              ) : pageImage ? (
                <img
                  src={pageImage}
                  alt={`PDF page ${(currentPageNum ?? 0) + 1}`}
                  className="origin-top-left select-none rounded shadow-sm"
                  draggable={false}
                  style={{
                    width: `${zoomLevel * 100}%`,
                    maxWidth: 'none',
                    transition: 'width 0.15s ease',
                  }}
                />
              ) : (
                <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
                  No page image available
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
