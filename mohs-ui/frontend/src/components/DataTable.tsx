import type { ReactNode } from "react";
import { flexRender, useTable, type ColumnDef, type RowData, type Table as TableInstance } from "@tanstack/react-table";
import { ChevronLeftIcon, ChevronRightIcon, Columns3Icon } from "lucide-react";
import { features, type AppFeatures } from "../lib/table";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Panel, PanelFooter } from "./Panel";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

/**
 * Tall enough that the sticky header has something to stick to, short enough that the pager stays
 * reachable without scrolling the page first. A viewport unit rather than a row count: the row
 * height is a design decision that may change, the "most of the screen, not all of it" intent
 * does not.
 */
const TABLE_MAX_HEIGHT = "max-h-[calc(100vh-22rem)] min-h-64";

function columnLabel(header: unknown, fallbackId: string): string {
  return typeof header === "string" ? header : fallbackId;
}

function ColumnsMenu<T extends RowData>({ table }: { table: TableInstance<AppFeatures, T> }) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm">
          <Columns3Icon />
          Columns
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {table
          .getAllColumns()
          .filter((column) => column.getCanHide())
          .map((column) => (
            <DropdownMenuCheckboxItem
              key={column.id}
              checked={column.getIsVisible()}
              onCheckedChange={(checked) => column.toggleVisibility(checked)}
              onSelect={(event) => event.preventDefault()}
            >
              {columnLabel(column.columnDef.header, column.id)}
            </DropdownMenuCheckboxItem>
          ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * A grid inside its own panel. The panel is not the caller's job on purpose: two of the four
 * pages used to render a bare table with a column menu floating above it while the other two put
 * everything in panels, and a table that carries its own frame cannot drift from that again.
 *
 * <p>Widths come from the columns' declared `size`, never from the cells — see
 * {@link ../lib/table}.
 */
export function DataTable<T extends RowData>({
  title,
  description,
  data,
  columns,
  onRowClick,
  getRowId,
  rowAccent,
  footer,
}: {
  title: string;
  description?: string;
  data: T[];
  columns: ColumnDef<AppFeatures, T, any>[];
  onRowClick?: (row: T) => void;
  getRowId?: (row: T) => string;
  /** A CSS color for the 3px bar on the row's edge — typically tied to its state. */
  rowAccent?: (row: T) => string | undefined;
  footer?: ReactNode;
}) {
  const table = useTable({ features, data, columns, getRowId });

  return (
    <Panel title={title} description={description} action={<ColumnsMenu table={table} />} flush>
      {/*
        `table-fixed` plus the colgroup below is the whole point: it makes the browser lay the
        grid out from the declared widths and never from the cells. Without it every stream tick
        re-measures the content — a relative timestamp growing by one character is enough to
        shove every column sideways.
      */}
      <div className={cn("overflow-auto", TABLE_MAX_HEIGHT)}>
        <Table className="table-fixed">
          <colgroup>
            {table.getVisibleLeafColumns().map((column) => (
              <col key={column.id} style={{ width: `${column.getSize()}px` }} />
            ))}
          </colgroup>
          <TableHeader className="sticky top-0 z-10 bg-card">
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="hover:bg-transparent">
                {headerGroup.headers.map((header) => (
                  <TableHead key={header.id} className="mono-label h-9 font-mono text-muted-foreground">
                    {header.isPlaceholder ? null : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows.map((row) => {
              const accent = rowAccent?.(row.original);
              return (
                <TableRow
                  key={row.id}
                  onClick={() => onRowClick?.(row.original)}
                  style={accent ? { boxShadow: `inset 3px 0 0 0 ${accent}` } : undefined}
                  className={cn(onRowClick && "cursor-pointer")}
                >
                  {row.getVisibleCells().map((cell) => (
                    // Fixed widths mean content can no longer push a column wider, so it has to
                    // be told what to do instead: clip, with the full value one hover away.
                    <TableCell key={cell.id} className="truncate">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>
      {footer && <PanelFooter>{footer}</PanelFooter>}
    </Panel>
  );
}

/**
 * Previous/next over a keyset cursor — no total, no page count, because `CursorPage` carries
 * neither. Rendered as a {@link DataTable} footer, not as a floating row under the panel.
 */
export function CursorPager({
  pageNumber,
  hasPrev,
  hasNext,
  onPrev,
  onNext,
}: {
  pageNumber: number;
  hasPrev: boolean;
  hasNext: boolean;
  onPrev: () => void;
  onNext: () => void;
}) {
  return (
    <>
      <span className="mr-1 font-mono text-xs tabular-nums text-muted-foreground">page {pageNumber}</span>
      <Button variant="outline" size="icon-sm" disabled={!hasPrev} onClick={onPrev} aria-label="Previous page">
        <ChevronLeftIcon />
      </Button>
      <Button variant="outline" size="icon-sm" disabled={!hasNext} onClick={onNext} aria-label="Next page">
        <ChevronRightIcon />
      </Button>
    </>
  );
}
