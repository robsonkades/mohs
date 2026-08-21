import { flexRender, useTable, type ColumnDef, type RowData } from "@tanstack/react-table";
import { ChevronLeftIcon, ChevronRightIcon, Columns3Icon } from "lucide-react";
import { features, type AppFeatures } from "../lib/table";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";

function columnLabel(header: unknown, fallbackId: string): string {
  return typeof header === "string" ? header : fallbackId;
}

export function DataTable<T extends RowData>({
  data,
  columns,
  onRowClick,
  getRowId,
  rowAccent,
}: {
  data: T[];
  columns: ColumnDef<AppFeatures, T, any>[];
  onRowClick?: (row: T) => void;
  getRowId?: (row: T) => string;
  /** Uma cor CSS para a barra de 3px na borda da linha — tipicamente ligada ao estado dela. */
  rowAccent?: (row: T) => string | undefined;
}) {
  const table = useTable({ features, data, columns, getRowId });

  return (
    <div className="flex flex-col gap-2">
      <div className="flex justify-end">
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
      </div>

      <div className="max-h-[70vh] overflow-auto rounded-lg border">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-background">
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
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}

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
    <div className="flex items-center justify-end gap-2 px-1 py-3">
      <span className="mr-2 font-mono text-xs tabular-nums text-muted-foreground">page {pageNumber}</span>
      <Button variant="outline" size="icon" disabled={!hasPrev} onClick={onPrev} aria-label="Previous page">
        <ChevronLeftIcon />
      </Button>
      <Button variant="outline" size="icon" disabled={!hasNext} onClick={onNext} aria-label="Next page">
        <ChevronRightIcon />
      </Button>
    </div>
  );
}
