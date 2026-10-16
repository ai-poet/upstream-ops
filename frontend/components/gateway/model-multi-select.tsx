import { useMemo, useRef, useState, type ClipboardEvent, type KeyboardEvent } from "react"
import { Check, Plus, X } from "lucide-react"
import { toast } from "sonner"
import { Popover, PopoverAnchor, PopoverContent } from "@/components/ui/popover"
import { copyText } from "@/lib/clipboard"
import { cn } from "@/lib/utils"

// 逗号（含全角）与换行都是分隔符：输入或粘贴 "a, b, c" 一次加三个
const SEPARATOR = /[,，\n]/

/**
 * 模型多选下拉（交互同 new-api 渠道的模型选择）：
 * - 已选模型显示为标签，点 × 移除，点标签复制模型名；
 * - 输入框既是搜索也是自定义：回车选中高亮项，没有匹配项时添加输入的自定义模型；
 * - 输入或粘贴逗号 / 换行分隔的列表一次添加多个；输入框为空时退格删除最后一个。
 */
export function ModelMultiSelect({
  options,
  selected,
  onChange,
  placeholder = "选择模型或输入自定义模型",
  maxVisibleChips = 40,
}: {
  options: string[]
  selected: string[]
  onChange: (models: string[]) => void
  placeholder?: string
  maxVisibleChips?: number
}) {
  const [input, setInput] = useState("")
  const [open, setOpen] = useState(false)
  // null = 跟随默认：有匹配的模型时高亮第一个模型，否则高亮「添加自定义模型」
  const [highlight, setHighlight] = useState<number | null>(null)
  const [expanded, setExpanded] = useState(false)
  const anchorRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const selectedSet = useMemo(() => new Set(selected.map((m) => m.toLowerCase())), [selected])
  const query = input.trim()
  const items = useMemo(() => {
    const seen = new Set<string>()
    const all: string[] = []
    for (const m of [...options, ...selected]) {
      const k = m.toLowerCase()
      if (!seen.has(k)) {
        seen.add(k)
        all.push(m)
      }
    }
    const q = query.toLowerCase()
    return q ? all.filter((m) => m.toLowerCase().includes(q)) : all
  }, [options, selected, query])
  const canCreate = query !== "" && !items.some((m) => m.toLowerCase() === query.toLowerCase())
  // 下拉里的行：可新建时第一行是「添加自定义模型」
  const rows = canCreate ? [{ create: true, model: query }, ...items.map((m) => ({ create: false, model: m }))] : items.map((m) => ({ create: false, model: m }))
  const defaultRow = canCreate && items.length > 0 ? 1 : 0
  const active = Math.min(highlight ?? defaultRow, Math.max(rows.length - 1, 0))

  const add = (models: string[]) => {
    const next = [...selected]
    const seen = new Set(selectedSet)
    for (const raw of models) {
      const m = raw.trim()
      if (!m || seen.has(m.toLowerCase())) continue
      seen.add(m.toLowerCase())
      next.push(m)
    }
    if (next.length !== selected.length) onChange(next)
  }
  const remove = (model: string) => onChange(selected.filter((m) => m !== model))
  const toggle = (model: string) => {
    if (selectedSet.has(model.toLowerCase())) {
      onChange(selected.filter((m) => m.toLowerCase() !== model.toLowerCase()))
    } else {
      add([model])
    }
  }

  const handleInput = (value: string) => {
    if (SEPARATOR.test(value)) {
      const parts = value.split(SEPARATOR)
      add(parts.slice(0, -1))
      setInput(parts.at(-1) ?? "")
    } else {
      setInput(value)
    }
    setHighlight(null)
    setOpen(true)
  }
  const handlePaste = (e: ClipboardEvent<HTMLInputElement>) => {
    const text = e.clipboardData.getData("text/plain")
    if (!SEPARATOR.test(text)) return
    e.preventDefault()
    add(text.split(SEPARATOR))
    setInput("")
  }
  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.nativeEvent.isComposing) return
    if (e.key === "ArrowDown") {
      e.preventDefault()
      setOpen(true)
      setHighlight(Math.min(active + 1, rows.length - 1))
    } else if (e.key === "ArrowUp") {
      e.preventDefault()
      setHighlight(Math.max(active - 1, 0))
    } else if (e.key === "Enter") {
      e.preventDefault()
      const row = rows[active]
      if (open && row) {
        if (row.create) add([row.model])
        else toggle(row.model)
        setInput("")
      } else if (query) {
        add([query])
        setInput("")
      }
    } else if (e.key === "Escape") {
      if (open) {
        e.preventDefault()
        e.stopPropagation()
        setOpen(false)
      }
    } else if (e.key === "Backspace" && input === "" && selected.length > 0) {
      remove(selected[selected.length - 1])
    }
  }

  const visible = expanded ? selected : selected.slice(0, maxVisibleChips)
  const hidden = selected.length - visible.length

  return (
    <Popover open={open && rows.length > 0} onOpenChange={setOpen}>
      <PopoverAnchor asChild>
        <div
          ref={anchorRef}
          className="flex min-h-9 w-full cursor-text flex-wrap items-center gap-1 rounded-md border border-input bg-transparent px-1.5 py-1 shadow-xs focus-within:border-ring focus-within:ring-[3px] focus-within:ring-ring/50"
          onClick={() => {
            inputRef.current?.focus()
            setOpen(true)
          }}
        >
          {visible.map((m) => (
            <span
              key={m}
              className="inline-flex max-w-full items-center gap-0.5 rounded-sm bg-muted py-0.5 pr-0.5 pl-1.5 font-mono text-xs"
            >
              <button
                type="button"
                title="点击复制"
                className="max-w-64 truncate hover:underline"
                onClick={(e) => {
                  e.stopPropagation()
                  void copyText(m).then(
                    () => toast.success(`已复制 ${m}`),
                    () => toast.error("复制失败"),
                  )
                }}
              >
                {m}
              </button>
              <button
                type="button"
                aria-label={`移除 ${m}`}
                className="rounded-sm p-0.5 text-muted-foreground hover:bg-foreground/10 hover:text-foreground"
                onClick={(e) => {
                  e.stopPropagation()
                  remove(m)
                }}
              >
                <X className="size-3" />
              </button>
            </span>
          ))}
          {hidden > 0 || (expanded && selected.length > maxVisibleChips) ? (
            <button
              type="button"
              className="rounded-sm px-1.5 py-0.5 text-xs text-muted-foreground hover:bg-muted"
              onClick={(e) => {
                e.stopPropagation()
                setExpanded((v) => !v)
              }}
            >
              {expanded ? "收起" : `+${hidden} 个`}
            </button>
          ) : null}
          <input
            ref={inputRef}
            value={input}
            placeholder={selected.length === 0 ? placeholder : ""}
            className="h-7 min-w-28 flex-1 bg-transparent px-1 text-sm outline-none placeholder:text-muted-foreground"
            onChange={(e) => handleInput(e.target.value)}
            onPaste={handlePaste}
            onKeyDown={handleKeyDown}
          />
        </div>
      </PopoverAnchor>
      <PopoverContent
        align="start"
        className="max-h-72 w-(--radix-popper-anchor-width) overflow-y-auto p-1"
        onOpenAutoFocus={(e) => e.preventDefault()}
        onCloseAutoFocus={(e) => e.preventDefault()}
        onInteractOutside={(e) => {
          // 点输入框所在的标签区不算「外部」，免得下拉一开一关
          if (anchorRef.current?.contains(e.target as Node)) e.preventDefault()
        }}
      >
        {rows.map((row, i) => {
          const isSelected = !row.create && selectedSet.has(row.model.toLowerCase())
          return (
            <button
              key={(row.create ? "create:" : "") + row.model}
              type="button"
              className={cn(
                "flex w-full items-center gap-2 rounded-sm px-2 py-1.5 text-left text-sm",
                i === active && "bg-accent text-accent-foreground",
              )}
              onMouseEnter={() => setHighlight(i)}
              // 不抢输入框焦点，方便连续选择
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => {
                if (row.create) add([row.model])
                else toggle(row.model)
                setInput("")
              }}
            >
              {row.create ? (
                <>
                  <Plus className="size-3.5 shrink-0" />
                  <span className="truncate">
                    添加自定义模型 <span className="font-mono">“{row.model}”</span>
                  </span>
                </>
              ) : (
                <>
                  <Check className={cn("size-3.5 shrink-0", isSelected ? "opacity-100" : "opacity-0")} />
                  <span className="truncate font-mono text-xs">{row.model}</span>
                </>
              )}
            </button>
          )
        })}
      </PopoverContent>
    </Popover>
  )
}
