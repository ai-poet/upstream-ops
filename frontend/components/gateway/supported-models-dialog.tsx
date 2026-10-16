import { useMemo, useState } from "react"
import { Plus, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"

/** 转发模型清单的简短摘要（表格单元格里显示）。 */
export function supportedModelsSummary(models?: string[] | null): string {
  const list = models ?? []
  if (list.length === 0) return "全部模型"
  if (list.length === 1) return list[0]
  return `${list[0]} 等 ${list.length} 个`
}

/** 把输入拆成模型名：逗号、空白、换行都算分隔。 */
function splitModels(text: string): string[] {
  return text
    .split(/[\s,，]+/)
    .map((m) => m.trim())
    .filter(Boolean)
}

/** 去重（不区分大小写，保留第一次出现的写法）。 */
function mergeModels(list: string[], add: string[]): string[] {
  const seen = new Set(list.map((m) => m.toLowerCase()))
  const out = [...list]
  for (const m of add) {
    const k = m.toLowerCase()
    if (!seen.has(k)) {
      seen.add(k)
      out.push(m)
    }
  }
  return out
}

/**
 * 编辑一条路由的「转发模型」（同 new-api 渠道的模型列表）。
 * 父组件用 key 让每次打开都重新取初值。
 */
export function SupportedModelsDialog({
  open,
  routeLabel,
  initial,
  modelOptions,
  onOpenChange,
  onSave,
}: {
  open: boolean
  routeLabel: string
  initial: string[]
  /** 组模型清单里的模型，供点选 */
  modelOptions: string[]
  onOpenChange: (open: boolean) => void
  onSave: (models: string[]) => void
}) {
  const [models, setModels] = useState<string[]>(initial)
  const [input, setInput] = useState("")
  const [filter, setFilter] = useState("")

  const selected = useMemo(() => new Set(models.map((m) => m.toLowerCase())), [models])
  const options = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return modelOptions.filter((m) => !selected.has(m.toLowerCase()) && (!q || m.toLowerCase().includes(q)))
  }, [modelOptions, selected, filter])

  const addInput = () => {
    const add = splitModels(input)
    if (add.length === 0) return
    setModels((prev) => mergeModels(prev, add))
    setInput("")
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[min(85dvh,680px)] w-full max-w-lg flex-col gap-3 overflow-hidden sm:max-w-xl">
        <DialogHeader className="shrink-0">
          <DialogTitle>转发模型 · {routeLabel}</DialogTitle>
          <DialogDescription>
            这条路由只接下面这些模型的请求，其它模型不会分到它（同 new-api 渠道的模型列表）。留空表示全部模型。
            按客户端请求的模型名匹配（组级映射后的名字也算），不区分大小写；末尾加 * 表示前缀匹配，如 gpt-image-*。
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-0 flex-1 space-y-4 overflow-y-auto pr-1">
          <div className="space-y-2">
            <div className="text-xs font-medium text-muted-foreground">
              已选 {models.length === 0 ? "（空 = 全部模型）" : `${models.length} 个`}
            </div>
            <div className="flex min-h-9 flex-wrap gap-1.5 rounded-md border border-border p-2">
              {models.length === 0 ? (
                <span className="text-xs text-muted-foreground">全部模型</span>
              ) : (
                models.map((m) => (
                  <Badge key={m} variant="secondary" className="gap-1 pr-1 font-mono text-xs">
                    {m}
                    <button
                      type="button"
                      className="rounded-sm p-0.5 hover:bg-muted-foreground/20"
                      aria-label={`移除 ${m}`}
                      onClick={() => setModels((prev) => prev.filter((x) => x !== m))}
                    >
                      <X className="size-3" />
                    </button>
                  </Badge>
                ))
              )}
            </div>
          </div>

          <div className="flex gap-2">
            <Input
              value={input}
              placeholder="输入模型名，回车添加；可一次粘贴多个"
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault()
                  addInput()
                }
              }}
            />
            <Button type="button" variant="outline" onClick={addInput} disabled={!input.trim()}>
              <Plus className="size-3.5" /> 添加
            </Button>
          </div>

          {modelOptions.length > 0 ? (
            <div className="space-y-2">
              <div className="flex items-center justify-between gap-2">
                <div className="text-xs font-medium text-muted-foreground">从组模型清单点选</div>
                {modelOptions.length > 12 ? (
                  <Input
                    className="h-7 w-40 text-xs"
                    value={filter}
                    placeholder="筛选"
                    onChange={(e) => setFilter(e.target.value)}
                  />
                ) : null}
              </div>
              <div className="flex max-h-40 flex-wrap gap-1.5 overflow-y-auto">
                {options.map((m) => (
                  <button
                    key={m}
                    type="button"
                    className="rounded-md border border-border px-2 py-0.5 font-mono text-xs text-muted-foreground hover:border-foreground/40 hover:text-foreground"
                    onClick={() => setModels((prev) => mergeModels(prev, [m]))}
                  >
                    + {m}
                  </button>
                ))}
                {options.length === 0 ? (
                  <span className="text-xs text-muted-foreground">没有可选的了</span>
                ) : null}
              </div>
            </div>
          ) : (
            <p className="text-xs text-muted-foreground">
              组模型清单还是空的；可以手动输入，或先在「模型映射」页同步模型。
            </p>
          )}
        </div>

        <DialogFooter className="shrink-0 gap-2 sm:justify-between">
          <Button type="button" variant="ghost" onClick={() => setModels([])} disabled={models.length === 0}>
            清空（全部模型）
          </Button>
          <div className="flex gap-2">
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              取消
            </Button>
            <Button
              type="button"
              onClick={() => {
                // 输入框里还没按回车的内容也算上
                onSave(mergeModels(models, splitModels(input)))
                onOpenChange(false)
              }}
            >
              确定
            </Button>
          </div>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
