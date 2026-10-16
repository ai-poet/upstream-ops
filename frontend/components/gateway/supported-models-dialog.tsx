import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Copy, Eraser, Loader2, Sparkles } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { copyText } from "@/lib/clipboard"
import { ModelMultiSelect } from "./model-multi-select"
import { UpstreamModelPicker } from "./upstream-model-picker"

/** 转发模型清单的简短摘要（表格单元格里显示）。 */
export function supportedModelsSummary(models?: string[] | null): string {
  const list = models ?? []
  if (list.length === 0) return "全部模型"
  if (list.length === 1) return list[0]
  return `${list[0]} 等 ${list.length} 个`
}

type Discovery =
  | { status: "idle" | "loading" }
  | { status: "error"; error: string }
  | { status: "success"; models: string[] }

/**
 * 编辑一条路由的「转发模型」，交互同 new-api 渠道的模型设置：
 * 模型多选下拉（可叉掉、可输入自定义）+ 从上游拉取这条路由能访问的模型勾选 + 复制 / 清除全部。
 * 父组件用 key 让每次打开都重新取初值。
 */
export function SupportedModelsDialog({
  open,
  routeLabel,
  initial,
  modelOptions,
  onFetchModels,
  onOpenChange,
  onSave,
}: {
  open: boolean
  routeLabel: string
  initial: string[]
  /** 组模型清单里的模型，作为下拉的候选 */
  modelOptions: string[]
  /** 拉取这条路由上游能访问的模型 */
  onFetchModels: () => Promise<string[]>
  onOpenChange: (open: boolean) => void
  onSave: (models: string[]) => void
}) {
  const [models, setModels] = useState<string[]>(initial)
  const [discovery, setDiscovery] = useState<Discovery>({ status: "idle" })
  const sequence = useRef(0)
  // 父组件每次渲染都会传新的函数，存进 ref，避免反复触发拉取
  const fetcher = useRef(onFetchModels)
  useEffect(() => {
    fetcher.current = onFetchModels
  }, [onFetchModels])

  const fetchModels = useCallback(async () => {
    const seq = ++sequence.current
    setDiscovery({ status: "loading" })
    try {
      const list = await fetcher.current()
      if (seq === sequence.current) setDiscovery({ status: "success", models: list })
    } catch (e) {
      if (seq === sequence.current) {
        setDiscovery({ status: "error", error: e instanceof Error ? e.message : "拉取模型失败" })
      }
    }
  }, [])

  // 打开即拉取一次：直接看到这条路由能访问哪些模型
  useEffect(() => {
    void fetchModels()
    return () => {
      sequence.current++
    }
  }, [fetchModels])

  const fetched = discovery.status === "success" ? discovery.models : []
  const options = useMemo(() => {
    const seen = new Set<string>()
    const out: string[] = []
    for (const m of [...fetched, ...modelOptions]) {
      const k = m.toLowerCase()
      if (!seen.has(k)) {
        seen.add(k)
        out.push(m)
      }
    }
    return out
  }, [fetched, modelOptions])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[min(90dvh,760px)] w-full max-w-lg flex-col gap-4 overflow-hidden sm:max-w-2xl">
        <DialogHeader className="shrink-0">
          <DialogTitle>转发模型 · {routeLabel}</DialogTitle>
          <DialogDescription>
            这条路由只转发选中的模型，其它模型不会分到它（同 new-api 渠道的模型列表）。一个都不选 = 转发全部模型。
            按客户端请求的模型名匹配；末尾加 * 表示前缀匹配，如 gpt-image-*。
          </DialogDescription>
        </DialogHeader>

        <div className="min-h-0 flex-1 space-y-4 overflow-y-auto pr-1">
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm font-medium">模型</span>
              <span className="text-xs text-muted-foreground">
                {models.length === 0 ? "未选择 = 全部模型" : `已选 ${models.length} 个`}
              </span>
            </div>
            <ModelMultiSelect options={options} selected={models} onChange={setModels} />
          </div>

          <div aria-live="polite" className="space-y-3">
            {discovery.status === "loading" ? (
              <div className="flex items-center gap-2 rounded-lg border border-border p-3 text-sm text-muted-foreground">
                <Loader2 className="size-4 animate-spin" /> 正在拉取这条路由能访问的模型…
              </div>
            ) : null}
            {discovery.status === "error" ? (
              <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm">
                <span className="min-w-0 break-all text-destructive">拉取模型失败：{discovery.error}</span>
                <Button type="button" size="sm" variant="outline" onClick={() => void fetchModels()}>
                  重试
                </Button>
              </div>
            ) : null}
            {discovery.status === "success" && discovery.models.length === 0 ? (
              <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border p-3 text-sm text-muted-foreground">
                <span>上游没有返回模型，可以在上面手动输入。</span>
                <Button type="button" size="sm" variant="outline" onClick={() => void fetchModels()}>
                  重试
                </Button>
              </div>
            ) : null}
            {discovery.status === "success" && discovery.models.length > 0 ? (
              <UpstreamModelPicker models={discovery.models} selected={models} existing={initial} onChange={setModels} />
            ) : null}
          </div>

          <div className="space-y-2 border-t border-border pt-3">
            <div>
              <p className="text-sm font-medium">快捷操作</p>
              <p className="text-xs text-muted-foreground">从上游拉取会用这条路由转发时的密钥调 /v1/models，列出它能访问的模型。</p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={discovery.status === "loading"}
                onClick={() => void fetchModels()}
              >
                <Sparkles className="size-4" /> 从上游获取
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={models.length === 0}
                onClick={() =>
                  void copyText(models.join(",")).then(
                    () => toast.success(`已复制 ${models.length} 个模型`),
                    () => toast.error("复制失败"),
                  )
                }
              >
                <Copy className="size-4" /> 复制全部
              </Button>
              <Button type="button" variant="ghost" size="sm" disabled={models.length === 0} onClick={() => setModels([])}>
                <Eraser className="size-4" /> 清除全部
              </Button>
            </div>
          </div>
        </div>

        <DialogFooter className="shrink-0 gap-2">
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button
            type="button"
            onClick={() => {
              onSave(models)
              onOpenChange(false)
            }}
          >
            确定
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
