// 渠道（路由）模型测试：照 new-api 的 ChannelTestDialog——列出这条路由的模型，
// 可单测、可批量（5 个一批并发、批间隔 100ms、可停止），可选端点与流式，显示首字 / 总耗时与失败原因。
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { ChevronDown, Gauge, Loader2, Plus, RefreshCw, Square } from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { cn } from "@/lib/utils"
import type { GatewayModelTestResult, GatewayRoute, GatewayTestEndpoint } from "@/lib/api-types"
import { formatLatency, latencyTone, routeTestModelPlan, routeTestModels } from "./gateway-utils"

const BATCH_TEST_CONCURRENCY = 5
const BATCH_TEST_DELAY_MS = 100

type EndpointChoice = "auto" | GatewayTestEndpoint

const ENDPOINT_OPTIONS: { value: EndpointChoice; label: string }[] = [
  { value: "auto", label: "自动（按模型）" },
  { value: "responses", label: "Responses · /v1/responses" },
  { value: "chat", label: "Chat · /v1/chat/completions" },
  { value: "anthropic", label: "Anthropic · /v1/messages" },
  { value: "gemini", label: "Gemini · generateContent" },
  { value: "images", label: "生图 · /v1/images/generations" },
]

const ENDPOINT_LABELS: Record<string, string> = {
  chat: "Chat",
  responses: "Responses",
  anthropic: "Messages",
  gemini: "Gemini",
  images: "生图",
}

const PROTOCOL_LABELS: Record<string, string> = {
  openai: "Chat",
  openai_chat: "Chat",
  openai_responses: "Responses",
  anthropic: "Anthropic",
  gemini: "Gemini",
}

/** 测试端点对应的入站协议，用来判断是否经过协议转换 */
const ENDPOINT_PROTOCOL: Record<string, string> = {
  chat: "openai_chat",
  images: "openai_chat",
  responses: "openai_responses",
  anthropic: "anthropic",
  gemini: "gemini",
}

export type RouteTestOptions = { endpoint?: GatewayTestEndpoint; stream: boolean }

type BatchProgress = { total: number; done: number; ok: number; failed: number }

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

type UpstreamState =
  | { status: "skip" | "loading" }
  | { status: "done"; models: string[] }
  | { status: "error"; error: string }

export function RouteTestDialog({
  open,
  onOpenChange,
  route,
  label,
  loadUpstreamModels,
  results,
  onTest,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  route: Partial<GatewayRoute> | null
  /** 路由名（渠道 / 源分组或直连渠道名） */
  label: string
  /** 拉这条路由上游的 /v1/models */
  loadUpstreamModels: () => Promise<string[]>
  /** 模型 → 这条路由最近一次测试结果 */
  results: Record<string, GatewayModelTestResult | undefined>
  onTest: (model: string, opts: RouteTestOptions) => Promise<GatewayModelTestResult | undefined>
}) {
  // 模型清单同 new-api 渠道自己的模型：转发模型的整名条目直接用；没配转发模型或有通配时，打开就拉上游
  const plan = useMemo(() => (route ? routeTestModelPlan(route) : { exact: [], needsUpstream: false }), [route])
  const [upstream, setUpstream] = useState<UpstreamState>({ status: plan.needsUpstream ? "loading" : "skip" })
  const loadRef = useRef(loadUpstreamModels)
  loadRef.current = loadUpstreamModels
  const fetchUpstream = useCallback(async () => {
    setUpstream({ status: "loading" })
    try {
      setUpstream({ status: "done", models: await loadRef.current() })
    } catch (e) {
      setUpstream({ status: "error", error: e instanceof Error ? e.message : "拉取失败" })
    }
  }, [])
  useEffect(() => {
    if (open && plan.needsUpstream) void fetchUpstream()
  }, [open, plan.needsUpstream, fetchUpstream])
  const models = useMemo(
    () => (route ? routeTestModels(route, upstream.status === "done" ? upstream.models : []) : []),
    [route, upstream],
  )

  const [endpoint, setEndpoint] = useState<EndpointChoice>("auto")
  const [stream, setStream] = useState(true)
  const [filter, setFilter] = useState("")
  const [extraModels, setExtraModels] = useState<string[]>([])
  const [customModel, setCustomModel] = useState("")
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [testing, setTesting] = useState<Set<string>>(new Set())
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [batch, setBatch] = useState<BatchProgress | null>(null)
  const [stopping, setStopping] = useState(false)
  const stopRef = useRef(false)

  const streamDisabled = endpoint === "images"
  const effectiveStream = !streamDisabled && stream

  // 路由的模型 + 这次手动加的 + 之前在这条路由上测过的（关掉再打开时手动加的模型不丢）
  const allModels = useMemo(() => {
    const out: string[] = []
    const seen = new Set<string>()
    const tested = Object.keys(results).filter((m) => results[m])
    for (const m of [...models, ...extraModels, ...tested]) {
      const k = m.toLowerCase()
      if (seen.has(k)) continue
      seen.add(k)
      out.push(m)
    }
    return out
  }, [models, extraModels, results])

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase()
    return q ? allModels.filter((m) => m.toLowerCase().includes(q)) : allModels
  }, [allModels, filter])

  const counts = useMemo(() => {
    let ok = 0
    let failed = 0
    for (const m of allModels) {
      const r = results[m]
      if (!r) continue
      if (r.ok) ok++
      else failed++
    }
    return { ok, failed, untested: allModels.length - ok - failed }
  }, [allModels, results])

  const markTesting = (model: string, on: boolean) =>
    setTesting((prev) => {
      const next = new Set(prev)
      if (on) next.add(model)
      else next.delete(model)
      return next
    })

  const opts = (): RouteTestOptions => ({
    endpoint: endpoint === "auto" ? undefined : endpoint,
    stream: effectiveStream,
  })

  async function testOne(model: string, o: RouteTestOptions = opts()) {
    markTesting(model, true)
    try {
      return await onTest(model, o)
    } catch (e) {
      toast.error(`${model}：${e instanceof Error ? e.message : "测试失败"}`)
      return undefined
    } finally {
      markTesting(model, false)
    }
  }

  async function runBatch(list: string[]) {
    const unique = [...new Set(list.map((m) => m.trim()).filter(Boolean))]
    if (unique.length === 0) return
    const o = opts()
    stopRef.current = false
    setStopping(false)
    let done = 0
    let ok = 0
    setBatch({ total: unique.length, done: 0, ok: 0, failed: 0 })
    try {
      for (let i = 0; i < unique.length; i += BATCH_TEST_CONCURRENCY) {
        if (stopRef.current) break
        const chunk = unique.slice(i, i + BATCH_TEST_CONCURRENCY)
        await Promise.allSettled(
          chunk.map(async (m) => {
            const r = await testOne(m, o)
            done++
            if (r?.ok) ok++
            setBatch({ total: unique.length, done, ok, failed: done - ok })
          }),
        )
        if (stopRef.current || i + BATCH_TEST_CONCURRENCY >= unique.length) break
        await sleep(BATCH_TEST_DELAY_MS)
      }
      const failed = done - ok
      if (stopRef.current && done < unique.length) {
        toast.message(`已停止：完成 ${done}/${unique.length}，成功 ${ok}，失败 ${failed}`)
      } else if (failed > 0) {
        toast.error(`测试完成：成功 ${ok}，失败 ${failed}`)
      } else {
        toast.success(`测试完成：${ok} 个模型全部可用`)
      }
    } finally {
      stopRef.current = false
      setStopping(false)
      setBatch(null)
      setSelected(new Set())
    }
  }

  function addCustomModel() {
    const m = customModel.trim()
    if (!m) return
    if (!allModels.some((x) => x.toLowerCase() === m.toLowerCase())) {
      setExtraModels((prev) => [...prev, m])
    }
    setCustomModel("")
    void testOne(m)
  }

  const batchTargets = selected.size > 0 ? filtered.filter((m) => selected.has(m)) : filtered
  const allFilteredSelected = filtered.length > 0 && filtered.every((m) => selected.has(m))
  const busy = batch != null

  return (
    <Dialog
      open={open}
      onOpenChange={(v) => {
        if (!v) stopRef.current = true
        onOpenChange(v)
      }}
    >
      <DialogContent className="flex max-h-[calc(100dvh-1rem)] flex-col gap-3 sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle className="flex flex-wrap items-center gap-x-2 pr-6">
            渠道测试
            <span className="min-w-0 truncate text-sm font-normal text-muted-foreground">{label}</span>
          </DialogTitle>
          <DialogDescription className="text-xs sm:text-sm">
            按这条路由已保存的配置给每个模型发一个 "hi"，看能否连通、记录首字与总耗时。会产生少量上游费用；不记使用记录，失败也不暂停路由。
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-[minmax(0,1fr)_auto] gap-3">
          <div className="space-y-1.5">
            <Label className="text-xs text-muted-foreground">测试端点</Label>
            <Select value={endpoint} onValueChange={(v) => setEndpoint(v as EndpointChoice)} disabled={busy}>
              <SelectTrigger className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {ENDPOINT_OPTIONS.map((o) => (
                  <SelectItem key={o.value} value={o.value}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {endpoint === "auto" ? (
              <p className="text-[11px] leading-4 text-muted-foreground">
                GPT / o 系 / codex 走 Responses，Claude 走 Messages，gpt-image 走生图，其余走 Chat
              </p>
            ) : null}
          </div>
          <div className="space-y-1.5">
            <Label className="text-xs text-muted-foreground">流式</Label>
            <div className="flex h-9 items-center gap-2">
              <Switch
                checked={effectiveStream}
                disabled={streamDisabled || busy}
                onCheckedChange={setStream}
              />
              <span className="text-xs text-muted-foreground">
                {streamDisabled ? "生图不支持" : effectiveStream ? "开" : "关"}
                <span className="hidden sm:inline">{!streamDisabled && effectiveStream ? "（量首字）" : ""}</span>
              </span>
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Input
            className="h-9 min-w-0 flex-1 basis-40"
            placeholder="筛选模型"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          {busy ? (
            <Button
              variant="outline"
              className="h-9"
              disabled={stopping}
              onClick={() => {
                stopRef.current = true
                setStopping(true)
              }}
            >
              <Square className="size-3.5" />
              {stopping ? "停止中…" : "停止"}
            </Button>
          ) : (
            <Button className="h-9" disabled={batchTargets.length === 0} onClick={() => void runBatch(batchTargets)}>
              <Gauge className="size-4" />
              {selected.size > 0
                ? `测试选中 ${batchTargets.length} 个`
                : filter.trim()
                  ? `测试匹配的 ${batchTargets.length} 个`
                  : `测试全部 ${batchTargets.length} 个`}
            </Button>
          )}
        </div>

        {plan.needsUpstream && upstream.status !== "loading" ? (
          <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
            {upstream.status === "error" ? (
              <span className="min-w-0 break-all text-red-700 dark:text-red-300">拉取上游模型失败：{upstream.error}</span>
            ) : (
              <span>
                {plan.exact.length > 0 ? "转发模型 + " : ""}上游 /v1/models 返回 {upstream.status === "done" ? upstream.models.length : 0} 个
              </span>
            )}
            <button
              type="button"
              className="inline-flex items-center gap-1 text-violet-700 hover:underline disabled:opacity-50 dark:text-violet-300"
              disabled={busy}
              onClick={() => void fetchUpstream()}
            >
              <RefreshCw className="size-3" />
              {upstream.status === "error" ? "重试" : "重新拉取"}
            </button>
          </div>
        ) : null}

        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <label className="flex items-center gap-1.5">
            <Checkbox
              checked={allFilteredSelected}
              disabled={busy || filtered.length === 0}
              onCheckedChange={(v) =>
                setSelected(v ? new Set(filtered) : new Set())
              }
            />
            全选
          </label>
          {batch ? (
            <span className="flex items-center gap-1 text-foreground">
              <Loader2 className="size-3.5 animate-spin" />
              测试中 {batch.done}/{batch.total} · 成功 {batch.ok} · 失败 {batch.failed}
            </span>
          ) : (
            <span>
              <span className="text-emerald-600 dark:text-emerald-400">成功 {counts.ok}</span> ·{" "}
              <span className="text-red-600 dark:text-red-400">失败 {counts.failed}</span> · 未测 {counts.untested}
            </span>
          )}
        </div>

        <div className="-mx-1 min-h-0 flex-1 space-y-2 overflow-y-auto px-1 sm:max-h-[50vh]">
          {filtered.map((m) => (
            <ModelRow
              key={m}
              model={m}
              result={results[m]}
              testing={testing.has(m)}
              disabled={busy}
              selected={selected.has(m)}
              expanded={expanded.has(m)}
              onSelect={(v) =>
                setSelected((prev) => {
                  const next = new Set(prev)
                  if (v) next.add(m)
                  else next.delete(m)
                  return next
                })
              }
              onToggleExpand={() =>
                setExpanded((prev) => {
                  const next = new Set(prev)
                  if (next.has(m)) next.delete(m)
                  else next.add(m)
                  return next
                })
              }
              onTest={() => void testOne(m)}
            />
          ))}
          {upstream.status === "loading" && filtered.length === 0 ? (
            <p className="flex items-center justify-center gap-2 rounded-lg border border-dashed border-border py-6 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              正在拉取这条路由上游的模型列表…
            </p>
          ) : filtered.length === 0 ? (
            <p className="rounded-lg border border-dashed border-border px-3 py-6 text-center text-sm text-muted-foreground">
              {allModels.length > 0
                ? "没有匹配的模型"
                : upstream.status === "error"
                  ? "没拿到模型列表，可点上面的重试，或在下面输入模型名测试"
                  : "上游没有返回模型，可在下面输入模型名测试"}
            </p>
          ) : null}
        </div>

        <div className="flex gap-2 border-t border-border pt-3">
          <Input
            className="h-9 min-w-0 flex-1"
            placeholder="测试其他模型名"
            value={customModel}
            onChange={(e) => setCustomModel(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") addCustomModel()
            }}
          />
          <Button variant="outline" className="h-9" disabled={!customModel.trim()} onClick={addCustomModel}>
            <Plus className="size-4" /> 测试
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}

function ModelRow({
  model,
  result,
  testing,
  disabled,
  selected,
  expanded,
  onSelect,
  onToggleExpand,
  onTest,
}: {
  model: string
  result?: GatewayModelTestResult
  testing: boolean
  disabled: boolean
  selected: boolean
  expanded: boolean
  onSelect: (v: boolean) => void
  onToggleExpand: () => void
  onTest: () => void
}) {
  const converted =
    !!result?.endpoint &&
    !!result.upstream_protocol &&
    ENDPOINT_PROTOCOL[result.endpoint] !== undefined &&
    normalizeProtocol(ENDPOINT_PROTOCOL[result.endpoint]) !== normalizeProtocol(result.upstream_protocol)
  return (
    <div
      className={cn(
        "rounded-lg border border-border p-2.5",
        result && !result.ok && !testing && "border-red-200 bg-red-50/40 dark:border-red-900/50 dark:bg-red-950/10",
      )}
    >
      <div className="flex items-center gap-2">
        <Checkbox checked={selected} disabled={disabled} onCheckedChange={(v) => onSelect(!!v)} />
        <span className="min-w-0 flex-1 truncate font-mono text-sm" title={model}>
          {model}
        </span>
        <StatusBadge result={result} testing={testing} />
        <Button
          size="icon"
          variant="ghost"
          className="size-8 shrink-0"
          disabled={testing || disabled}
          title="测试这个模型"
          onClick={onTest}
        >
          {testing ? <Loader2 className="size-4 animate-spin" /> : <Gauge className="size-4" />}
        </Button>
      </div>
      {result && !testing ? (
        <div className="mt-1 space-y-1 pl-6 text-xs">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-muted-foreground">
            {result.ok ? (
              <>
                {result.first_token_ms != null ? (
                  <span>
                    首字 <span className={cn("font-medium tabular-nums", latencyTone(result.first_token_ms, "first"))}>{formatLatency(result.first_token_ms)}</span>
                  </span>
                ) : null}
                <span>
                  总耗时 <span className={cn("font-medium tabular-nums", latencyTone(result.latency_ms, "total"))}>{formatLatency(result.latency_ms)}</span>
                </span>
              </>
            ) : (
              <span className="min-w-0 break-all text-red-700 dark:text-red-300">
                {result.status_code && (result.status_code < 200 || result.status_code >= 300)
                  ? `HTTP ${result.status_code} · `
                  : ""}
                {result.error || "失败"}
              </span>
            )}
            <span>
              {ENDPOINT_LABELS[result.endpoint ?? ""] ?? result.endpoint ?? "—"}
              {result.stream ? " · 流式" : ""}
              {converted ? ` → ${PROTOCOL_LABELS[result.upstream_protocol ?? ""] ?? result.upstream_protocol}` : ""}
            </span>
            <button type="button" className="inline-flex items-center gap-0.5 text-violet-700 hover:underline dark:text-violet-300" onClick={onToggleExpand}>
              详情
              <ChevronDown className={cn("size-3 transition-transform", expanded && "rotate-180")} />
            </button>
          </div>
          {result.ok && result.reply ? (
            <div className="truncate text-muted-foreground" title={result.reply}>
              回复：{result.reply}
            </div>
          ) : null}
          {expanded ? (
            <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-0.5 rounded-md bg-muted/40 p-2 font-mono text-[11px]">
              <dt className="text-muted-foreground">上游模型</dt>
              <dd className="break-all">{result.upstream_model || "—"}</dd>
              {result.response_model ? (
                <>
                  <dt className="text-muted-foreground">响应模型</dt>
                  <dd className="break-all">{result.response_model}</dd>
                </>
              ) : null}
              <dt className="text-muted-foreground">上游路径</dt>
              <dd className="break-all">{result.upstream_path || "—"}</dd>
              <dt className="text-muted-foreground">上游协议</dt>
              <dd>{PROTOCOL_LABELS[result.upstream_protocol ?? ""] ?? result.upstream_protocol ?? "—"}</dd>
              <dt className="text-muted-foreground">状态码</dt>
              <dd>{result.status_code || "—"}</dd>
              <dt className="text-muted-foreground">首字 / 总耗时</dt>
              <dd>
                {formatLatency(result.first_token_ms)} / {formatLatency(result.latency_ms)}
              </dd>
              {result.error ? (
                <>
                  <dt className="text-muted-foreground">错误</dt>
                  <dd className="break-all whitespace-pre-wrap">{result.error}</dd>
                </>
              ) : null}
            </dl>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

function normalizeProtocol(p?: string) {
  return p === "openai" ? "openai_chat" : (p ?? "")
}

function StatusBadge({ result, testing }: { result?: GatewayModelTestResult; testing: boolean }) {
  if (testing) {
    return (
      <Badge variant="outline" className="shrink-0 gap-1">
        <Loader2 className="size-3 animate-spin" />
        测试中
      </Badge>
    )
  }
  if (!result) {
    return (
      <Badge variant="outline" className="shrink-0 text-muted-foreground">
        未测试
      </Badge>
    )
  }
  return result.ok ? (
    <Badge className="shrink-0 bg-emerald-600 text-white hover:bg-emerald-600">成功</Badge>
  ) : (
    <Badge variant="destructive" className="shrink-0">
      失败
    </Badge>
  )
}
