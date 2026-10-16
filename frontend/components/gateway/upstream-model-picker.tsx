import { useId, useMemo, useState } from "react"
import { ChevronDown } from "lucide-react"
import { Checkbox } from "@/components/ui/checkbox"
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { Input } from "@/components/ui/input"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

// 按模型名前缀归到厂商（同 new-api 拉取模型后的分类展示）
const VENDOR_RULES: [string, RegExp][] = [
  ["OpenAI", /^(gpt|o\d|chatgpt|dall-e|text-embedding-(3|ada)|whisper|tts|omni|codex|sora|computer-use)/],
  ["Anthropic", /^claude/],
  ["Google", /^(gemini|gemma|imagen|veo|text-embedding-0|embedding-|learnlm)/],
  ["DeepSeek", /^deepseek/],
  ["xAI", /^grok/],
  ["阿里通义", /^(qwen|qwq|qvq|wan)/],
  ["智谱", /^(glm|cogview|cogvideo|chatglm)/],
  ["Moonshot", /^(kimi|moonshot)/],
  ["字节豆包", /^(doubao|seed|seedream|seedance)/],
  ["MiniMax", /^(minimax|abab|hailuo)/],
  ["Mistral", /^(mistral|codestral|pixtral|magistral|devstral)/],
  ["Meta", /^llama/],
]

function vendorOf(model: string): string {
  const m = model.toLowerCase().replace(/^.*\//, "")
  for (const [name, re] of VENDOR_RULES) {
    if (re.test(m)) return name
  }
  return "其它"
}

function groupByVendor(models: string[]): [string, string[]][] {
  const groups = new Map<string, string[]>()
  for (const m of models) {
    const v = vendorOf(m)
    groups.set(v, [...(groups.get(v) ?? []), m])
  }
  return [...groups.entries()].sort(([a], [b]) => (a === "其它" ? 1 : b === "其它" ? -1 : a.localeCompare(b)))
}

function VendorGroup({
  name,
  models,
  selected,
  onChange,
}: {
  name: string
  models: string[]
  selected: string[]
  onChange: (models: string[]) => void
}) {
  const id = useId()
  const selectedSet = new Set(selected)
  const count = models.filter((m) => selectedSet.has(m)).length
  const all = count === models.length
  return (
    <Collapsible defaultOpen className="rounded-lg border border-border">
      <div className="flex items-center gap-3 px-3">
        <Checkbox
          aria-label={`全选 ${name}`}
          checked={all ? true : count > 0 ? "indeterminate" : false}
          onCheckedChange={(checked) =>
            onChange(checked === true ? [...new Set([...selected, ...models])] : selected.filter((m) => !models.includes(m)))
          }
        />
        <CollapsibleTrigger className="group flex min-w-0 flex-1 items-center gap-2 py-2 text-sm">
          <span className="truncate font-medium">
            {name} ({models.length})
          </span>
          <span className="ml-auto text-xs text-muted-foreground">
            {count} / {models.length}
          </span>
          <ChevronDown className="size-4 transition-transform group-data-[state=closed]:-rotate-90" />
        </CollapsibleTrigger>
      </div>
      <CollapsibleContent className="border-t border-border px-3 py-2">
        <div className="grid gap-2 sm:grid-cols-2">
          {models.map((m) => (
            <div key={m} className="flex min-w-0 items-start gap-2">
              <Checkbox
                id={`${id}-${m}`}
                className="mt-0.5"
                checked={selectedSet.has(m)}
                onCheckedChange={(checked) =>
                  onChange(checked === true ? [...selected, m] : selected.filter((x) => x !== m))
                }
              />
              <label htmlFor={`${id}-${m}`} className="min-w-0 cursor-pointer break-all font-mono text-xs leading-5">
                {m}
              </label>
            </div>
          ))}
        </div>
      </CollapsibleContent>
    </Collapsible>
  )
}

type Tab = "new" | "existing" | "removed"

/**
 * 上游拉到的模型勾选（同 new-api 的 UpstreamModelSelection）：
 * 按厂商分组、组内可全选；分「新模型 / 已有 / 上游已没有」三栏，对比打开弹窗时已选的模型。
 */
export function UpstreamModelPicker({
  models,
  selected,
  existing,
  onChange,
}: {
  /** 上游返回的模型 */
  models: string[]
  /** 当前已选 */
  selected: string[]
  /** 打开弹窗时已选的模型（用于区分新增 / 已有 / 上游已没有） */
  existing: string[]
  onChange: (models: string[]) => void
}) {
  const [search, setSearch] = useState("")
  const groups = useMemo(() => {
    const q = search.trim().toLowerCase()
    const match = (m: string) => !q || m.toLowerCase().includes(q)
    const existingSet = new Set(existing.map((m) => m.toLowerCase()))
    const fetchedSet = new Set(models.map((m) => m.toLowerCase()))
    const fetched = models.filter(match)
    return {
      new: fetched.filter((m) => !existingSet.has(m.toLowerCase())),
      existing: fetched.filter((m) => existingSet.has(m.toLowerCase())),
      // 之前选了、上游这次没返回的（通配条目不算）
      removed: existing.filter((m) => !m.endsWith("*") && !fetchedSet.has(m.toLowerCase()) && match(m)),
    }
  }, [models, existing, search])

  const counts = { new: groups.new.length, existing: groups.existing.length, removed: groups.removed.length }
  const defaultTab: Tab = counts.new ? "new" : counts.existing ? "existing" : "removed"
  const [tab, setTab] = useState<Tab>(defaultTab)
  const activeTab: Tab = counts[tab] > 0 ? tab : defaultTab

  const list = (items: string[]) =>
    items.length === 0 ? (
      <p className="py-3 text-sm text-muted-foreground">没有匹配的模型</p>
    ) : (
      groupByVendor(items).map(([name, ms]) => (
        <VendorGroup key={name} name={name} models={ms} selected={selected} onChange={onChange} />
      ))
    )

  return (
    <div className="space-y-3">
      <Input placeholder="搜索模型..." value={search} onChange={(e) => setSearch(e.target.value)} />
      <Tabs value={activeTab} onValueChange={(v) => setTab(v as Tab)}>
        <TabsList className="flex h-auto w-full flex-wrap">
          <TabsTrigger value="new" disabled={!counts.new}>
            新模型 ({counts.new})
          </TabsTrigger>
          <TabsTrigger value="existing" disabled={!counts.existing}>
            已有 ({counts.existing})
          </TabsTrigger>
          {counts.removed > 0 ? <TabsTrigger value="removed">上游已没有 ({counts.removed})</TabsTrigger> : null}
        </TabsList>
        <TabsContent value="new" className="max-h-80 space-y-2 overflow-y-auto">
          {list(groups.new)}
        </TabsContent>
        <TabsContent value="existing" className="max-h-80 space-y-2 overflow-y-auto">
          {list(groups.existing)}
        </TabsContent>
        {counts.removed > 0 ? (
          <TabsContent value="removed" className="max-h-80 space-y-2 overflow-y-auto">
            <p className="text-xs text-muted-foreground">
              这些模型之前选过，但上游这次没有返回。取消勾选即可移除，保留勾选则照旧转发。
            </p>
            {list(groups.removed)}
          </TabsContent>
        ) : null}
      </Tabs>
      <p className="text-sm text-muted-foreground">已选 {selected.length} 个模型</p>
    </div>
  )
}
