import { AlertTriangle, Pencil, Plus, RefreshCw, Trash2 } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import type { ModelPriceOverride, ReferencePrice, UnpricedModel } from "@/lib/api-types"
import { dateTime, formatTokens } from "@/lib/format"
import { cn } from "@/lib/utils"
import {
  parsePriceTiers,
  perTokenToMTok,
  type PriceFormState,
  type PriceTierFormState,
} from "./gateway-utils"

type PricesPanelProps = {
  busy: boolean
  prices: ModelPriceOverride[]
  /** 最近计费时找不到价格的模型 */
  unpriced: UnpricedModel[]
  unpricedDays: number
  onPriceUnpriced: (model: string) => void
  /** 立即从远程同步价目表 */
  syncingPrices: boolean
  onSyncPrices: () => void
  /** 最近一次同步查到的参考价（按模型名）；null 表示还没同步过 */
  references: Record<string, ReferencePrice> | null
  onApplyReferences: (models: string[]) => void
  priceDialogOpen: boolean
  editingPrice: ModelPriceOverride | null
  priceForm: PriceFormState
  setPriceForm: (form: PriceFormState) => void
  onPriceDialogOpenChange: (open: boolean) => void
  onCreatePrice: () => void
  onSavePrice: () => void
  onOpenDefaults: () => void
  onEditPrice: (p: ModelPriceOverride) => void
  onDeletePrice: (p: ModelPriceOverride) => void
}

function PriceSummary({ price }: { price: ModelPriceOverride }) {
  switch (price.billing_mode || "token") {
    case "per_request": {
      const tiers = parsePriceTiers(price.pricing_tiers_json)
      return (
        <div className="space-y-0.5 text-xs tabular-nums">
          <div>${price.per_request_price ?? 0} / 次</div>
          {tiers.length > 0 ? (
            <div className="text-muted-foreground">{tiers.length} 个分层价格</div>
          ) : null}
        </div>
      )
    }
    case "image":
      return (
        <div className="text-xs tabular-nums">
          1K ${price.image_price_1k ?? 0} · 2K ${price.image_price_2k ?? 0} · 4K ${price.image_price_4k ?? 0}
        </div>
      )
    case "video":
      return (
        <div className="text-xs tabular-nums">
          480p ${price.video_price_480p ?? 0} · 720p ${price.video_price_720p ?? 0} · 1080p ${price.video_price_1080p ?? 0}
        </div>
      )
    default:
      return (
        <div className="space-y-0.5 text-xs tabular-nums">
          <div>
            输入 ${perTokenToMTok(price.input_price_per_token)} · 输出 ${perTokenToMTok(price.output_price_per_token)} / MTok
          </div>
          <div className="text-muted-foreground">
            缓存写入 ${perTokenToMTok(price.cache_creation_price_per_token)} · 读取 ${perTokenToMTok(price.cache_read_price_per_token)}
          </div>
        </div>
      )
  }
}

function SyncPricesButton({
  syncing,
  disabled,
  onSync,
  label,
}: {
  syncing: boolean
  disabled: boolean
  onSync: () => void
  label: string
}) {
  return (
    <Button variant="outline" onClick={onSync} disabled={disabled || syncing}>
      <RefreshCw className={cn("size-4", syncing && "animate-spin")} /> {syncing ? "同步中…" : label}
    </Button>
  )
}

/** 参考价能否一键采用：按 token 或按张有价格 */
function referenceApplicable(ref?: ReferencePrice) {
  return !!ref && (ref.input_price_per_token > 0 || ref.output_price_per_token > 0 || (ref.output_price_per_image ?? 0) > 0)
}

function ReferenceSummary({ reference }: { reference: ReferencePrice }) {
  const perImage = reference.output_price_per_image ?? 0
  const tokenPriced = reference.input_price_per_token > 0 || reference.output_price_per_token > 0
  return (
    <div className="space-y-0.5 text-xs tabular-nums">
      <div className="break-all text-muted-foreground">{reference.source_key}</div>
      {tokenPriced ? (
        <div>
          输入 ${perTokenToMTok(reference.input_price_per_token)} · 输出 ${perTokenToMTok(reference.output_price_per_token)} / MTok
          {reference.cache_read_price_per_token > 0
            ? ` · 缓存读 $${perTokenToMTok(reference.cache_read_price_per_token)}`
            : ""}
        </div>
      ) : perImage > 0 ? (
        <div>${perImage} / 张</div>
      ) : null}
      {reference.first_tier_up_to ? (
        <div className="text-warning">
          分档定价，这是 ≤{formatTokens(reference.first_tier_up_to)} 输入的最低档；采用后更长的请求也按这一档计
        </div>
      ) : null}
    </div>
  )
}

function UnpricedModelsCard({
  items,
  days,
  busy,
  syncing,
  references,
  onPrice,
  onSync,
  onApply,
}: {
  items: UnpricedModel[]
  days: number
  busy: boolean
  syncing: boolean
  references: Record<string, ReferencePrice> | null
  onPrice: (model: string) => void
  onSync: () => void
  onApply: (models: string[]) => void
}) {
  const pending = items.filter((m) => !m.priced_now)
  if (pending.length === 0) return null
  const applicable = pending.filter((m) => referenceApplicable(references?.[m.model])).map((m) => m.model)
  return (
    <Card className="overflow-hidden border-warning/40 bg-warning/5 shadow-none">
      <CardContent className="space-y-3 p-4 sm:p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="flex min-w-0 flex-1 items-start gap-2">
            <AlertTriangle className="mt-0.5 size-4 shrink-0 text-warning" />
            <div className="space-y-1">
              <p className="text-sm font-medium">{pending.length} 个模型找不到价格</p>
              <p className="text-xs leading-5 text-muted-foreground">
                最近 {days} 天里，这些模型的请求在计费时找不到价格，费用按 0 记、不扣额度。「从远程同步价格」会先更新网关价目表，再到 LiteLLM 全量价目表查参考价（官方渠道优先），采用后存为价格覆盖，可再编辑。新的价格只对之后的请求生效，已有的记录不会补算。
              </p>
            </div>
          </div>
          <div className="flex shrink-0 flex-wrap gap-2">
            <SyncPricesButton syncing={syncing} disabled={busy} onSync={onSync} label="从远程同步价格" />
            {applicable.length > 0 ? (
              <Button onClick={() => onApply(applicable)} disabled={busy || syncing}>
                全部采用参考价（{applicable.length}）
              </Button>
            ) : null}
          </div>
        </div>
        <div className="overflow-x-auto rounded-md border bg-background">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>模型</TableHead>
                <TableHead className="text-right">成功请求</TableHead>
                <TableHead className="text-right">输入 / 输出</TableHead>
                <TableHead>最近一次</TableHead>
                <TableHead>远程参考价</TableHead>
                <TableHead className="w-[176px] text-right">操作</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pending.map((m) => (
                <TableRow key={m.model}>
                  <TableCell className="max-w-72 break-all text-xs font-medium">{m.model}</TableCell>
                  <TableCell className="text-right text-xs tabular-nums">
                    {m.success_requests.toLocaleString("zh-CN")}
                  </TableCell>
                  <TableCell
                    className="text-right text-xs tabular-nums"
                    title={`缓存读 ${formatTokens(m.cache_read_tokens)} · 缓存写 ${formatTokens(m.cache_creation_tokens)}`}
                  >
                    {formatTokens(m.input_tokens)} / {formatTokens(m.output_tokens)}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">{dateTime(m.last_seen_at)}</TableCell>
                  <TableCell className="min-w-56">
                    {references?.[m.model] ? (
                      <ReferenceSummary reference={references[m.model]} />
                    ) : (
                      <span className="text-xs text-muted-foreground">
                        {references ? "参考表里也没有" : "同步后显示"}
                      </span>
                    )}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-1.5">
                      {referenceApplicable(references?.[m.model]) ? (
                        <Button size="sm" onClick={() => onApply([m.model])} disabled={busy || syncing}>
                          采用
                        </Button>
                      ) : null}
                      <Button size="sm" variant="outline" onClick={() => onPrice(m.model)} disabled={busy}>
                        <Plus className="size-3.5" /> 设置价格
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}

function PricingTierRow({
  tier,
  index,
  onChange,
  onRemove,
}: {
  tier: PriceTierFormState
  index: number
  onChange: (tier: PriceTierFormState) => void
  onRemove: () => void
}) {
  return (
    <div className="rounded-md border bg-background p-3">
      <div className="flex items-center justify-between gap-2">
        <Badge variant="secondary">档位 {index + 1}</Badge>
        <Button
          size="icon"
          type="button"
          variant="ghost"
          className="size-7 text-destructive hover:text-destructive"
          title="删除档位"
          onClick={onRemove}
        >
          <Trash2 className="size-3.5" />
          <span className="sr-only">删除档位</span>
        </Button>
      </div>
      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <div className="space-y-1.5">
          <Label>尺寸标签（可选）</Label>
          <Input
            placeholder="例如 2K / HD"
            value={tier.tier_label}
            onChange={(e) => onChange({ ...tier, tier_label: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label>单价（$/次）</Label>
          <Input
            type="number"
            min="0"
            step="0.000001"
            value={tier.per_request_price}
            onChange={(e) => onChange({ ...tier, per_request_price: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label>最小 Token</Label>
          <Input
            type="number"
            min="0"
            step="1"
            value={tier.min_tokens}
            onChange={(e) => onChange({ ...tier, min_tokens: e.target.value })}
          />
        </div>
        <div className="space-y-1.5">
          <Label>最大 Token（留空不限）</Label>
          <Input
            type="number"
            min="0"
            step="1"
            value={tier.max_tokens}
            onChange={(e) => onChange({ ...tier, max_tokens: e.target.value })}
          />
        </div>
      </div>
    </div>
  )
}

export function PricesPanel({
  busy,
  prices,
  unpriced,
  unpricedDays,
  onPriceUnpriced,
  syncingPrices,
  onSyncPrices,
  references,
  onApplyReferences,
  priceDialogOpen,
  editingPrice,
  priceForm,
  setPriceForm,
  onPriceDialogOpenChange,
  onCreatePrice,
  onSavePrice,
  onOpenDefaults,
  onEditPrice,
  onDeletePrice,
}: PricesPanelProps) {
  const billingMode = priceForm.billing_mode

  return (
    <div className="space-y-4">
      <UnpricedModelsCard
        items={unpriced}
        days={unpricedDays}
        busy={busy}
        syncing={syncingPrices}
        references={references}
        onPrice={onPriceUnpriced}
        onSync={onSyncPrices}
        onApply={onApplyReferences}
      />
      <Card className="overflow-hidden border-border shadow-none">
        <CardContent className="space-y-4 p-4 sm:p-5">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="text-sm leading-6 text-muted-foreground">
              已启用的覆盖价优先于系统内置价。Token 按 <strong>$/MTok</strong> 输入，图片按 $/张，视频按 $/秒。
            </p>
            <div className="flex shrink-0 flex-wrap gap-2">
              <SyncPricesButton syncing={syncingPrices} disabled={busy} onSync={onSyncPrices} label="同步远程价目" />
              <Button variant="outline" onClick={onOpenDefaults} disabled={busy}>
                查看系统默认价
              </Button>
              <Button onClick={onCreatePrice} disabled={busy}>
                <Plus className="size-4" /> 新建价格覆盖
              </Button>
            </div>
          </div>

          <div className="overflow-x-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>模型</TableHead>
                  <TableHead>计费方式</TableHead>
                  <TableHead>价格</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead className="w-[92px] text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {prices.map((price) => (
                  <TableRow key={price.id}>
                    <TableCell className="max-w-72 break-all text-xs font-medium">
                      {price.model_name}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {price.billing_mode === "per_request"
                          ? "按请求"
                          : price.billing_mode === "image"
                            ? "图片"
                            : price.billing_mode === "video"
                              ? "视频"
                              : "Token"}
                      </Badge>
                    </TableCell>
                    <TableCell className="min-w-64">
                      <PriceSummary price={price} />
                    </TableCell>
                    <TableCell>
                      <Badge variant={price.enabled ? "default" : "secondary"}>
                        {price.enabled ? "启用" : "禁用"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8"
                          title="编辑价格覆盖"
                          onClick={() => onEditPrice(price)}
                          disabled={busy}
                        >
                          <Pencil className="size-4" />
                          <span className="sr-only">编辑价格覆盖</span>
                        </Button>
                        <Button
                          size="icon"
                          variant="ghost"
                          className="size-8 text-destructive hover:text-destructive"
                          title="删除价格覆盖"
                          onClick={() => onDeletePrice(price)}
                          disabled={busy}
                        >
                          <Trash2 className="size-4" />
                          <span className="sr-only">删除价格覆盖</span>
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
                {prices.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={5} className="h-28 text-center text-muted-foreground">
                      暂无价格覆盖，计费使用系统内置默认价
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Dialog open={priceDialogOpen} onOpenChange={onPriceDialogOpenChange}>
        <DialogContent className="flex max-h-[min(90dvh,760px)] w-[calc(100vw-1.5rem)] min-w-0 flex-col gap-3 overflow-hidden sm:max-w-2xl">
          <DialogHeader className="shrink-0">
            <DialogTitle>{editingPrice ? "编辑价格覆盖" : "新建价格覆盖"}</DialogTitle>
            <DialogDescription>
              已启用的覆盖价优先于系统内置价；未覆盖的模型继续使用默认价。
            </DialogDescription>
          </DialogHeader>

          <div className="min-h-0 flex-1 space-y-4 overflow-y-auto overscroll-contain pr-0.5">
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1.5">
                <Label>模型名</Label>
                <Input
                  placeholder="claude-sonnet-4"
                  value={priceForm.model_name}
                  onChange={(e) => setPriceForm({ ...priceForm, model_name: e.target.value })}
                />
              </div>
              <div className="space-y-1.5">
                <Label>计费方式</Label>
                <Select
                  value={billingMode}
                  onValueChange={(value) =>
                    setPriceForm({
                      ...priceForm,
                      billing_mode: value as PriceFormState["billing_mode"],
                    })
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="token">Token</SelectItem>
                    <SelectItem value="per_request">按请求</SelectItem>
                    <SelectItem value="image">图片</SelectItem>
                    <SelectItem value="video">视频</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            {billingMode === "token" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="text-sm font-medium">Token 价格</div>
                <div className="grid gap-3 sm:grid-cols-2">
                  {[
                    ["input_mtok", "输入 $/MTok"],
                    ["output_mtok", "输出 $/MTok"],
                    ["cache_create_mtok", "缓存写入 $/MTok"],
                    ["cache_read_mtok", "缓存读取 $/MTok"],
                  ].map(([field, label]) => (
                    <div className="space-y-1.5" key={field}>
                      <Label>{label}</Label>
                      <Input
                        type="number"
                        min="0"
                        step="0.01"
                        value={priceForm[field as keyof Pick<PriceFormState, "input_mtok" | "output_mtok" | "cache_create_mtok" | "cache_read_mtok">]}
                        onChange={(e) => setPriceForm({ ...priceForm, [field]: e.target.value })}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {billingMode === "per_request" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="space-y-1.5">
                  <Label>默认按次价格（$）</Label>
                  <Input
                    type="number"
                    min="0"
                    step="0.000001"
                    value={priceForm.per_request_price}
                    onChange={(e) => setPriceForm({ ...priceForm, per_request_price: e.target.value })}
                  />
                </div>
                <div className="border-t pt-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <Label>分层价格</Label>
                      <p className="text-xs text-muted-foreground">尺寸标签优先匹配；留空时按 Token 区间匹配，最大 Token 留空表示不限。</p>
                    </div>
                    <Button
                      size="sm"
                      type="button"
                      variant="outline"
                      onClick={() =>
                        setPriceForm({
                          ...priceForm,
                          pricing_tiers: [
                            ...priceForm.pricing_tiers,
                            {
                              tier_label: "",
                              min_tokens: "0",
                              max_tokens: "",
                              per_request_price: "0",
                            },
                          ],
                        })
                      }
                    >
                      <Plus className="size-3.5" /> 新增档位
                    </Button>
                  </div>
                  {priceForm.pricing_tiers.length > 0 ? (
                    <div className="mt-3 space-y-2">
                      {priceForm.pricing_tiers.map((tier, index) => (
                        <PricingTierRow
                          key={index}
                          tier={tier}
                          index={index}
                          onChange={(next) => {
                            const tiers = [...priceForm.pricing_tiers]
                            tiers[index] = next
                            setPriceForm({ ...priceForm, pricing_tiers: tiers })
                          }}
                          onRemove={() =>
                            setPriceForm({
                              ...priceForm,
                              pricing_tiers: priceForm.pricing_tiers.filter((_, tierIndex) => tierIndex !== index),
                            })
                          }
                        />
                      ))}
                    </div>
                  ) : (
                    <div className="mt-3 rounded-md border border-dashed px-3 py-4 text-center text-xs text-muted-foreground">
                      未设置分层时，所有请求使用默认按次价格。
                    </div>
                  )}
                </div>
              </div>
            ) : null}

            {billingMode === "image" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="text-sm font-medium">图片价格（$/张）</div>
                <div className="grid gap-3 sm:grid-cols-3">
                  {[
                    ["image_price_1k", "1K"],
                    ["image_price_2k", "2K"],
                    ["image_price_4k", "4K"],
                  ].map(([field, label]) => (
                    <div className="space-y-1.5" key={field}>
                      <Label>{label}</Label>
                      <Input
                        type="number"
                        min="0"
                        step="0.000001"
                        value={priceForm[field as keyof Pick<PriceFormState, "image_price_1k" | "image_price_2k" | "image_price_4k">]}
                        onChange={(e) => setPriceForm({ ...priceForm, [field]: e.target.value })}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            {billingMode === "video" ? (
              <div className="space-y-3 rounded-md border bg-muted/20 p-3">
                <div className="text-sm font-medium">视频价格（$/秒）</div>
                <div className="grid gap-3 sm:grid-cols-3">
                  {[
                    ["video_price_480p", "480p"],
                    ["video_price_720p", "720p"],
                    ["video_price_1080p", "1080p"],
                  ].map(([field, label]) => (
                    <div className="space-y-1.5" key={field}>
                      <Label>{label}</Label>
                      <Input
                        type="number"
                        min="0"
                        step="0.000001"
                        value={priceForm[field as keyof Pick<PriceFormState, "video_price_480p" | "video_price_720p" | "video_price_1080p">]}
                        onChange={(e) => setPriceForm({ ...priceForm, [field]: e.target.value })}
                      />
                    </div>
                  ))}
                </div>
              </div>
            ) : null}

            <div className="flex items-center justify-between gap-3 rounded-md border bg-muted/20 p-3">
              <div>
                <Label>启用覆盖</Label>
                <p className="text-xs text-muted-foreground">关闭后保留配置，但计费回退到系统默认价。</p>
              </div>
              <Switch
                checked={priceForm.enabled}
                onCheckedChange={(enabled) => setPriceForm({ ...priceForm, enabled })}
              />
            </div>
          </div>

          <DialogFooter className="shrink-0">
            <Button variant="outline" onClick={() => onPriceDialogOpenChange(false)} disabled={busy}>
              取消
            </Button>
            <Button onClick={onSavePrice} disabled={busy}>
              保存价格覆盖
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
