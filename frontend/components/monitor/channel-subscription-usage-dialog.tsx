"use client"

import { useEffect, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { apiFetch } from "@/lib/api"
import { channelTypeLabel, dateTime, decimal } from "@/lib/format"
import { formatForecastAt, subscriptionWindowDepletion, type WindowDepletion } from "@/lib/forecast"
import { cn } from "@/lib/utils"
import type {
  Channel,
  ChannelSubscriptionUsage,
  ChannelSubscriptionUsageInfo,
  ChannelSubscriptionUsageWindow,
} from "@/lib/api-types"

interface ChannelSubscriptionUsageDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  channel: Channel | null
}

function normalizeUsageWindow(value: unknown): ChannelSubscriptionUsageWindow | null {
  if (!value || typeof value !== "object") return null
  const raw = value as Record<string, unknown>
  const limit = Number(raw.limit_usd)
  if (!Number.isFinite(limit) || limit <= 0) return null
  const used = Number(raw.used_usd) || 0
  const usedPct = Math.max(0, Math.min(100, Number(raw.used_percent) || 0))
  const remainingPct = Math.max(0, Math.min(100, Number(raw.remaining_percent) || (100 - usedPct)))
  return {
    limit_usd: limit,
    used_usd: used,
    remaining_usd: Math.max(0, Number(raw.remaining_usd) || 0),
    remaining_percent: remainingPct,
    used_percent: usedPct,
    window_start: raw.window_start == null ? null : String(raw.window_start),
    resets_at: raw.resets_at == null ? null : String(raw.resets_at),
    resets_in_seconds: Number(raw.resets_in_seconds) || 0,
  }
}

function normalizeSubscriptionUsageInfo(value: unknown): ChannelSubscriptionUsageInfo {
  const wrapped = value && typeof value === "object" && "data" in value
    ? (value as { data?: unknown }).data
    : value
  const root = wrapped && typeof wrapped === "object" ? wrapped as Record<string, unknown> : {}
  const items = Array.isArray(root.items) ? root.items : []
  return {
    items: items.map((item) => {
      const raw = item && typeof item === "object" ? item as Record<string, unknown> : {}
      return {
        id: Number(raw.id) || 0,
        group_id: Number(raw.group_id) || 0,
        group_name: String(raw.group_name ?? ""),
        status: String(raw.status ?? ""),
        starts_at: raw.starts_at == null ? null : String(raw.starts_at),
        expires_at: raw.expires_at == null ? null : String(raw.expires_at),
        expires_in_days: Number(raw.expires_in_days) || 0,
        daily: normalizeUsageWindow(raw.daily),
        weekly: normalizeUsageWindow(raw.weekly),
        monthly: normalizeUsageWindow(raw.monthly),
      }
    }).filter((item) => item.id > 0),
  }
}

function usageWindowItems(item: ChannelSubscriptionUsage) {
  return [
    { key: "daily", label: "今日", value: item.daily },
    { key: "weekly", label: "本周", value: item.weekly },
    { key: "monthly", label: "本月", value: item.monthly },
  ].filter((entry): entry is { key: string; label: string; value: ChannelSubscriptionUsageWindow } => !!entry.value && entry.value.limit_usd > 0)
}

function subscriptionStatusLabel(status: string) {
  const map: Record<string, string> = {
    active: "生效中",
    expired: "已过期",
    revoked: "已撤销",
    disabled: "已停用",
  }
  return map[status] ?? (status || "未知")
}

function lowestWindow(items: ChannelSubscriptionUsage[], key: "daily" | "weekly" | "monthly") {
  let current: ChannelSubscriptionUsageWindow | null = null
  for (const item of items) {
    const value = item[key]
    if (!value || value.limit_usd <= 0) continue
    if (!current || value.remaining_percent < current.remaining_percent) current = value
  }
  return current
}

export function ChannelSubscriptionUsageSummary({ channel }: { channel: Channel }) {
  const supported = channel.type === "sub2api" && !!channel.subscription_enabled
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [usage, setUsage] = useState<ChannelSubscriptionUsageInfo | null>(null)

  useEffect(() => {
    if (!supported) return
    let cancelled = false
    setLoading(true)
    setError(null)
    apiFetch<unknown>(`/channels/${channel.id}/subscription-usage`)
      .then((data) => {
        if (!cancelled) setUsage(normalizeSubscriptionUsageInfo(data))
      })
      .catch((e) => {
        const err = e as Error
        if (!cancelled) setError(err.message || "加载订阅用量失败")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [channel.id, supported])

  if (!supported) return null

  const items = usage?.items ?? []
  const stats = [
    { label: "日剩余", value: lowestWindow(items, "daily") },
    { label: "周剩余", value: lowestWindow(items, "weekly") },
    { label: "月剩余", value: lowestWindow(items, "monthly") },
  ].filter((item): item is { label: string; value: ChannelSubscriptionUsageWindow } => !!item.value)

  return (
    <div className="mt-3 rounded-lg border border-border bg-muted/20 px-3 py-2">
      <div className="flex min-h-7 items-center justify-between gap-2">
        <div className="min-w-0">
          <p className="text-[11px] font-medium leading-none text-foreground">订阅用量</p>
          {!loading && items.length ? (
            <p className="mt-1 text-[10px] leading-none text-muted-foreground">{items.length} 个订阅</p>
          ) : null}
        </div>
        {loading ? (
          <Loader2 className="size-3 animate-spin text-muted-foreground" />
        ) : stats.length ? (
          <div className="flex shrink-0 items-center gap-1.5">
            {stats.map((item) => (
              <span key={item.label} className="rounded-md bg-background px-1.5 py-1 text-[10px] font-medium text-foreground">
                {item.label.replace("剩余", "")} {decimal(item.value.remaining_percent, 0)}%
              </span>
            ))}
          </div>
        ) : null}
      </div>
      {error ? (
        <p className="mt-1 truncate text-[11px] text-danger" title={error}>{error}</p>
      ) : loading && !usage ? (
        <p className="mt-1 text-[11px] text-muted-foreground">加载中…</p>
      ) : !items.length ? (
        <p className="mt-1 text-[11px] text-muted-foreground">暂无生效订阅</p>
      ) : !stats.length ? (
        <p className="mt-1 text-[11px] text-muted-foreground">当前订阅不限用量</p>
      ) : null}
    </div>
  )
}

/** windowDepletionText 订阅额度窗口的预计用完说明；无法估算时返回 null。 */
function windowDepletionText(depletion: WindowDepletion | null, now: number) {
  if (!depletion) return null
  if (depletion.kind === "exhausted") return "额度已用完"
  if (depletion.kind === "enough") return "按当前速度，重置前够用"
  return `按当前速度，预计 ${formatForecastAt(depletion.at, now)} 用完`
}

function windowDepletionTone(depletion: WindowDepletion | null) {
  if (depletion?.kind === "exhausted") return "text-danger"
  if (depletion?.kind === "eta") return "text-warning"
  return "text-foreground"
}

function WindowDepletionLine({ value, now }: { value: ChannelSubscriptionUsageWindow; now: number }) {
  const depletion = subscriptionWindowDepletion(value, now)
  const text = windowDepletionText(depletion, now)
  if (!text) return null
  return <p className={cn(depletion?.kind !== "enough" && windowDepletionTone(depletion))}>{text}</p>
}

/**
 * earliestWindowDepletion 在所有订阅的日 / 周 / 月额度里找最先用完（或已用完）的一个，
 * 用于卡片上的一句话摘要；都够用时返回 enough，都无法估算时返回 null。
 */
function earliestWindowDepletion(items: ChannelSubscriptionUsage[], now: number) {
  let best: { label: string; depletion: WindowDepletion } | null = null
  const rank = (d: WindowDepletion) => (d.kind === "exhausted" ? -Infinity : d.kind === "eta" ? d.at : Infinity)
  for (const item of items) {
    for (const { label, value } of usageWindowItems(item)) {
      const depletion = subscriptionWindowDepletion(value, now)
      if (!depletion) continue
      if (!best || rank(depletion) < rank(best.depletion)) best = { label, depletion }
    }
  }
  return best
}

/** ChannelSubscriptionUsageMetricTiles 渠道卡片里的订阅用量格，只在 Sub2API 开启订阅时显示，占满一行。 */
export function ChannelSubscriptionUsageMetricTiles({ channel }: { channel: Channel }) {
  const enabled = channel.type === "sub2api" && !!channel.subscription_enabled
  const [loading, setLoading] = useState(false)
  const [usage, setUsage] = useState<ChannelSubscriptionUsageInfo | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)

  useEffect(() => {
    if (!enabled) return
    let cancelled = false
    setLoading(true)
    apiFetch<unknown>(`/channels/${channel.id}/subscription-usage`)
      .then((data) => {
        if (!cancelled) setUsage(normalizeSubscriptionUsageInfo(data))
      })
      .catch(() => {
        if (!cancelled) setUsage(null)
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [channel.id, enabled])

  if (!enabled) return null

  const now = Date.now()
  const items = usage?.items ?? []
  const stats = [
    { label: "日", value: lowestWindow(items, "daily") },
    { label: "周", value: lowestWindow(items, "weekly") },
    { label: "月", value: lowestWindow(items, "monthly") },
  ].filter((item): item is { label: string; value: ChannelSubscriptionUsageWindow } => !!item.value)
  const lowest = stats.reduce<ChannelSubscriptionUsageWindow | null>((current, item) => {
    if (!current || item.value.remaining_percent < current.remaining_percent) return item.value
    return current
  }, null)
  const subscriptionText = loading && !usage ? "加载中" : `${items.length} 个`
  const usageText = loading && !usage ? "加载中" : lowest ? `${decimal(lowest.remaining_percent, 0)}%` : "不限"
  const hasSubscriptions = items.length > 0
  const earliest = earliestWindowDepletion(items, now)

  return (
    <>
      <div className="col-span-2 flex h-16 min-w-0 flex-col justify-between rounded-md border border-border bg-muted/20 px-2.5 py-2 sm:col-span-3">
        <div className="flex min-w-0 items-center justify-between gap-2">
          <span className="shrink-0 text-[10px] leading-none text-muted-foreground">订阅 / {subscriptionText}</span>
          {earliest ? (
            <span className={cn("truncate text-[10px] leading-none", windowDepletionTone(earliest.depletion))}>
              {earliest.depletion.kind === "eta"
                ? `${earliest.label}额度预计 ${formatForecastAt(earliest.depletion.at, now)} 用完`
                : earliest.depletion.kind === "exhausted"
                  ? `${earliest.label}额度已用完`
                  : "按当前速度各额度够用"}
            </span>
          ) : null}
        </div>
        <div className="grid grid-cols-4 gap-1.5">
          {stats.length ? (
            <>
              {stats.map((item) => {
                const depletion = subscriptionWindowDepletion(item.value, now)
                const depletionText = windowDepletionText(depletion, now)
                return (
                  <Tooltip key={item.label} delayDuration={150}>
                    <TooltipTrigger asChild>
                      <span
                        className={cn(
                          "truncate rounded bg-background px-1.5 py-1 text-center text-[10px] font-medium",
                          windowDepletionTone(depletion),
                        )}
                      >
                        {item.label} {decimal(item.value.remaining_percent, 0)}%
                      </span>
                    </TooltipTrigger>
                    <TooltipContent side="top" className="text-xs">
                      <p>已用 ${decimal(item.value.used_usd, 2)} / 限制 ${decimal(item.value.limit_usd, 2)}</p>
                      {depletionText ? <p className="mt-0.5 opacity-80">{depletionText}</p> : null}
                    </TooltipContent>
                  </Tooltip>
                )
              })}
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="col-start-4 h-auto min-w-0 px-1 text-[10px]"
                onClick={() => setDialogOpen(true)}
              >
                用量
              </Button>
            </>
          ) : (
            <>
              <span className="col-span-3 truncate text-[10px] text-muted-foreground">
                {!loading && !hasSubscriptions ? "未订阅" : `订阅用量 ${usageText}`}
              </span>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-auto min-w-0 px-1 text-[10px]"
                onClick={() => setDialogOpen(true)}
              >
                用量
              </Button>
            </>
          )}
        </div>
      </div>
      <ChannelSubscriptionUsageDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        channel={channel}
      />
    </>
  )
}

export function ChannelSubscriptionUsageDialog({
  open,
  onOpenChange,
  channel,
}: ChannelSubscriptionUsageDialogProps) {
  const channelID = channel?.id ?? null
  const [loading, setLoading] = useState(false)
  const [reloadTick, setReloadTick] = useState(0)
  const [error, setError] = useState<string | null>(null)
  const [usage, setUsage] = useState<ChannelSubscriptionUsageInfo | null>(null)

  useEffect(() => {
    if (!open || channelID == null) return
    let cancelled = false
    setLoading(true)
    setError(null)
    apiFetch<unknown>(`/channels/${channelID}/subscription-usage`)
      .then((data) => {
        if (!cancelled) setUsage(normalizeSubscriptionUsageInfo(data))
      })
      .catch((e) => {
        const err = e as Error
        if (!cancelled) {
          setUsage(null)
          setError(err.message || "加载订阅用量失败")
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, channelID, reloadTick])

  const items = usage?.items ?? []
  const now = Date.now()
  const description = channel
    ? `${channel.name} · ${channelTypeLabel(channel.type)}`
    : "查看 Sub2API 当前订阅的日、周、月用量。"

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>订阅用量</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/20 px-3 py-4 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" />
            加载订阅用量中…
          </div>
        ) : error ? (
          <div className="space-y-3">
            <div className="rounded-lg border border-border bg-muted/20 px-3 py-3 text-sm text-destructive">
              {error}
            </div>
            <Button type="button" variant="outline" size="sm" onClick={() => setReloadTick((v) => v + 1)}>
              <RefreshCw className="mr-1 size-3.5" />
              重新加载
            </Button>
          </div>
        ) : !items.length ? (
          <div className="rounded-lg border border-border bg-muted/20 px-3 py-4 text-sm text-muted-foreground">
            暂无生效中的订阅。
          </div>
        ) : (
          <div className="space-y-3">
            {items.map((item) => {
              const windows = usageWindowItems(item)
              return (
                <div key={item.id} className="rounded-lg border border-border bg-muted/20 px-3 py-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="break-words text-sm font-medium">
                        {item.group_name || `分组 ${item.group_id || item.id}`}
                      </p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        {subscriptionStatusLabel(item.status)}
                        {item.expires_at ? ` · 到期 ${dateTime(item.expires_at)}` : ""}
                      </p>
                    </div>
                    {item.expires_in_days > 0 ? (
                      <span className="shrink-0 rounded-md bg-background px-2 py-1 text-xs text-muted-foreground">
                        剩 {item.expires_in_days} 天
                      </span>
                    ) : null}
                  </div>

                  {windows.length ? (
                    <div className="mt-3 grid gap-3 md:grid-cols-3">
                      {windows.map(({ key, label, value }) => (
                        <div key={key} className="rounded-md bg-background px-3 py-2.5">
                          <div className="mb-1.5 flex items-center justify-between gap-2 text-xs">
                            <span className="font-medium text-foreground">{label}</span>
                            <span className="text-muted-foreground">
                              剩 {decimal(value.remaining_percent, 1)}%
                            </span>
                          </div>
                          <Progress value={value.used_percent} className="h-1.5" />
                          <div className="mt-2 space-y-1 text-[11px] text-muted-foreground">
                            <p>
                              已用 ${decimal(value.used_usd, 2)} / 限制 ${decimal(value.limit_usd, 2)}
                            </p>
                            <p>剩余 ${decimal(value.remaining_usd, 2)}</p>
                            {value.resets_at ? <p>重置 {dateTime(value.resets_at)}</p> : null}
                            <WindowDepletionLine value={value} now={now} />
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="mt-3 text-xs text-muted-foreground">该订阅暂无额度限制。</p>
                  )}
                </div>
              )
            })}
          </div>
        )}

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={loading}
            onClick={() => setReloadTick((v) => v + 1)}
            className={cn(loading && "opacity-80")}
          >
            <RefreshCw className={cn("mr-1 size-3.5", loading && "animate-spin")} />
            刷新
          </Button>
          <Button type="button" onClick={() => onOpenChange(false)}>
            关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
