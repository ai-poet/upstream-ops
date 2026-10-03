/**
 * 余额 / 订阅额度的"预计用完时间"估算与展示。
 *
 * 渠道的消耗速度由后端每次采集余额后用最近 2 小时的余额快照估算，折算成日消耗
 * （backend/storage/balance_burn.go），这里只负责结合最近一次余额推算时间点、汇总分组，以及格式化。
 */
import type { Channel, ChannelSubscriptionUsageWindow } from "@/lib/api-types"

const HOUR_MS = 60 * 60 * 1000
const DAY_MS = 24 * HOUR_MS
// 订阅周期刚开始时用量太少，按平均速度外推误差很大，至少过 30 分钟再估算。
const MIN_WINDOW_ELAPSED_MS = 30 * 60 * 1000

function parseTime(iso?: string | null): number | null {
  if (!iso) return null
  const t = new Date(iso).getTime()
  return Number.isFinite(t) ? t : null
}

function finiteOrNull(v?: number | null): number | null {
  return v != null && Number.isFinite(v) ? v : null
}

export interface BalanceForecast {
  /** 最近一次采集的余额；尚未采集时为 null */
  balance: number | null
  /** 按最近速度折算的日消耗；样本不足时为 null */
  dailyCost: number | null
  /** 参与估算的样本跨度（小时） */
  spanHours: number | null
  /** 预计用完的时间戳（ms）；无余额、样本不足或没有消耗时为 null */
  depletesAt: number | null
}

/** 按最近一次余额与日均消耗推算渠道的预计用完时间，以余额采集时间为起点。 */
export function channelBalanceForecast(c: Channel): BalanceForecast {
  const balance = finiteOrNull(c.last_balance)
  const dailyCost = finiteOrNull(c.balance_daily_cost)
  const sampledAt = parseTime(c.last_balance_at)
  let depletesAt: number | null = null
  if (balance != null && dailyCost != null && dailyCost > 0 && sampledAt != null) {
    depletesAt = sampledAt + (Math.max(0, balance) / dailyCost) * DAY_MS
  }
  return { balance, dailyCost, spanHours: finiteOrNull(c.balance_cost_span_hours), depletesAt }
}

export interface GroupBalanceForecast {
  /** 组内各渠道按日均消耗折算到当前时刻的余额之和 */
  balance: number
  /** 组内折算日消耗之和；没有任何渠道能估算时为 null */
  dailyCost: number | null
  /** 已采集余额但样本不足、没计入消耗的渠道数 */
  unmeasured: number
  /** 整组预计用完的时间戳（ms）：合计余额 ÷ 合计日均消耗；没有消耗时为 null */
  depletesAt: number | null
  /** 组内最早用完的渠道 */
  earliest: { channel: Channel; at: number } | null
}

/**
 * 汇总一组渠道的余额与消耗，估算整组的预计用完时间。
 *
 * 各渠道余额采集时间不同，先按各自的消耗速度折算到 now 再相加；
 * 这样只有一个渠道的分组，结果与该渠道自己的预计用完时间一致。
 * 假设组内渠道互为备份（某个用完后流量转到其它渠道），所以用合计余额 ÷ 合计消耗速度。
 */
export function groupBalanceForecast(channels: Channel[], now = Date.now()): GroupBalanceForecast {
  let balance = 0
  let dailyCost: number | null = null
  let unmeasured = 0
  let earliest: GroupBalanceForecast["earliest"] = null
  for (const c of channels) {
    const f = channelBalanceForecast(c)
    if (f.balance == null) continue
    if (f.dailyCost == null) {
      unmeasured++
      balance += Math.max(0, f.balance)
      continue
    }
    dailyCost = (dailyCost ?? 0) + f.dailyCost
    const sampledAt = parseTime(c.last_balance_at) ?? now
    const elapsedDays = Math.max(0, now - sampledAt) / DAY_MS
    balance += Math.max(0, f.balance - f.dailyCost * elapsedDays)
    if (f.depletesAt != null && (earliest == null || f.depletesAt < earliest.at)) {
      earliest = { channel: c, at: f.depletesAt }
    }
  }
  const depletesAt = dailyCost != null && dailyCost > 0 ? now + (balance / dailyCost) * DAY_MS : null
  return { balance, dailyCost, unmeasured, depletesAt, earliest }
}

export type WindowDepletion =
  | { kind: "exhausted" }
  /** 按本周期的平均速度，重置前会用完 */
  | { kind: "eta"; at: number }
  /** 按本周期的平均速度，重置前用不完 */
  | { kind: "enough" }

/**
 * 订阅额度窗口（日 / 周 / 月）按本周期已用量的平均速度估算何时用完。
 * 缺少周期起止时间、周期刚开始不到 30 分钟时无法估算，返回 null。
 */
export function subscriptionWindowDepletion(
  w: ChannelSubscriptionUsageWindow,
  now = Date.now(),
): WindowDepletion | null {
  if (w.remaining_usd <= 0 || w.remaining_percent <= 0) return { kind: "exhausted" }
  const start = parseTime(w.window_start)
  const resetsAt = parseTime(w.resets_at)
  if (start == null || resetsAt == null) return null
  const elapsed = now - start
  if (elapsed < MIN_WINDOW_ELAPSED_MS) return null
  if (w.used_usd <= 0) return { kind: "enough" }
  const at = now + (w.remaining_usd / w.used_usd) * elapsed
  return at < resetsAt ? { kind: "eta", at } : { kind: "enough" }
}

/** 时间点的简短写法：今天 / 明天 HH:mm，一年内 MM/DD HH:mm，更远只显示日期。 */
export function formatForecastAt(at: number, now = Date.now()): string {
  const d = new Date(at)
  const hm = d.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false })
  const today = new Date(now)
  today.setHours(0, 0, 0, 0)
  const dayDiff = Math.floor((at - today.getTime()) / DAY_MS)
  if (dayDiff === 0) return `今天 ${hm}`
  if (dayDiff === 1) return `明天 ${hm}`
  const md = `${String(d.getMonth() + 1).padStart(2, "0")}/${String(d.getDate()).padStart(2, "0")}`
  if (at - now < 300 * DAY_MS) return `${md} ${hm}`
  return `${d.getFullYear()}/${md}`
}

/** 剩余时长：约 N 小时 / 约 N.N 天 / 1 年以上。 */
export function formatRemaining(ms: number): string {
  if (ms < HOUR_MS) return "不到 1 小时"
  if (ms < 2 * DAY_MS) return `约 ${Math.round(ms / HOUR_MS)} 小时`
  if (ms >= 365 * DAY_MS) return "1 年以上"
  const days = ms / DAY_MS
  return `约 ${days < 10 ? days.toFixed(1) : Math.round(days)} 天`
}

/** 样本跨度：不足 2 天按小时，否则按天。 */
export function formatSpan(hours: number): string {
  if (hours < 48) return `${Math.max(1, Math.round(hours))} 小时`
  return `${Number((hours / 24).toFixed(1))} 天`
}

/** 剩余时间越短颜色越醒目：1 天内红色，3 天内黄色。 */
export function remainingTone(ms: number): string {
  if (ms < DAY_MS) return "text-danger"
  if (ms < 3 * DAY_MS) return "text-warning"
  return "text-foreground"
}
