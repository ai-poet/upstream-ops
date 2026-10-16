import { useEffect, useMemo, useState } from "react"
import { useLocation, useNavigate } from "react-router-dom"
import { useTheme } from "next-themes"
import {
  Activity,
  Github,
  Home,
  LogOut,
  Menu,
  Moon,
  Network,
  RefreshCw,
  Settings,
  Sun,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import { useAuth } from "@/lib/auth-context"
import { apiFetch } from "@/lib/api"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { useAppVersion, useChannels } from "@/lib/queries"
import type { AppVersion } from "@/lib/api-types"
import { relativeTime } from "@/lib/format"
import { toast } from "sonner"

/** 顶部导航标签：value 即路由 path。 */
const NAV_TABS = [
  { value: "/", label: "主页", icon: Home },
  { value: "/gateway", label: "请求网关", icon: Network },
  { value: "/settings", label: "系统设置", icon: Settings },
] as const

/** 当前路由对应的标签；不在标签里的页面（打码平台、通知渠道）不高亮任何一个。 */
function activeNavTab(pathname: string): string {
  const hit = NAV_TABS.find((t) =>
    t.value === "/" ? pathname === "/" : pathname === t.value || pathname.startsWith(`${t.value}/`),
  )
  return hit?.value ?? ""
}

/**
 * 页面导航标签。activationMode=manual：方向键只移动焦点，回车 / 空格才跳转，
 * 避免键盘浏览时每按一次方向键就切一次页面。
 */
function NavTabs({ className }: { className?: string }) {
  const navigate = useNavigate()
  const { pathname } = useLocation()
  return (
    <Tabs value={activeNavTab(pathname)} onValueChange={(v) => navigate(v)} activationMode="manual">
      <TabsList aria-label="页面导航" className={cn("h-10 rounded-lg p-[3px]", className)}>
        {NAV_TABS.map(({ value, label, icon: Icon }) => (
          <TabsTrigger key={value} value={value} className="h-full gap-1.5 px-3.5">
            <Icon className="size-4" />
            {label}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  )
}

export function MonitorHeader() {
  const { theme, setTheme } = useTheme()
  const { username, authDisabled, logout } = useAuth()
  const refresh = useTriggerRefresh()
  const channels = useChannels()
  const appVersion = useAppVersion()
  const [mounted, setMounted] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [checkingVersion, setCheckingVersion] = useState(false)

  const appTitle = appVersion.data?.title?.trim() || "UpstreamOps"
  const version = appVersion.data?.version?.trim()
  const latestVersion = appVersion.data?.latest_version?.trim()
  const updateAvailable = Boolean(appVersion.data?.update_available && latestVersion)
  const updateURL = appVersion.data?.release_url?.trim() || appVersion.data?.repo_url?.trim()

  useEffect(() => setMounted(true), [])

  useEffect(() => {
    document.title = appTitle
  }, [appTitle])

  /**
   * 找出所有渠道中最近一次采集时间——这是"上次采集"展示的依据，
   * 让用户知道页面上的余额到底是多新的快照（区别于"我刚点了刷新"）。
   */
  const lastCollectedAt = useMemo(() => {
    const list = channels.data ?? []
    let best: string | null = null
    let bestT = -Infinity
    for (const c of list) {
      if (!c.last_balance_at) continue
      const t = new Date(c.last_balance_at).getTime()
      if (Number.isFinite(t) && t > bestT) {
        bestT = t
        best = c.last_balance_at
      }
    }
    return best
  }, [channels.data])

  function handleRefresh() {
    setSyncing(true)
    refresh()
    setTimeout(() => setSyncing(false), 800)
  }

  async function handleCheckVersion() {
    setCheckingVersion(true)
    try {
      const result = await apiFetch<AppVersion>("/version?force=1")
      appVersion.setData(result)
      if (result.update_error) {
        toast.error(result.update_error)
      } else if (result.update_available && result.latest_version) {
        toast.warning(`发现新版本 ${result.latest_version}`)
      } else {
        toast.success("当前已是最新版本")
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "检测更新失败")
    } finally {
      setCheckingVersion(false)
    }
  }

  const isDark = mounted && theme === "dark"

  return (
    <header className="sticky top-0 z-20 border-b border-border bg-background/95 backdrop-blur-sm">
      <div className="mx-auto flex h-12 max-w-[120rem] items-center justify-between gap-2 px-3 sm:h-14 sm:gap-4 sm:px-6 lg:px-8">
        {/* left: logo + title */}
        <div className="flex min-w-0 flex-1 items-center gap-2 sm:gap-2.5">
          <div className="flex size-7 shrink-0 items-center justify-center rounded-lg bg-foreground text-background sm:size-8">
            <Activity className="size-3.5 sm:size-4" strokeWidth={2.5} />
          </div>
          <div className="min-w-0">
            <h1 className="truncate text-sm font-semibold tracking-tight text-foreground sm:text-base">
              {appTitle}
            </h1>
            {version ? (
              <p className="truncate text-[10px] leading-3 text-muted-foreground sm:text-[11px]">
                <button
                  type="button"
                  className="font-medium underline-offset-2 hover:text-foreground hover:underline"
                  onClick={handleCheckVersion}
                  disabled={checkingVersion}
                  title="点击检测更新"
                >
                  {checkingVersion ? "检测中..." : `v${version}`}
                </button>
                {updateAvailable ? (
                  <a
                    href={updateURL || "https://github.com/bejix/upstream-ops"}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="ml-1.5 font-medium text-emerald-600 underline-offset-2 hover:text-emerald-700 hover:underline sm:ml-2"
                  >
                    有新版本 {latestVersion}
                  </a>
                ) : null}
              </p>
            ) : null}
          </div>
        </div>

        {/* right: actions */}
        <div className="flex shrink-0 items-center gap-1.5 sm:gap-3">
          {/* wide screens: last collected + refresh */}
          <div className="hidden items-center gap-2 sm:flex">
            <span className="hidden text-xs text-muted-foreground xl:inline">
              {"上次采集 "}
              <span className="font-medium text-foreground">{relativeTime(lastCollectedAt)}</span>
            </span>
            <Tooltip delayDuration={200}>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon"
                  onClick={handleRefresh}
                  disabled={syncing}
                  className="size-9 border-border bg-background text-foreground hover:bg-muted"
                  aria-label="刷新视图"
                >
                  <RefreshCw className={cn("size-4", syncing && "animate-spin")} />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="max-w-xs text-xs">
                <p>
                  {"上次采集 "}
                  <span className="font-medium">{relativeTime(lastCollectedAt)}</span>
                </p>
                <p className="mt-1">{"点击重新拉取最新的快照数据。"}</p>
                <p className="mt-1 text-muted-foreground">
                  {"提示：实际采集由后台定时任务执行，如需立即采集请到具体渠道点 \"同步\"。"}
                </p>
              </TooltipContent>
            </Tooltip>
          </div>

          {/* page navigation: inline on md+ (below md it moves to the second row) */}
          <NavTabs className="hidden md:inline-flex" />

          {/* mobile: refresh only (keeps one-tap access) */}
          <Button
            variant="outline"
            size="icon"
            onClick={handleRefresh}
            disabled={syncing}
            className="size-9 border-border bg-background text-foreground hover:bg-muted sm:hidden"
            aria-label="刷新视图"
          >
            <RefreshCw className={cn("size-4", syncing && "animate-spin")} />
          </Button>

          {/* mobile: secondary actions in a menu (navigation lives in the tab row below) */}
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="outline"
                size="icon"
                className="size-9 border-border bg-background text-foreground hover:bg-muted sm:hidden"
                aria-label="更多菜单"
              >
                <Menu className="size-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-52">
              <DropdownMenuLabel className="font-normal text-muted-foreground">
                上次采集{" "}
                <span className="font-medium text-foreground">{relativeTime(lastCollectedAt)}</span>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem asChild>
                <a
                  href="https://github.com/bejix/upstream-ops"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Github className="size-4" />
                  GitHub 仓库
                </a>
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={() => setTheme(isDark ? "light" : "dark")}>
                {isDark ? <Moon className="size-4" /> : <Sun className="size-4" />}
                {isDark ? "切换浅色主题" : "切换深色主题"}
              </DropdownMenuItem>
              {authDisabled ? null : (
                <>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onSelect={logout}>
                    <LogOut className="size-4" />
                    {username ? `${username} · 退出` : "退出登录"}
                  </DropdownMenuItem>
                </>
              )}
            </DropdownMenuContent>
          </DropdownMenu>

          {/* sm+: secondary actions as icon buttons */}
          <div className="hidden items-center gap-1.5 sm:flex">
            <Tooltip delayDuration={200}>
              <TooltipTrigger asChild>
                <Button
                  asChild
                  variant="outline"
                  size="icon"
                  className="size-9 border-border bg-background text-foreground hover:bg-muted"
                  aria-label="GitHub 仓库"
                >
                  <a
                    href="https://github.com/bejix/upstream-ops"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    <Github className="size-4" />
                  </a>
                </Button>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="text-xs">
                {"GitHub · bejix/upstream-ops"}
              </TooltipContent>
            </Tooltip>

            <Tooltip delayDuration={200}>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon"
                  onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
                  className="size-9 border-border bg-background text-foreground hover:bg-muted"
                  aria-label="切换主题"
                >
                  {isDark ? <Moon className="size-4" /> : <Sun className="size-4" />}
                </Button>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="text-xs">
                {isDark ? "深色模式 · 点击切换浅色" : "浅色模式 · 点击切换深色"}
              </TooltipContent>
            </Tooltip>

            {authDisabled ? null : (
              <Tooltip delayDuration={200}>
                <TooltipTrigger asChild>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={logout}
                    className="size-9 border-border bg-background text-foreground hover:bg-muted"
                    aria-label="退出登录"
                  >
                    <LogOut className="size-4" />
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="bottom" className="text-xs">
                  {username ? `${username} · 退出登录` : "退出登录"}
                </TooltipContent>
              </Tooltip>
            )}
          </div>
        </div>
      </div>

      {/* below md: page navigation as a full-width tab row */}
      <div className="mx-auto max-w-[120rem] px-3 pb-2 sm:px-6 md:hidden">
        <NavTabs className="flex w-full" />
      </div>
    </header>
  )
}
