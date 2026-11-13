"use client"

import { useCallback, useEffect, useRef, useState, type FormEvent } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import { useAuth } from "@/lib/auth-context"
import { useAppVersion } from "@/lib/queries"
import { apiFetch, type ApiError } from "@/lib/api"

interface CaptchaResponse {
  enabled: boolean
  captcha_id: string
  image_base64: string
}

export function LoginPage() {
  const { login } = useAuth()
  const appVersion = useAppVersion()
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [captcha, setCaptcha] = useState<CaptchaResponse | null>(null)
  const [captchaEnabled, setCaptchaEnabled] = useState<boolean | null>(null)
  const [captchaCode, setCaptchaCode] = useState("")
  const [captchaLoading, setCaptchaLoading] = useState(true)
  const [captchaError, setCaptchaError] = useState<string | null>(null)
  const [captchaImageFailed, setCaptchaImageFailed] = useState(false)
  const captchaRequest = useRef(0)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const appTitle = appVersion.data?.title?.trim() || "UpstreamOps"
  const captchaImageUnavailable = captchaEnabled === true &&
    !captchaLoading &&
    (!captcha?.image_base64 || captchaImageFailed)
  const captchaHint = captchaError ??
    (captchaImageUnavailable ? "验证码图片不可用，可点击重试；也可以直接提交" : null)

  useEffect(() => {
    document.title = appTitle
  }, [appTitle])

  const refreshCaptcha = useCallback(async () => {
    const requestID = ++captchaRequest.current
    setCaptchaLoading(true)
    setCaptchaError(null)
    setCaptchaImageFailed(false)
    setCaptcha(null)
    setCaptchaCode("")
    try {
      const result = await apiFetch<CaptchaResponse>("/auth/captcha", {
        skipAuthErrorHandler: true,
      })
      if (requestID !== captchaRequest.current) return
      setCaptchaEnabled(result.enabled)
      setCaptcha(result.enabled ? result : null)
    } catch {
      if (requestID !== captchaRequest.current) return
      setCaptchaError("验证码加载失败，可点击重试；也可以直接提交")
    } finally {
      if (requestID === captchaRequest.current) {
        setCaptchaLoading(false)
      }
    }
  }, [])

  useEffect(() => {
    void refreshCaptcha()
    return () => {
      captchaRequest.current += 1
    }
  }, [refreshCaptcha])

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await login(
        username.trim(),
        password,
        captcha?.captcha_id ?? "",
        captchaCode.trim(),
      )
    } catch (err) {
      const e = err as ApiError
      const code = (e.body as { code?: string } | undefined)?.code
      if (code === "captcha_unavailable") {
        setError("验证码服务暂时不可用，请刷新后重试")
        await refreshCaptcha()
      } else if (code === "captcha_invalid" || code === "captcha_required") {
        setError(code === "captcha_required" ? "请输入验证码" : "验证码错误或已过期，请重新输入")
        await refreshCaptcha()
      } else if (e.status === 401) {
        setError("账号或密码错误")
        // 校验账号密码前验证码已被一次性消费，失败后必须换一张。
        if (captchaEnabled === true) await refreshCaptcha()
      } else {
        setError(e.message || "登录失败")
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4">
      <Card className="w-full max-w-sm py-6">
        <CardHeader className="space-y-1.5">
          <CardTitle className="text-2xl">{appTitle}</CardTitle>
          <CardDescription>登录后台，监控渠道余额和倍率。</CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="username">账号</Label>
              <Input
                id="username"
                name="username"
                autoComplete="username"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                disabled={submitting}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">密码</Label>
              <Input
                id="password"
                name="password"
                type="password"
                autoComplete="current-password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                disabled={submitting}
              />
            </div>
            {captchaEnabled === true ? (
              <div className="space-y-2">
                <Label htmlFor="captcha">验证码</Label>
                <div className="flex gap-2">
                  <Input
                    id="captcha"
                    name="captcha_code"
                    autoComplete="off"
                    autoCapitalize="characters"
                    autoCorrect="off"
                    spellCheck={false}
                    maxLength={4}
                    placeholder="4 位验证码"
                    value={captchaCode}
                    onChange={(e) => setCaptchaCode(e.target.value)}
                    required
                    disabled={submitting || captchaLoading || !captcha?.image_base64 || captchaImageFailed}
                  />
                  <button
                    type="button"
                    className="flex h-9 w-28 shrink-0 items-center justify-center overflow-hidden rounded-md border bg-background text-xs text-muted-foreground transition-colors hover:bg-accent disabled:opacity-50"
                    onClick={() => void refreshCaptcha()}
                    disabled={captchaLoading || submitting}
                    aria-label="刷新图形验证码"
                    title="点击刷新验证码"
                  >
                    {captcha?.image_base64 && !captchaImageFailed ? (
                      <img
                        src={captcha.image_base64}
                        alt="图形验证码，点击刷新"
                        className="h-full w-full object-cover"
                        onError={() => setCaptchaImageFailed(true)}
                      />
                    ) : (
                      captchaLoading ? "加载中…" : "加载失败，点此重试"
                    )}
                  </button>
                </div>
                {captchaHint ? (
                  <p className="text-xs text-destructive" role="alert">{captchaHint}</p>
                ) : null}
              </div>
            ) : null}
            {error ? (
              <p className="text-sm text-destructive" role="alert">
                {error}
              </p>
            ) : null}
            <Button
              type="submit"
              className="w-full"
              disabled={submitting}
            >
              {submitting ? "登录中…" : "登录"}
            </Button>
          </form>
        </CardContent>
      </Card>
    </div>
  )
}
