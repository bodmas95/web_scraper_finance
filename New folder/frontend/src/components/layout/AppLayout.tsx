import type { ReactNode } from 'react'
import { Separator } from '@/components/ui/separator'

export function AppLayout({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen bg-background">
      <header
        className="shadow-sm"
        style={{
          background: 'linear-gradient(135deg, hsl(267 60% 40%) 0%, hsl(267 55% 52%) 100%)',
        }}
      >
        <div className="mx-auto flex h-14 max-w-7xl items-center px-6">
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-bold tracking-tight text-white">
              Nx Intelligence
            </h1>
            <Separator
              orientation="vertical"
              className="mx-1 h-5 bg-white/30"
            />
            <span className="text-sm font-medium text-white/80">
              BREF Financial Automation
            </span>
          </div>
          <div className="ml-auto">
            <span className="text-[10px] font-semibold uppercase tracking-[0.2em] text-white/50">
              Natixis
            </span>
          </div>
        </div>
        {/* Accent line */}
        <div
          className="h-[2px]"
          style={{
            background: 'linear-gradient(90deg, hsl(267 80% 70%), hsl(290 60% 65%), hsl(267 80% 70%))',
          }}
        />
      </header>
      <main className="mx-auto px-6 py-6" style={{ maxWidth: '1600px' }}>{children}</main>
    </div>
  )
}
