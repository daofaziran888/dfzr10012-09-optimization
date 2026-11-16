import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/latency'
const columns = [
  "记录编号",
  "流作业名",
  "端到端延迟",
  "处理延迟",
  "网络延迟",
  "延迟阈值",
  "超标次数",
  "记录时间"
]
const actions = [
  "标记超标",
  "优化处理",
  "归档记录"
]
const statuses = [
  "采集中",
  "已超标",
  "已恢复",
  "已归档"
]

type Row = Record<string, string | number | null | undefined>

type Batch = {
  批次编号: string
  阈值版本: string
  状态: string
  完整性: string
}

type SyncResult = {
  记录编号: string
  事件记录编号: string
  批次: Batch
  窗口结果行: Record<string, unknown>
  结论: string
}

type SyncAllReport = {
  已关联: SyncResult[]
  问题: Array<{ 记录编号: string; 原因: string }>
  结论: string
}

export default function LatencyView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [report, setReport] = useState<SyncAllReport | null>(null)
  const [selected, setSelected] = useState<Row | null>(null)
  const [syncResult, setSyncResult] = useState<SyncResult | null>(null)
  const [busy, setBusy] = useState(false)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const stats = useMemo(
    () => [
      { label: "延迟记录总数", value: total },
      { label: "已超标记录", value: rows.filter((row) => row.status === '已超标').length },
      { label: "已关联窗口", value: rows.filter((row) => row.归集状态 === '已归集').length },
    ],
    [rows, total],
  )

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('延迟记录列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '延迟追踪管理列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  const loadSyncReport = useCallback(async () => {
    try {
      const response = await request(`${ENDPOINT}/sync`)
      if (!response.ok) throw new Error('消费侧关联报告读取失败')
      setReport(await response.json())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '消费侧关联报告读取失败')
    }
  }, [])

  useEffect(() => {
    void loadSyncReport()
  }, [loadSyncReport])

  async function syncEntry(row: Row) {
    setSelected(row)
    setSyncResult(null)
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/sync`, {
        method: 'POST',
        body: JSON.stringify({ values: {} }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '消费侧关联失败')
      }
      setSyncResult(payload.entry as SyncResult)
      setNotice(payload.message ?? '')
      await loadSyncReport()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '消费侧关联失败')
    } finally {
      setBusy(false)
    }
  }

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    setBusy(true)
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '延迟追踪管理动作未生效')
      }
      if (!payload.ok) {
        throw new Error(payload.message ?? '延迟追踪管理动作未生效')
      }
      setNotice(payload.message ?? '')
      await reload()
      await loadSyncReport()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '延迟追踪管理操作失败')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="page" data-module="latency">
      <header className="page-head">
        <div>
          <h2>延迟追踪管理</h2>
          <p className="page-desc">
            记录端到端延迟并按记录编号关联窗口触发结果数据集；消费侧与列表页读取同一份生效批次，
            命中多个窗口时拒绝写入，避免同一事件产生两个互相冲突的结论。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('延迟记录登记入口尚未接入审批流')}>
            登记延迟记录
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出延迟追踪清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <div className="table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              {columns.map((column) => <th key={column}>{column}</th>)}
              <th>事件记录编号</th>
              <th>关联批次</th>
              <th>关联窗口</th>
              <th>可执行动作</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={String(row.id)}
                className={selected?.id === row.id ? 'row-active' : undefined}
                onClick={() => void syncEntry(row)}
                style={{ cursor: 'pointer' }}
              >
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td className="mono">{String(row.事件记录编号 ?? '—')}</td>
                <td className="mono">
                  {row.关联批次编号
                    ? <span className="badge ok">{String(row.关联批次编号)} / {String(row.关联阈值版本)}</span>
                    : '—'}
                </td>
                <td className="mono">
                  {row.关联窗口开始 !== undefined && row.关联窗口开始 !== null
                    ? `${String(row.关联窗口开始)}–${String(row.关联窗口结束)}`
                    : '—'}
                </td>
                <td className="row-actions" onClick={(event) => event.stopPropagation()}>
                  {actions.map((action) => (
                    <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                      {action}
                    </button>
                  ))}
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td className="empty-state" colSpan={columns.length + 4}>暂无延迟追踪数据，可先登记延迟记录</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {selected && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h3 className="panel-title">消费侧关联 · {String(selected.记录编号)}</h3>
              <p className="panel-sub">
                按记录编号到当前生效批次里查窗口归属；批次不一致时拒绝写入，不做跨版本合并。
              </p>
            </div>
            <div className="panel-actions">
              <button className="btn" type="button" disabled={busy} onClick={() => void syncEntry(selected)}>
                重新关联
              </button>
            </div>
          </div>
          {syncResult ? (
            <>
              <p className="hint"><strong>{syncResult.结论}</strong></p>
              <table className="detail-table">
                <thead>
                  <tr><th>记录编号</th><th>批次编号</th><th>阈值版本</th><th>窗口开始</th><th>窗口结束</th><th>来源</th></tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="mono">{syncResult.事件记录编号}</td>
                    <td className="mono">{syncResult.批次.批次编号}</td>
                    <td className="mono">{syncResult.批次.阈值版本}</td>
                    <td>{String(syncResult.窗口结果行.窗口开始)}</td>
                    <td>{String(syncResult.窗口结果行.窗口结束)}</td>
                    <td><span className={`badge ${syncResult.窗口结果行.来源 === '沿用快照' ? 'warn' : 'ok'}`}>{String(syncResult.窗口结果行.来源)}</span></td>
                  </tr>
                </tbody>
              </table>
            </>
          ) : (
            <p className="hint">还没有关联结果，点击「重新关联」按当前生效批次校验。</p>
          )}
        </section>
      )}

      <section className="panel">
        <div className="panel-head">
          <div>
            <h3 className="panel-title">消费侧批量校验</h3>
            <p className="panel-sub">{report?.结论 ?? '正在读取消费侧关联状态…'}</p>
          </div>
          <div className="panel-actions">
            <button className="btn" type="button" onClick={() => void loadSyncReport()}>刷新校验</button>
          </div>
        </div>
        {report && report.问题.length > 0 && (
          <div className="checks">
            {report.问题.map((item) => (
              <div className="check-line" key={item.记录编号}>
                <span className="badge bad">冲突</span>
                <strong>{item.记录编号}</strong>
                <span className="hint">{item.原因}</span>
              </div>
            ))}
          </div>
        )}
        {report && report.问题.length === 0 && (
          <div className="checks">
            {report.已关联.map((item) => (
              <div className="check-line" key={item.记录编号}>
                <span className="badge ok">已归集</span>
                <strong>{item.事件记录编号}</strong>
                <span className="hint">
                  {item.批次.批次编号}（{item.批次.阈值版本}）窗口 {String(item.窗口结果行.窗口开始)}–{String(item.窗口结果行.窗口结束)}
                </span>
              </div>
            ))}
          </div>
        )}
      </section>

      <footer className="page-foot">
        <span>共 {total} 条延迟追踪记录</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
