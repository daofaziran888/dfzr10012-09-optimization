import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/checkpoint'
const columns = [
  "记录编号",
  "流作业名",
  "检查点ID",
  "状态大小",
  "保存路径",
  "创建时间",
  "恢复耗时",
  "记录状态"
]
const actions = [
  "恢复状态",
  "清理记录",
  "归档记录"
]
const statuses = [
  "已创建",
  "已恢复",
  "已过期",
  "已清理"
]

type Row = Record<string, string | number | null | undefined>

type Batch = {
  批次编号: string
  窗口编号: string
  阈值版本: string
  状态: string
  完整性: string
  数据集来源: string
}

type RestoreView = {
  记录编号: string
  窗口编号: string
  检查点阈值版本: string
  批次: Batch
  窗口结果行: Array<Record<string, unknown>>
  版本冲突说明: string | null
  结论: string
}

export default function CheckpointView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [selected, setSelected] = useState<Row | null>(null)
  const [restore, setRestore] = useState<RestoreView | null>(null)
  const [restoreError, setRestoreError] = useState('')
  const [busy, setBusy] = useState(false)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const stats = useMemo(
    () => [
      { label: "检查点记录总数", value: total },
      { label: "已恢复记录", value: rows.filter((row) => row.status === '已恢复').length },
      { label: "已过期记录", value: rows.filter((row) => row.status === '已过期').length },
    ],
    [rows, total],
  )

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('检查点记录列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '检查点恢复列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  const inspectRestore = useCallback(async (row: Row) => {
    setSelected(row)
    setRestore(null)
    setRestoreError('')
    if (!row.窗口编号) {
      setRestoreError('该检查点没有关联窗口编号，恢复时会明确拒绝而不是猜测窗口归属')
      return
    }
    try {
      const response = await request(`${ENDPOINT}/${row.id}/restore`)
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '检查点恢复预检失败')
      }
      setRestore(payload as RestoreView)
    } catch (error) {
      setRestoreError(error instanceof Error ? error.message : '检查点恢复预检失败')
    }
  }, [])

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
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '检查点恢复动作未生效')
      }
      if (!payload.ok) {
        throw new Error(payload.message ?? '检查点恢复动作未生效')
      }
      setNotice(payload.message ?? '')
      await reload()
      await inspectRestore(row)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '检查点恢复操作失败')
    } finally {
      setBusy(false)
    }
  }

  const conflict = Boolean(restore?.版本冲突说明)

  return (
    <section className="page" data-module="checkpoint">
      <header className="page-head">
        <div>
          <h2>检查点恢复管理</h2>
          <p className="page-desc">
            维护检查点的状态快照与恢复路径；恢复时只读取检查点声明的阈值版本，
            同一迟到事件不会在旧阈值与新阈值下同时进入两个窗口。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('检查点记录登记入口尚未接入审批流')}>
            登记检查点记录
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出检查点恢复清单
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
              <th>窗口编号</th>
              <th>阈值版本</th>
              <th>恢复批次</th>
              <th>可执行动作</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={String(row.id)}
                className={selected?.id === row.id ? 'row-active' : undefined}
                onClick={() => void inspectRestore(row)}
                style={{ cursor: 'pointer' }}
              >
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td className="mono">{String(row.窗口编号 ?? '—')}</td>
                <td className="mono">{String(row.阈值版本 ?? '—')}</td>
                <td className="mono">
                  {row.恢复批次编号
                    ? <span className="badge ok">{String(row.恢复批次编号)} / {String(row.恢复阈值版本)}</span>
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
                <td className="empty-state" colSpan={columns.length + 4}>暂无检查点恢复数据，可先登记检查点记录</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {selected && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h3 className="panel-title">恢复口径预检 · {String(selected.记录编号)}</h3>
              <p className="panel-sub">
                检查点声明阈值版本 <span className="mono">{String(selected.阈值版本 ?? '未标注')}</span>；
                恢复只认这一个版本对应的唯一完整批次。
              </p>
            </div>
            <div className="panel-actions">
              <button
                className="btn"
                type="button"
                disabled={busy}
                onClick={() => void inspectRestore(selected)}
              >
                重新预检
              </button>
            </div>
          </div>

          {restoreError && <p className="error-text">{restoreError}</p>}

          {restore && (
            <>
              <p className="hint"><strong>{restore.结论}</strong></p>
              {conflict && (
                <p className="hint">
                  <span className="badge warn">版本并存</span> {restore.版本冲突说明}
                </p>
              )}
              <table className="detail-table">
                <thead>
                  <tr>
                    <th>批次编号</th><th>阈值版本</th><th>状态</th><th>完整性</th><th>数据集来源</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td className="mono">{restore.批次.批次编号}</td>
                    <td className="mono">{restore.批次.阈值版本}</td>
                    <td><span className="badge ok">{restore.批次.状态}</span></td>
                    <td>{restore.批次.完整性}</td>
                    <td>{restore.批次.数据集来源}</td>
                  </tr>
                </tbody>
              </table>
              <table className="detail-table">
                <thead>
                  <tr><th>窗口开始</th><th>窗口结束</th><th>阈值版本</th><th>记录数</th><th>事件记录编号</th></tr>
                </thead>
                <tbody>
                  {restore.窗口结果行.map((row, index) => (
                    <tr key={index}>
                      <td>{String(row.窗口开始)}</td>
                      <td>{String(row.窗口结束)}</td>
                      <td className="mono">{String(row.阈值版本)}</td>
                      <td>{String(row.记录数)}</td>
                      <td className="mono">{((row.事件记录编号 as string[]) ?? []).join('、')}</td>
                    </tr>
                  ))}
                  {restore.窗口结果行.length === 0 && (
                    <tr><td colSpan={5} className="empty-state">该批次没有窗口结果行</td></tr>
                  )}
                </tbody>
              </table>
            </>
          )}
        </section>
      )}

      <footer className="page-foot">
        <span>共 {total} 条检查点恢复记录</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
