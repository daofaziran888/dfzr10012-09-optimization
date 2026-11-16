import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/window'
const DATASET_ENDPOINT = `${ENDPOINT}/datasets`
const columns = [
  "窗口编号",
  "数据流名称",
  "窗口类型",
  "窗口长度",
  "滑动步长",
  "延迟容忍",
  "触发策略",
  "窗口状态"
]
const actions = [
  "启用窗口",
  "暂停窗口",
  "关闭窗口"
]
const statuses = [
  "草稿",
  "生效中",
  "已暂停",
  "已关闭"
]

type Row = Record<string, string | number | null | undefined>

type DatasetRow = {
  窗口开始: number
  窗口结束: number
  窗口类型: string
  触发策略: string
  阈值版本: string
  窗口长度: number
  滑动步长: number
  延迟容忍: number
  记录数: number
  聚合值: number
  事件记录编号: string[]
  来源: string
}

type Batch = {
  批次编号: string
  窗口编号: string
  阈值版本: string
  状态: string
  完整性: string
  数据集来源: string
  窗口数: number
  沿用快照窗口数: number
  记录数: number
  窗口长度: number
  滑动步长: number
  延迟容忍: number
  重算说明?: string
}

type Preview = {
  旧参数: Record<string, string | number>
  新参数: Record<string, string | number>
  受影响事件: string[]
  受影响窗口: Array<Record<string, unknown>>
  需重算窗口数: number
  超期丢弃记录: string[]
  结论: string
}

type CheckItem = { 检查项: string; 通过: boolean; 说明: string }

const thresholdFields = [
  { key: "窗口类型", label: "窗口类型", options: ["滚动窗口", "滑动窗口"] },
  { key: "窗口长度", label: "窗口长度（秒）" },
  { key: "滑动步长", label: "滑动步长（秒）" },
  { key: "延迟容忍", label: "延迟容忍（秒）" },
  { key: "触发策略", label: "触发策略", options: ["水位线触发", "处理时间触发", "计数触发"] },
] as const

const blankThreshold: Record<string, string> = {
  窗口类型: "",
  窗口长度: "",
  滑动步长: "",
  延迟容忍: "",
  触发策略: "",
}

export default function WindowView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [selected, setSelected] = useState<Row | null>(null)
  const [thresholdDraft, setThresholdDraft] = useState<Record<string, string>>(blankThreshold)
  const [preview, setPreview] = useState<Preview | null>(null)
  const [batch, setBatch] = useState<Batch | null>(null)
  const [datasetRows, setDatasetRows] = useState<DatasetRow[]>([])
  const [checks, setChecks] = useState<CheckItem[]>([])
  const [batches, setBatches] = useState<Batch[]>([])
  const [history, setHistory] = useState<Array<Record<string, unknown>>>([])
  const [busy, setBusy] = useState(false)
  const [upgradeLog, setUpgradeLog] = useState<string>('')
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const token = useMemo(() => Math.random().toString(36).slice(2, 10), [])

  const loadDataset = useCallback(async (windowId: string) => {
    const response = await request(`${DATASET_ENDPOINT}?window_id=${encodeURIComponent(windowId)}`)
    if (!response.ok) throw new Error('窗口触发结果数据集批次读取失败')
    const payload = await response.json()
    setBatches(payload.items ?? [])
    const active = (payload.items ?? []).find((item: Batch) => item.状态 === '生效')
    if (!active) {
      setBatch(null)
      setDatasetRows([])
      setChecks([])
      return
    }
    setBatch(active)
    const [detail, verify] = await Promise.all([
      request(`${DATASET_ENDPOINT}/${encodeURIComponent(active.批次编号)}`),
      request(`${DATASET_ENDPOINT}/${encodeURIComponent(active.批次编号)}/verify`),
    ])
    if (detail.ok) {
      const body = await detail.json()
      setDatasetRows(body.rows ?? [])
    }
    if (verify.ok) {
      const body = await verify.json()
      setChecks(body.检查项 ?? [])
    }
  }, [])

  const loadThreshold = useCallback(async (row: Row) => {
    const response = await request(`${ENDPOINT}/${row.id}/threshold`)
    if (!response.ok) throw new Error('窗口阈值读取失败')
    const payload = await response.json()
    const current = payload.当前参数 ?? {}
    setThresholdDraft({
      窗口类型: String(current.窗口类型 ?? ''),
      窗口长度: String(current.窗口长度 ?? ''),
      滑动步长: String(current.滑动步长 ?? ''),
      延迟容忍: String(current.延迟容忍 ?? ''),
      触发策略: String(current.触发策略 ?? ''),
    })
    setHistory(payload.阈值版本历史 ?? [])
  }, [])

  const selectWindow = useCallback(
    async (row: Row) => {
      setErrorMessage('')
      setPreview(null)
      setSelected(row)
      try {
        await Promise.all([loadDataset(String(row.窗口编号)), loadThreshold(row)])
      } catch (error) {
        setErrorMessage(error instanceof Error ? error.message : '窗口触发结果数据集读取失败')
      }
    },
    [loadDataset, loadThreshold],
  )

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('窗口定义列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
      const current = selected
      if (current) {
        const fresh = (payload.items ?? []).find((row: Row) => row.id === current.id)
        if (fresh) setSelected(fresh)
      }
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '窗口计算配置列表读取失败')
    }
  }, [filters, selected])

  useEffect(() => {
    void reload()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('窗口计算配置动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '窗口计算配置操作失败')
    }
  }

  function updateDraft(key: string, value: string) {
    setThresholdDraft((prev) => ({ ...prev, [key]: value }))
    setPreview(null)
  }

  function submittedValues(): Record<string, string | number> {
    const values: Record<string, string | number> = {}
    for (const field of thresholdFields) {
      const raw = thresholdDraft[field.key]
      if (raw === undefined || raw === '') continue
      values[field.key] = field.key === "窗口类型" || field.key === "触发策略" ? raw : Number(raw)
    }
    return values
  }

  async function runPreview() {
    if (!selected) return
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${selected.id}/threshold/preview`, {
        method: 'POST',
        body: JSON.stringify({ values: submittedValues() }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '阈值预演失败')
      }
      setPreview(payload.entry as Preview)
      setNotice(payload.message ?? '')
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '阈值预演失败')
    } finally {
      setBusy(false)
    }
  }

  async function applyThreshold() {
    if (!selected) return
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${selected.id}/threshold/actions`, {
        method: 'POST',
        body: JSON.stringify({
          values: { ...submittedValues(), 幂等键: `ui-${token}-${selected.id}` },
        }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '阈值调整未生效')
      }
      if (!payload.ok) {
        throw new Error(payload.message ?? '阈值调整未生效')
      }
      setNotice(payload.message ?? '')
      await reload()
      await selectWindow({ ...selected, 阈值版本: payload.entry?.batch?.阈值版本 ?? selected.阈值版本 })
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '阈值调整未生效')
    } finally {
      setBusy(false)
    }
  }

  async function runUpgrade(stage: string) {
    setBusy(true)
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/upgrade/actions`, {
        method: 'POST',
        body: JSON.stringify({ values: { stage } }),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '升级阶段执行失败')
      }
      setUpgradeLog(`${stage}：${payload.message}`)
      await reload()
      if (selected) await selectWindow(selected)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '升级阶段执行失败')
    } finally {
      setBusy(false)
    }
  }

  async function repairVersion() {
    if (!selected) return
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const response = await request(`${ENDPOINT}/${selected.id}/repair`, { method: 'POST' })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(typeof payload.detail === 'string' ? payload.detail : '异常版本改正失败')
      }
      if (!payload.ok) {
        throw new Error(payload.message ?? '异常版本改正后仍有校验不通过项')
      }
      setNotice(payload.message ?? '')
      await reload()
      await selectWindow(selected)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '异常版本改正失败')
    } finally {
      setBusy(false)
    }
  }

  const stats = useMemo(
    () => [
      { label: "窗口定义总数", value: total },
      { label: "生效中窗口", value: rows.filter((row) => row.status === '生效中').length },
      { label: "生效数据集", value: rows.filter((row) => row.数据集状态 === '生效').length },
    ],
    [rows, total],
  )

  const activeVersion = selected?.生效批次阈值版本 ?? selected?.阈值版本 ?? '—'
  const failedChecks = checks.filter((item) => !item.通过)

  return (
    <section className="page" data-module="window">
      <header className="page-head">
        <div>
          <h2>窗口计算配置管理</h2>
          <p className="page-desc">
            维护窗口类型、窗口长度、滑动步长、延迟容忍与触发策略；调整阈值后只重算受影响窗口，
            校验通过再写入窗口触发结果数据集，并在结果里保留窗口边界与所用阈值版本。
          </p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('窗口定义登记入口尚未接入审批流')}>
            登记窗口定义
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出窗口计算配置清单
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
              <th>阈值版本</th>
              <th>生效批次</th>
              <th>可执行动作</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={String(row.id)}
                className={selected?.id === row.id ? 'row-active' : undefined}
                onClick={() => void selectWindow(row)}
                style={{ cursor: 'pointer' }}
              >
                {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
                <td className="mono">{String(row.阈值版本 ?? '—')}</td>
                <td className="mono">
                  {row.数据集状态 === '生效'
                    ? <span className="badge ok">{String(row.生效批次编号)} / {String(row.生效批次阈值版本)}</span>
                    : <span className="badge bad">{String(row.数据集说明 ?? '缺失')}</span>}
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
                <td className="empty-state" colSpan={columns.length + 3}>暂无窗口计算配置数据，可先登记窗口定义</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {selected && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h3 className="panel-title">阈值调整与受影响事件校验 · {String(selected.窗口编号)}</h3>
              <p className="panel-sub">
                当前阈值版本 <span className="mono">{String(selected.阈值版本 ?? '—')}</span>
                ，生效批次 <span className="mono">{String(activeVersion)}</span>
                。调整后只重算归属变化的窗口，其余窗口沿用历史快照。
              </p>
            </div>
            <div className="panel-actions">
              <button className="btn" type="button" disabled={busy} onClick={() => void runPreview()}>
                校验受影响事件
              </button>
              <button className="btn primary" type="button" disabled={busy} onClick={() => void applyThreshold()}>
                调整阈值并写入数据集
              </button>
            </div>
          </div>

          <div className="grid-form">
            {thresholdFields.map((field) => (
              <label key={field.key}>
                <span>{field.label}</span>
                {'options' in field && field.options ? (
                  <select value={thresholdDraft[field.key]} onChange={(event) => updateDraft(field.key, event.target.value)}>
                    {field.options.map((option) => <option key={option} value={option}>{option}</option>)}
                  </select>
                ) : (
                  <input
                    type="number"
                    min={0}
                    value={thresholdDraft[field.key]}
                    onChange={(event) => updateDraft(field.key, event.target.value)}
                  />
                )}
              </label>
            ))}
          </div>

          {preview && (
            <div>
              <p className="hint">
                <strong>{preview.结论}</strong>
                {preview.受影响事件.length > 0 && (
                  <>；受影响事件：<span className="mono">{preview.受影响事件.join('、')}</span></>
                )}
                {preview.超期丢弃记录.length > 0 && (
                  <>；超期丢弃：<span className="mono">{preview.超期丢弃记录.join('、')}</span></>
                )}
              </p>
              <table className="detail-table">
                <thead>
                  <tr><th>窗口开始</th><th>窗口结束</th><th>变化类型</th><th>新增记录</th><th>移除记录</th></tr>
                </thead>
                <tbody>
                  {preview.受影响窗口.map((item, index) => (
                    <tr key={index}>
                      <td>{String(item.窗口开始)}</td>
                      <td>{String(item.窗口结束)}</td>
                      <td>{String(item.变化类型)}</td>
                      <td className="mono">{((item.新增记录 as string[]) ?? []).join('、') || '—'}</td>
                      <td className="mono">{((item.移除记录 as string[]) ?? []).join('、') || '—'}</td>
                    </tr>
                  ))}
                  {preview.受影响窗口.length === 0 && (
                    <tr><td colSpan={5} className="empty-state">阈值调整不改变任何窗口归属</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {selected && (
        <section className="panel">
          <div className="panel-head">
            <div>
              <h3 className="panel-title">窗口触发结果数据集 · {String(selected.窗口编号)}</h3>
              <p className="panel-sub">
                {batch
                  ? <>批次 <span className="mono">{batch.批次编号}</span>（阈值版本 <span className="mono">{batch.阈值版本}</span>，
                    {batch.状态}，{batch.完整性}） · 窗口长度 {batch.窗口长度}s / 步长 {batch.滑动步长}s / 延迟容忍 {batch.延迟容忍}s
                    · 重算 {batch.窗口数 - batch.沿用快照窗口数} 个窗口，沿用快照 {batch.沿用快照窗口数} 个</>
                  : '当前窗口没有生效的窗口触发结果数据集'}
              </p>
            </div>
            <div className="panel-actions">
              {batch && (
                <button
                  className="btn"
                  type="button"
                  onClick={() => window.open(`${DATASET_ENDPOINT}/${batch.批次编号}/verify`, '_blank')}
                >
                  查看完整校验
                </button>
              )}
              <button className="btn ghost" type="button" disabled={busy} onClick={() => void repairVersion()}>
                改正异常版本
              </button>
            </div>
          </div>

          <div className="table-wrap">
            <table className="detail-table">
              <thead>
                <tr>
                  <th>窗口开始</th><th>窗口结束</th><th>窗口类型</th><th>触发策略</th>
                  <th>窗口长度</th><th>滑动步长</th><th>延迟容忍</th><th>所用阈值版本</th>
                  <th>记录数</th><th>聚合值</th><th>事件记录编号</th><th>来源</th>
                </tr>
              </thead>
              <tbody>
                {datasetRows.map((row, index) => (
                  <tr key={index}>
                    <td>{row.窗口开始}</td>
                    <td>{row.窗口结束}</td>
                    <td>{row.窗口类型}</td>
                    <td>{row.触发策略}</td>
                    <td>{row.窗口长度}s</td>
                    <td>{row.滑动步长}s</td>
                    <td>{row.延迟容忍}s</td>
                    <td className="mono">{row.阈值版本}</td>
                    <td>{row.记录数}</td>
                    <td>{row.聚合值}</td>
                    <td className="mono">{row.事件记录编号.join('、')}</td>
                    <td>
                      <span className={`badge ${row.来源 === '沿用快照' ? 'warn' : 'ok'}`}>{row.来源}</span>
                    </td>
                  </tr>
                ))}
                {datasetRows.length === 0 && (
                  <tr><td colSpan={12} className="empty-state">暂无窗口触发结果行</td></tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="checks">
            {checks.map((item) => (
              <div className="check-line" key={item.检查项}>
                <span className={`badge ${item.通过 ? 'ok' : 'bad'}`}>{item.通过 ? '通过' : '不通过'}</span>
                <strong>{item.检查项}</strong>
                <span className="hint">{item.说明}</span>
              </div>
            ))}
          </div>

          <h4 className="panel-title" style={{ marginTop: 14 }}>数据集批次与阈值版本历史</h4>
          <div className="table-wrap">
            <table className="detail-table">
              <thead>
                <tr><th>批次编号</th><th>阈值版本</th><th>状态</th><th>完整性</th><th>来源</th><th>窗口数</th><th>记录数</th><th>说明</th></tr>
              </thead>
              <tbody>
                {batches.map((item) => (
                  <tr key={item.批次编号}>
                    <td className="mono">{item.批次编号}</td>
                    <td className="mono">{item.阈值版本}</td>
                    <td><span className={`badge ${item.状态 === '生效' ? 'ok' : item.状态 === '失败' ? 'bad' : ''}`}>{item.状态}</span></td>
                    <td>{item.完整性}</td>
                    <td>{item.数据集来源}</td>
                    <td>{item.窗口数}</td>
                    <td>{item.记录数}</td>
                    <td>{item.重算说明 ?? '—'}</td>
                  </tr>
                ))}
                {batches.length === 0 && (
                  <tr><td colSpan={8} className="empty-state">暂无数据集批次</td></tr>
                )}
              </tbody>
            </table>
          </div>
          {history.length > 0 && (
            <p className="hint">
              阈值版本历史：{history.map((item) => `${String(item.阈值版本)}（延迟容忍 ${String(item.延迟容忍)}s，${String(item.状态)}）`).join('；')}
            </p>
          )}
        </section>
      )}

      <section className="panel">
        <div className="panel-head">
          <div>
            <h3 className="panel-title">数据集升级流程</h3>
            <p className="panel-sub">覆盖旧数据库、新环境、部署回滚与依赖兼容；每一步都可以重复执行而不会产生第二份生效数据集。</p>
          </div>
          <div className="panel-actions">
            {['旧数据库', '新环境', '部署回滚', '依赖兼容'].map((stage) => (
              <button key={stage} className="btn" type="button" disabled={busy} onClick={() => void runUpgrade(stage)}>
                {stage}
              </button>
            ))}
          </div>
        </div>
        <p className="hint">
          {upgradeLog
            ? <><strong>{upgradeLog}</strong></>
            : '旧数据库批次缺少阈值版本与窗口边界时，先执行「旧数据库」补齐字段，再执行「依赖兼容」确认检查点与消费侧都能解析到唯一版本。'}
        </p>
        {failedChecks.length > 0 && (
          <p className="hint">
            当前生效批次有 {failedChecks.length} 项校验不通过：{failedChecks.map((item) => item.检查项).join('、')}
          </p>
        )}
      </section>

      <footer className="page-foot">
        <span>共 {total} 条窗口计算配置记录</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
