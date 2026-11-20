import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type PendingReview = {
  清单编号: string
  来源行: string
  来源行ID: number
  来源通道: number
  事件类型: string
  异常原因: string
  关闭结论?: string
  待补验字段: string[]
  已确认结构范围: { fields?: string[]; schema_versions?: string[]; schema_ids?: number[] }
  可正式取数字段: string[]
  已入湖字段: string[]
  状态: string
  更新时间: string
}

export type UnknownFieldDetail = {
  明细编号: string
  来源行: string
  来源行ID: number
  来源通道: number
  事件类型: string
  字段名称: string
  字段值: unknown
  未知原因: string
  校验状态: string
  处置结论: string
  可正式取数: boolean
  当前Schema版本: string[]
  修订记录: { 时间: string; 校验状态: string; 处置结论: string; Schema版本: string[] }[]
}

export async function listPendingReviews(
  status: string = '待补验',
): Promise<{ items: PendingReview[]; total: number }> {
  const query = status ? `?status=${encodeURIComponent(status)}` : ''
  const response = await request(`/api/pipeline/webhook-ingest/pending${query}`)
  if (!response.ok) throw new Error('待处理清单读取失败')
  return await response.json()
}

export async function listUnknownFields(
  rawEventId?: number,
  status?: string,
): Promise<{ items: UnknownFieldDetail[]; total: number }> {
  const query = new URLSearchParams()
  if (rawEventId !== undefined) query.set('raw_event_id', String(rawEventId))
  if (status) query.set('status', status)
  const response = await request(`/api/pipeline/webhook-ingest/unknown-fields?${query.toString()}`)
  if (!response.ok) throw new Error('未知字段处置明细读取失败')
  return await response.json()
}

export async function revalidateRawEvent(rawEventId: number): Promise<ActionResult> {
  const response = await request('/api/pipeline/webhook-ingest/revalidate', {
    method: 'POST',
    body: JSON.stringify({ raw_event_id: rawEventId }),
  })
  return await response.json()
}

export type RawEventTrace = {
  ok: boolean
  原始接入编号: string
  事件类型: string
  通道ID: number
  原始Payload: Record<string, unknown>
  接收时间: string
  处理结论: string
  结构校验状态: string
  已确认结构范围: { fields?: string[]; schema_versions?: string[] }
  可用字段: string[]
  待补验字段: string[]
  已入湖字段: string[]
  初始结构结论: { unknown_fields?: string[]; message?: string }
  当前结构结论: { unknown_fields?: string[]; message?: string }
  结论记录: { 时间: string; 类型: string; 中断: boolean }[]
  未知字段明细: UnknownFieldDetail[]
  待处理清单: PendingReview | null
}

export async function loadRawEventTrace(rawEventId: number): Promise<RawEventTrace> {
  const response = await request(`/api/pipeline/webhook-ingest/raw/${rawEventId}`)
  if (!response.ok) throw new Error('原始接入记录读取失败')
  return await response.json()
}
