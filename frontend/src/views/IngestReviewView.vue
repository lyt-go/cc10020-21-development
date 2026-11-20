<template>
  <section class="page" data-testid="ingest-review">
    <header class="page-head">
      <div>
        <h2>接入补验工作台</h2>
        <p class="page-desc">
          结构校验失败或中断的原始接入数据在这里留存；已确认结构范围内的字段照常进入正式分区，
          未知字段单列处置明细。补齐结构定义后，补验只更正同一来源行。
        </p>
      </div>
      <div class="page-actions">
        <select v-model="statusFilter" data-testid="pending-status" @change="loadPending">
          <option value="待补验">仅看待补验</option>
          <option value="待更正">仅看待更正</option>
          <option value="已关闭">仅看已关闭</option>
          <option value="">全部</option>
        </select>
        <button class="btn" data-testid="reload" type="button" @click="reload">刷新清单</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">待处理来源行</span>
        <strong class="stat-value" data-testid="pending-total">{{ pending.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未知字段明细</span>
        <strong class="stat-value" data-testid="unknown-total">{{ unknownTotal }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">当前查看</span>
        <strong class="stat-value">{{ selected ? selected.原始接入编号 : '未选择' }}</strong>
      </article>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>清单编号</th>
          <th>来源行</th>
          <th>事件类型</th>
          <th>状态</th>
          <th>已确认结构范围</th>
          <th>可正式取数字段</th>
          <th>异常原因</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in pending"
          :key="item.清单编号"
          :data-testid="`pending-${item.清单编号}`"
          :data-status="item.状态"
          :class="{ selected: selectedId === item.来源行ID }"
        >
          <td>{{ item.清单编号 }}</td>
          <td>{{ item.来源行 }}</td>
          <td>{{ item.事件类型 }}</td>
          <td>{{ item.状态 }}</td>
          <td>{{ (item.已确认结构范围?.fields ?? []).join('、') || '—' }}</td>
          <td>{{ item.可正式取数字段.join('、') || '—' }}</td>
          <td class="reason">{{ item.异常原因 }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`trace-${item.来源行ID}`" @click="loadTrace(item.来源行ID)">
              查看原文与结论
            </button>
            <button class="link" type="button" :data-testid="`revalidate-${item.来源行ID}`" @click="revalidate(item.来源行ID)">
              重新补验
            </button>
          </td>
        </tr>
        <tr v-if="!pending.length">
          <td colspan="8" class="empty-state">当前筛选条件下没有待处理记录</td>
        </tr>
      </tbody>
    </table>

    <section v-if="selected" class="trace-panel" data-testid="trace-panel">
      <header class="trace-head">
        <h3>{{ selected.原始接入编号 }} · {{ selected.事件类型 }}</h3>
        <span class="trace-meta">结构校验：{{ selected.结构校验状态 }} · 处理结论：{{ selected.处理结论 }}</span>
      </header>

      <dl class="trace-grid">
        <div>
          <dt>最初收到的原文</dt>
          <dd><code data-testid="raw-payload">{{ rawPayloadText }}</code></dd>
        </div>
        <div>
          <dt>已确认结构范围</dt>
          <dd>
            {{ (selected.已确认结构范围?.fields ?? []).join('、') || '暂无' }}
            <span v-if="selected.已确认结构范围?.schema_versions?.length">
              （结构版本 {{ selected.已确认结构范围.schema_versions.join('、') }}）
            </span>
          </dd>
        </div>
        <div>
          <dt>可正式取数 / 已入湖</dt>
          <dd>{{ selected.可用字段.join('、') || '无' }} / {{ selected.已入湖字段.join('、') || '无' }}</dd>
        </div>
        <div>
          <dt>待补验字段</dt>
          <dd>{{ selected.待补验字段.join('、') || '无' }}</dd>
        </div>
        <div>
          <dt>初始结构结论</dt>
          <dd data-testid="initial-conclusion">{{ selected.初始结构结论?.message || '—' }}</dd>
        </div>
        <div>
          <dt>当前结构结论</dt>
          <dd data-testid="current-conclusion">{{ selected.当前结构结论?.message || '—' }}</dd>
        </div>
      </dl>

      <h4>结论修订</h4>
      <ul class="timeline">
        <li v-for="(item, index) in selected.结论记录" :key="index">
          {{ item.类型 }} · {{ item.时间 }}
          <span v-if="item.中断" class="error-text">（校验中断）</span>
        </li>
      </ul>

      <h4>未知字段处置明细</h4>
      <table class="data-table">
        <thead>
          <tr><th>明细编号</th><th>来源行</th><th>字段名称</th><th>字段值</th><th>校验状态</th><th>可正式取数</th><th>处置结论</th></tr>
        </thead>
        <tbody>
          <tr v-for="detail in selected.未知字段明细" :key="detail.明细编号" :data-testid="`unknown-${detail.明细编号}`">
            <td>{{ detail.明细编号 }}</td>
            <td>{{ detail.来源行 }}</td>
            <td>{{ detail.字段名称 }}</td>
            <td>{{ formatValue(detail.字段值) }}</td>
            <td>{{ detail.校验状态 }}</td>
            <td>{{ detail.可正式取数 ? '是' : '否' }}</td>
            <td class="reason">{{ detail.处置结论 }}</td>
          </tr>
          <tr v-if="!selected.未知字段明细.length">
            <td colspan="7" class="empty-state">这笔数据没有未知字段</td>
          </tr>
        </tbody>
      </table>

      <p v-if="selected.待处理清单" class="pending-note" data-testid="pending-note">
        待处理清单 {{ selected.待处理清单.清单编号 }}：{{ selected.待处理清单.状态 }}。
        {{ selected.待处理清单.异常原因 }}
        <template v-if="selected.待处理清单.关闭结论">（{{ selected.待处理清单.关闭结论 }}）</template>
      </p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  listPendingReviews,
  listUnknownFields,
  loadRawEventTrace,
  revalidateRawEvent,
  type PendingReview,
  type RawEventTrace,
} from '@/api/backend-service'

const pending = ref<PendingReview[]>([])
const unknownTotal = ref(0)
const selected = ref<RawEventTrace | null>(null)
const statusFilter = ref('待补验')
const errorMessage = ref('')

const selectedId = computed(() => (selected.value ? Number(selected.value.原始接入编号.split('-').pop()) : 0))
const rawPayloadText = computed(() =>
  selected.value ? JSON.stringify(selected.value.原始Payload, null, 2) : '')

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '—'
  return typeof value === 'object' ? JSON.stringify(value) : String(value)
}

async function loadPending() {
  errorMessage.value = ''
  try {
    const payload = await listPendingReviews(statusFilter.value)
    pending.value = payload.items
    const details = await listUnknownFields()
    unknownTotal.value = details.total
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '待处理清单读取失败'
  }
}

async function loadTrace(rawEventId: number) {
  errorMessage.value = ''
  try {
    selected.value = await loadRawEventTrace(rawEventId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '原始接入记录读取失败'
  }
}

async function revalidate(rawEventId: number) {
  errorMessage.value = ''
  try {
    const result = await revalidateRawEvent(rawEventId)
    if (!result.ok) throw new Error(result.message)
    await loadPending()
    await loadTrace(rawEventId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '补验失败'
  }
}

async function reload() {
  await loadPending()
  if (selected.value) {
    await loadTrace(Number(selectedId.value))
  }
}

onMounted(loadPending)
</script>

<style scoped>
.selected { background: #eff6ff; }
.reason { max-width: 260px; }
.trace-panel { margin-top: 20px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
.trace-head { display: flex; justify-content: space-between; align-items: baseline; }
.trace-head h3 { margin: 0 0 4px; }
.trace-meta { color: var(--muted); font-size: 12px; }
.trace-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 12px 0; }
.trace-grid dt { color: var(--muted); font-size: 12px; }
.trace-grid dd { margin: 4px 0 0; font-size: 13px; }
.trace-grid code { display: block; white-space: pre-wrap; background: #f8fafc; padding: 8px; border-radius: 6px; font-size: 12px; }
.timeline { margin: 0 0 12px; padding-left: 18px; font-size: 13px; }
.pending-note { font-size: 13px; color: var(--muted); }
</style>
