<template>
  <section class="page" data-module="proxy">
    <header class="page-head">
      <div>
        <h2>采集代理路由管理</h2>
        <p class="page-desc">代理路由读取数据源批量故障切结果，每个数据源组只更新唯一主源，并保留可恢复的原主源位置与协议映射校验项。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记代理配置</button>
        <button class="btn" type="button" @click="applyAll">应用全部待处理队列</button>
        <button class="btn" type="button" @click="recompute">重算故障切队列</button>
        <button class="btn" type="button" @click="exportRows">导出采集代理路由清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <h3>代理路由清单（唯一主源 + 可恢复原主源位置）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in routeColumns" :key="column">{{ column }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in routeRows" :key="String(row['数据源组'])">
          <td v-for="column in routeColumns" :key="column">{{ row[column] ?? '—' }}</td>
        </tr>
        <tr v-if="!routeRows.length">
          <td :colspan="routeColumns.length" class="empty-state">暂无代理路由清单</td>
        </tr>
      </tbody>
    </table>

    <h3>数据源批量故障切待处理队列</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in queueColumns" :key="column">{{ column }}</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in queueRows" :key="String(row['队列id'])">
          <td v-for="column in queueColumns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-if="row['队列状态'] === '待处理'"
              class="link"
              type="button"
              @click="applyOne(row['队列id'])"
            >
              应用
            </button>
          </td>
        </tr>
        <tr v-if="!queueRows.length">
          <td :colspan="queueColumns.length + 1" class="empty-state">待处理队列已清空</td>
        </tr>
      </tbody>
    </table>

    <h3>代理配置</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无采集代理路由数据，可先登记代理配置</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集代理路由记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/proxy'
const columns = ['配置编号', '代理地址', '代理协议', '区域标签', '数据源组', '主源条目编号', '备用恢复位置', '协议映射校验', '可用比例', '切换阈值', '响应延迟', '配置状态']
const routeColumns = ['数据源组', '代理配置编号', '代理地址', '唯一主源条目编号', '主源接入协议', '原主源条目编号', '可恢复原主源位置', '备用恢复位置', '恢复清单', '协议映射校验项', '路由版本']
const queueColumns = ['队列id', '数据源组', '来源批次', '原主源条目编号', '新主源条目编号', '恢复清单', '协议映射校验项', '队列状态']
const actions = ['启用代理', '降级代理', '关闭代理']

const rows = ref<Row[]>([])
const routeRows = ref<Row[]>([])
const queueRows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ['配置编号', '代理地址', '数据源组']

const stats = computed(() => [
  { label: '代理配置总数', value: rows.value.length },
  { label: '待处理队列项', value: queueRows.value.filter((row) => row['队列状态'] === '待处理').length },
  { label: '已生成路由组', value: routeRows.value.filter((row) => Number(row['路由版本']) > 0).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '代理配置登记入口尚未接入审批流'
}

async function post(path: string) {
  const response = await request(path, { method: 'POST' })
  const payload = await response.json().catch(() => ({}))
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.message ?? payload.detail ?? '操作未生效')
  }
  return payload
}

async function applyOne(queueId: number | string | null) {
  errorMessage.value = ''
  try {
    const payload = await post(`${ENDPOINT}/failover/queue/${queueId}/apply`)
    window.alert(payload.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '队列应用失败'
  }
}

async function applyAll() {
  errorMessage.value = ''
  try {
    const payload = await post(`${ENDPOINT}/failover/apply-all`)
    window.alert(`已应用 ${payload['应用条数']} 条队列项`)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量应用失败'
  }
}

async function recompute() {
  errorMessage.value = ''
  try {
    const payload = await post(`${ENDPOINT}/failover/recompute`)
    window.alert(payload.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重算失败'
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    if (!response.ok) {
      throw new Error('采集代理路由动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集代理路由操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const [listResponse, routeResponse, queueResponse] = await Promise.all([
      request(`${ENDPOINT}?${query}`),
      request(`${ENDPOINT}/failover/routes`),
      request(`${ENDPOINT}/failover/queue`),
    ])
    if (!listResponse.ok) throw new Error('代理配置列表读取失败')
    if (!routeResponse.ok) throw new Error('代理路由清单读取失败')
    if (!queueResponse.ok) throw new Error('故障切待处理队列读取失败')
    const payload = await listResponse.json()
    const routePayload = await routeResponse.json()
    const queuePayload = await queueResponse.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    routeRows.value = routePayload.items ?? []
    queueRows.value = queuePayload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集代理路由列表读取失败'
  }
}

onMounted(reload)
</script>
