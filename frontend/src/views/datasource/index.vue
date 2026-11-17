<template>
  <section class="page" data-module="datasource">
    <header class="page-head">
      <div>
        <h2>采集数据源池管理</h2>
        <p class="page-desc">维护采集数据源的协议配置、优先级排序与接入管理；同一数据源组只保留一个可采集主源，故障切换后的字段可逐条复核。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记数据源条目</button>
        <button class="btn" type="button" @click="reviewSelected">复核所选条目</button>
        <button class="btn" type="button" @click="goFailover">前往批量故障切</button>
        <button class="btn" type="button" @click="exportRows">导出采集数据源池清单</button>
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

    <h3>数据源组主备唯一性</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in groupColumns" :key="column">{{ column }}</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in groupRows" :key="String(row['数据源组'])">
          <td v-for="column in groupColumns" :key="column">{{ row[column] ?? '—' }}</td>
        </tr>
      </tbody>
    </table>

    <h3>数据源条目（含主备角色、健康采样、配额与复核状态）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>选择</th>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td><input v-model="selectedIds" type="checkbox" :value="Number(row.id)" /></td>
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
          <td :colspan="columns.length + 2" class="empty-state">暂无采集数据源池数据，可先登记数据源条目</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集数据源池记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const router = useRouter()
const ENDPOINT = '/api/datasource'
const columns = ['条目编号', '数据源名称', '源类型', '接入协议', '认证方式', '优先级', '健康采样时间', '健康新鲜度分', '数据源组', '主备角色', '当前日配额', '原主保留位置', '协议映射校验', '最近采集', '条目状态', '复核状态']
const groupColumns = ['数据源组', '条目数', '主源条目编号', '主源数量', '主源唯一', '可恢复原主源', '保留位置']
const actions = ['接入源', '暂停源', '移除源']
const stats = [{ label: '数据源条目总数', value: 0 }, { label: '已接入源', value: 0 }, { label: '已暂停源', value: 0 }]

const rows = ref<Row[]>([])
const groupRows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const selectedIds = ref<number[]>([])
const filterFields = ['条目编号', '数据源名称', '数据源组']

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '数据源条目登记入口尚未接入审批流'
}

function goFailover() {
  void router.push('/failover')
}

async function reviewSelected() {
  errorMessage.value = ''
  if (!selectedIds.value.length) {
    errorMessage.value = '请先勾选需要复核的数据源条目'
    return
  }
  try {
    const response = await request(`${ENDPOINT}/review`, {
      method: 'POST',
      body: JSON.stringify({ entry_ids: selectedIds.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message ?? '复核未生效')
    selectedIds.value = []
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '字段复核失败'
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
      throw new Error('采集数据源池动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集数据源池操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const [listResponse, groupResponse] = await Promise.all([
      request(`${ENDPOINT}?${query}`),
      request(`${ENDPOINT}/groups/overview`),
    ])
    if (!listResponse.ok) throw new Error('数据源条目列表读取失败')
    if (!groupResponse.ok) throw new Error('数据源组主备总览读取失败')
    const payload = await listResponse.json()
    const groupPayload = await groupResponse.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    groupRows.value = groupPayload.items ?? []
    stats[0].value = total.value
    stats[1].value = rows.value.filter((row) => row['条目状态'] === '已接入').length
    stats[2].value = rows.value.filter((row) => row['条目状态'] === '已暂停').length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集数据源池列表读取失败'
  }
}

onMounted(reload)
</script>
