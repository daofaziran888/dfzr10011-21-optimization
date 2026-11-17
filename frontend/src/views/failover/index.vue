<template>
  <section class="page" data-module="failover">
    <header class="page-head">
      <div>
        <h2>数据源批量故障切</h2>
        <p class="page-desc">
          采集数据源池 → 代理模块待处理队列 → 采集代理路由的批量故障切换。同组唯一主源、
          逐条结果可复核、整组重试只继续未切换条目，重复执行幂等不产生第二个主源。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="recompute">其他入口重算队列</button>
        <button class="btn primary" type="button" @click="reloadAll">刷新全部数据</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <div class="filter-bar">
      <label class="filter-item">
        <span>批量条目 id（逗号分隔，一次处理多条）</span>
        <input v-model="entryIdsText" placeholder="例如：1,4,7" />
      </label>
      <label class="filter-item">
        <span>同优先级冲突策略</span>
        <select v-model="policy">
          <option v-for="p in policies" :key="p" :value="p">{{ p }}</option>
        </select>
      </label>
      <button class="btn primary" type="button" @click="submitBatch">提交批量故障切换</button>
    </div>
    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <h3>数据源组主备唯一性（采集数据源池 → 代理路由差异）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in groupColumns" :key="column">{{ column }}</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in groupRows" :key="String(row['数据源组'])">
          <td v-for="column in groupColumns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="restore(String(row['数据源组'] ?? ''))">原主源恢复</button>
          </td>
        </tr>
      </tbody>
    </table>

    <h3>故障切换批次与逐条结果（含切换前后快照，可复核）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in batchColumns" :key="column">{{ column }}</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in batches" :key="String(batch['批次号'])">
          <td v-for="column in batchColumns" :key="column">{{ batch[column] ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="toggleBatch(String(batch['批次号'] ?? ''))">
              {{ expanded === batch['批次号'] ? '收起逐条' : '查看逐条' }}
            </button>
            <button class="link" type="button" @click="retry(String(batch['批次号'] ?? ''))">整组重试</button>
          </td>
        </tr>
      </tbody>
    </table>

    <div v-if="expandedItems.length" class="data-table-wrap">
      <h4>批次 {{ expanded }} 逐条结果</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in itemColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in expandedItems" :key="String(item['序号'])">
            <td v-for="column in itemColumns" :key="column">
              <template v-if="column === '处理结果'">
                <span :class="resultClass(item['处理结果'])">{{ item['处理结果'] }}</span>
              </template>
              <template v-else-if="column === '冲突裁决'">
                {{ (item['冲突裁决'] as unknown as Row | undefined)?.['冲突说明'] || '—' }}
              </template>
              <template v-else>{{ item[column] ?? '—' }}</template>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <h3>代理模块 · 数据源批量故障切待处理队列</h3>
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
              @click="applyQueue(Number(row['队列id']))"
            >
              应用到代理路由
            </button>
          </td>
        </tr>
        <tr v-if="!queueRows.length">
          <td :colspan="queueColumns.length + 1" class="empty-state">待处理队列已清空</td>
        </tr>
      </tbody>
    </table>
    <div class="page-actions">
      <button class="btn primary" type="button" @click="applyAll">全部应用到代理路由</button>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/failover'

const policies = ref<string[]>([])
const policy = ref('原数据源亲和优先')
const entryIdsText = ref('')
const errorMessage = ref('')
const groupRows = ref<Row[]>([])
const batches = ref<Row[]>([])
const queueRows = ref<Row[]>([])
const expanded = ref('')

const groupColumns = ['数据源组', '条目数', '主源条目编号', '主源数量', '主源唯一', '可恢复原主源', '保留位置']
const batchColumns = ['批次号', '冲突处理策略', '批次状态', '提交条目数', '成功数', '失败数', '跳过数']
const itemColumns = ['序号', '条目编号', '数据源组', '处理结果', '原主源条目编号', '新主源条目编号', '恢复清单', '配额重算', '复核状态', '冲突裁决', '结果说明']
const queueColumns = ['队列id', '数据源组', '来源批次', '原主源条目编号', '新主源条目编号', '恢复清单', '协议映射校验项', '队列状态']

const expandedItems = computed<Row[]>(() => {
  const batch = batches.value.find((item) => item['批次号'] === expanded.value)
  return (batch?.['逐条结果'] as Row[] | undefined) ?? []
})

const stats = computed(() => [
  { label: '数据源组', value: groupRows.value.length },
  { label: '唯一主源组数', value: groupRows.value.filter((row) => row['主源唯一']).length },
  { label: '故障切批次', value: batches.value.length },
  { label: '待处理队列项', value: queueRows.value.filter((row) => row['队列状态'] === '待处理').length },
])

function resultClass(result: unknown) {
  if (result === '成功') return 'ok-text'
  if (result === '失败') return 'error-text'
  return 'muted-text'
}

async function getJson(path: string) {
  const response = await request(path)
  if (!response.ok) throw new Error(`接口返回 ${response.status}`)
  return response.json()
}

async function postJson(path: string, body?: unknown) {
  const response = await request(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined })
  const payload = await response.json()
  if (!response.ok) throw new Error(String(payload.detail ?? '操作未生效'))
  return payload
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [policyData, groupData, batchData, queueData] = await Promise.all([
      getJson(`${ENDPOINT}/policies`),
      getJson(`${ENDPOINT}/groups`),
      getJson(`${ENDPOINT}/batches`),
      getJson('/api/proxy/failover/queue'),
    ])
    policies.value = policyData.policies ?? []
    groupRows.value = groupData.items ?? []
    batches.value = (batchData.items ?? []).slice().reverse()
    queueRows.value = queueData.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '故障切数据加载失败'
  }
}

async function submitBatch() {
  errorMessage.value = ''
  const entryIds = entryIdsText.value
    .split(/[，,]/)
    .map((text) => Number(text.trim()))
    .filter((num) => Number.isInteger(num) && num > 0)
  if (!entryIds.length) {
    errorMessage.value = '请至少填写一个数据源条目 id'
    return
  }
  try {
    const payload = await postJson(`${ENDPOINT}/batches`, { entry_ids: entryIds, policy: policy.value })
    entryIdsText.value = ''
    window.alert(payload.message)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量故障切换提交失败'
  }
}

async function retry(batchNo: string) {
  errorMessage.value = ''
  try {
    const payload = await postJson(`${ENDPOINT}/batches/${batchNo}/retry`)
    window.alert(payload.message)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '整组重试失败'
  }
}

function toggleBatch(batchNo: string) {
  expanded.value = expanded.value === batchNo ? '' : batchNo
}

async function applyQueue(queueId: string | number) {
  errorMessage.value = ''
  try {
    const payload = await postJson(`/api/proxy/failover/queue/${queueId}/apply`)
    window.alert(payload.message)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '队列应用失败'
  }
}

async function applyAll() {
  errorMessage.value = ''
  try {
    const payload = await postJson('/api/proxy/failover/apply-all')
    window.alert(`已应用 ${payload['应用条数']} 条队列项`)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量应用失败'
  }
}

async function restore(group: string | number) {
  errorMessage.value = ''
  try {
    const payload = await postJson(`${ENDPOINT}/groups/${group}/restore`)
    window.alert(payload.message)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '原主源恢复失败'
  }
}

async function recompute() {
  errorMessage.value = ''
  try {
    const payload = await postJson(`${ENDPOINT}/recompute?source=前端手动重算`)
    window.alert(`重算完成：新增队列项 ${payload.result['新增队列项'].length} 条`)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重算失败'
  }
}

onMounted(reloadAll)
</script>
