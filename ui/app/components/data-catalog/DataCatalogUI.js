import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";

export class DataCatalogUI extends ViewComponent {

	isPublic = false;

  /** @Prop */ showWindowPopup = false;

  /** @Prop */ uniqueId = false;

  /** @Prop */ popup = false;

	async stOnRender(){
		await Assets.import({ path: '/app/assets/css/data-catalog.css' });
	}

  async stAfterInit(){
    this.popup = document.getElementById(this.uniqueId);
  }

  closePopup(){
		this.popup.classList.add('hidden');
		this.popup.classList.remove('minimized', 'maximized');
		this.isMinimized = false;
		this.showWindowPopup = false;
  }

  showPopup = () => {
    this.showWindowPopup = true;
    this.popup.classList.remove('hidden');
  }

}


/**
 

let currentPipeline = null;
let currentTable    = null;
let currentFilter   = 'all';
let editingCell     = null;

const PIPELINES = {
  p1: {
    name: 'orcl_testing_to_mssql',
    tables: {
      'CUSTOMERS': {
        columns: [
          { name:'CUSTOMER_ID',  type:'integer', version:1, deleted:0, semantic:'customer_identifier', sem_source:'rule', validated:1 },
          { name:'FIRST_NAME',   type:'varchar', version:1, deleted:0, semantic:'person_name',         sem_source:'rule', validated:1 },
          { name:'LAST_NAME',    type:'varchar', version:2, deleted:0, semantic:'person_name',         sem_source:'rule', validated:1 },
          { name:'EMAIL',        type:'varchar', version:1, deleted:0, semantic:'email',               sem_source:'rule', validated:1 },
          { name:'PHONE',        type:'varchar', version:1, deleted:0, semantic:'phone_number',        sem_source:'rule', validated:0 },
          { name:'CREDIT_LIMIT', type:'decimal', version:3, deleted:0, semantic:'financial_value',     sem_source:'llm',  validated:0 },
          { name:'OLD_SEGMENT',  type:'varchar', version:2, deleted:1, semantic:'',                    sem_source:'',     validated:0 },
        ]
      },
      'ORDERS': {
        columns: [
          { name:'ORDER_ID',     type:'integer', version:1, deleted:0, semantic:'identifier',          sem_source:'rule', validated:1 },
          { name:'CUSTOMER_ID',  type:'integer', version:1, deleted:0, semantic:'customer_identifier', sem_source:'rule', validated:1 },
          { name:'ORDER_DATE',   type:'date',    version:1, deleted:0, semantic:'date',                sem_source:'rule', validated:1 },
          { name:'TOTAL_AMOUNT', type:'decimal', version:2, deleted:0, semantic:'financial_value',     sem_source:'llm',  validated:0 },
          { name:'STATUS',       type:'varchar', version:1, deleted:0, semantic:'status',              sem_source:'rule', validated:1 },
        ]
      }
    }
  },
  p2: {
    name: '0_testing_mssql_to_psql',
    tables: {
      'human_resources_shift': {
        columns: [
          { name:'shift_id',   type:'integer', version:1, deleted:0, semantic:'identifier', sem_source:'rule', validated:1 },
          { name:'name',       type:'varchar', version:1, deleted:0, semantic:'name',       sem_source:'rule', validated:1 },
          { name:'start_time', type:'time',    version:1, deleted:0, semantic:'time',       sem_source:'llm',  validated:0 },
          { name:'end_time',   type:'time',    version:1, deleted:0, semantic:'time',       sem_source:'llm',  validated:0 },
        ]
      }
    }
  }
};

let RULES = [
  { id:1,  pattern:'.*_id$|^id$',                                           concept:'identifier',          confidence:0.95 },
  { id:2,  pattern:'.*_at$|.*_date$|^date.*|.*date$',                       concept:'date',                confidence:0.95 },
  { id:3,  pattern:'.*name.*',                                              concept:'name',                confidence:0.90 },
  { id:4,  pattern:'.*amount.*|.*price.*|.*salary.*|.*revenue.*|.*total.*', concept:'financial_value',     confidence:0.90 },
  { id:5,  pattern:'.*email.*',                                             concept:'email',               confidence:0.98 },
  { id:6,  pattern:'.*phone.*|.*mobile.*',                                  concept:'phone_number',        confidence:0.95 },
  { id:7,  pattern:'.*address.*|.*street.*|.*city.*|.*zip.*',               concept:'address',             confidence:0.88 },
  { id:8,  pattern:'.*status.*|.*state.*',                                  concept:'status',              confidence:0.85 },
  { id:9,  pattern:'.*count.*|.*qty.*|.*quantity.*',                        concept:'metric',              confidence:0.85 },
  { id:10, pattern:'.*customer.*|.*client.*|.*cust.*',                      concept:'customer_identifier', confidence:0.92 },
];
let ruleIdCounter = 11;

function showToast(msg, type='default') {
  const t = document.getElementById('toast');
  t.className = 'toast show ' + type;
  document.getElementById('toastMsg').textContent = msg;
  setTimeout(() => t.classList.remove('show'), 2500);
}

function switchTab(tab, btn) {
  document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  btn.classList.add('active');
  if (tab === 'rules') renderRules();
  if (tab === 'history') renderHistory();
}

function onPipelineChange() {
  const val = document.getElementById('pipelineSelect').value;
  currentPipeline = val || null;
  currentTable = null;
  renderSidebar();
  renderColumns();
  updateStats();
}

function renderSidebar() {
  const list = document.getElementById('tableList');
  if (!currentPipeline || !PIPELINES[currentPipeline]) {
    list.innerHTML = '<div class="empty" style="padding:24px 20px"><div class="empty-text">Select a pipeline</div></div>';
    document.getElementById('tableCount').textContent = '0';
    return;
  }
  const tables = PIPELINES[currentPipeline].tables;
  const names = Object.keys(tables);
  document.getElementById('tableCount').textContent = names.length;
  list.innerHTML = names.map(name => {
    const cols = tables[name].columns;
    const active = cols.filter(c => !c.deleted).length;
    const deleted = cols.filter(c => c.deleted).length;
    return `<div class="table-item ${currentTable === name ? 'active' : ''}" onclick="selectTable('${name}')">
      <div class="table-item-icon">⬡</div>
      <div class="table-item-info">
        <div class="table-item-name">${name}</div>
        <div class="table-item-meta">${active} cols${deleted ? ` · ${deleted} deleted` : ''}</div>
      </div>
      <div class="badge badge-blue">${cols.length}</div>
    </div>`;
  }).join('');
}

function selectTable(name) {
  currentTable = name;
  renderSidebar();
  renderColumns();
}

function updateStats() {
  if (!currentPipeline) {
    ['statTotal','statActive','statDeleted','statEvolved'].forEach(id => document.getElementById(id).textContent = '—');
    return;
  }
  const tables = PIPELINES[currentPipeline].tables;
  let total=0, active=0, deleted=0, evolved=0;
  Object.values(tables).forEach(t => t.columns.forEach(c => {
    total++;
    if (c.deleted) deleted++; else active++;
    if (c.version > 1) evolved++;
  }));
  document.getElementById('statTotal').textContent   = total;
  document.getElementById('statActive').textContent  = active;
  document.getElementById('statDeleted').textContent = deleted;
  document.getElementById('statEvolved').textContent = evolved;
}

function setFilter(f, btn) {
  currentFilter = f;
  document.querySelectorAll('.filter-chip').forEach(el => el.classList.remove('active'));
  btn.classList.add('active');
  renderColumns();
}

function filterColumns() { renderColumns(); }

function getFilteredCols() {
  if (!currentPipeline || !currentTable) return [];
  const cols = PIPELINES[currentPipeline].tables[currentTable].columns;
  const search = document.getElementById('colSearch').value.toLowerCase();
  return cols.filter(c => {
    if (search && !c.name.toLowerCase().includes(search)) return false;
    if (currentFilter === 'active')  return !c.deleted;
    if (currentFilter === 'deleted') return c.deleted;
    if (currentFilter === 'evolved') return c.version > 1;
    if (currentFilter === 'pending') return !c.semantic && !c.deleted;
    return true;
  });
}

function renderColumns() {
  const tbody = document.getElementById('colTableBody');
  document.getElementById('panelTableName').textContent = currentTable || 'Select a table';
  const cols = getFilteredCols();
  if (!currentTable || cols.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7"><div class="empty"><div class="empty-icon">◈</div><div class="empty-text">${!currentTable ? 'Select a table' : 'No columns match filter'}</div></div></td></tr>`;
    return;
  }
  tbody.innerHTML = cols.map((c, i) => {
    const statusBadge = c.deleted
      ? `<span class="badge badge-red">deleted</span>`
      : c.version > 1
        ? `<span class="badge badge-orange">evolved</span>`
        : `<span class="badge badge-green">active</span>`;

    const semCell = c.deleted ? '—' : c.semantic
      ? `<div class="semantic-cell">
           <span class="semantic-tag ${c.validated ? '' : 'pending'}" onclick="editSemantic(${i}, this)">${c.semantic}</span>
           ${c.validated ? '<span style="color:var(--success);font-size:10px">✓</span>' : '<span style="color:var(--warning);font-size:10px">⏳</span>'}
           <span style="font-family:var(--mono);font-size:10px;color:var(--muted)">${c.sem_source}</span>
         </div>`
      : `<div class="semantic-cell"><span class="semantic-tag empty" onclick="editSemantic(${i}, this)">+ assign</span></div>`;

    const versionEl = c.version > 1
      ? `<span class="version-chip changed">v${c.version} ↑</span>`
      : `<span class="version-chip">v${c.version}</span>`;

    return `<tr>
      <td><span class="col-name ${c.deleted ? 'col-name-deleted' : ''}">${c.name}</span></td>
      <td><span class="type-pill">${c.type}</span></td>
      <td>${versionEl}</td>
      <td>${statusBadge}</td>
      <td>${semCell}</td>
      <td><span style="font-family:var(--mono);font-size:11px;color:var(--muted)">${PIPELINES[currentPipeline]?.name || ''}</span></td>
      <td>${!c.deleted ? `<button class="icon-btn" onclick="showColHistory('${c.name}')">⊙</button>` : ''}</td>
    </tr>`;
  }).join('');
}

function editSemantic(idx, el) {
  if (editingCell) return;
  editingCell = true;
  const cols = PIPELINES[currentPipeline].tables[currentTable].columns;
  const visible = getFilteredCols();
  const col = visible[idx];
  const colIdx = cols.indexOf(col);
  const input = document.createElement('input');
  input.className = 'semantic-edit-input';
  input.value = col.semantic || '';
  input.placeholder = 'concept name...';
  el.replaceWith(input);
  input.focus();
  function save() {
    const val = input.value.trim();
    cols[colIdx].semantic = val;
    cols[colIdx].validated = 0;
    cols[colIdx].sem_source = 'manual';
    editingCell = null;
    renderColumns();
    if (val) showToast(`Semantic concept "${val}" assigned`, 'success');
  }
  input.addEventListener('blur', save);
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') save();
    if (e.key === 'Escape') { editingCell = null; renderColumns(); }
  });
}

function exportCSV() {
  const cols = getFilteredCols();
  if (!cols.length) return;
  const header = 'column_name,data_type,version,is_deleted,semantic_concept,validated\n';
  const rows = cols.map(c => `${c.name},${c.type},${c.version},${c.deleted},${c.semantic},${c.validated}`).join('\n');
  const blob = new Blob([header + rows], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `${currentTable}_catalog.csv`;
  a.click();
  showToast('Exported CSV', 'success');
}

function renderRules() {
  document.getElementById('rulesGrid').innerHTML = RULES.map(r => `
    <div class="rule-row" id="rule-${r.id}">
      <div class="rule-cell">
        <input class="rule-input rule-pattern" value="${r.pattern}" onchange="updateRule(${r.id}, 'pattern', this.value)" placeholder="regex pattern">
      </div>
      <div class="rule-cell">
        <input class="rule-input rule-concept" value="${r.concept}" onchange="updateRule(${r.id}, 'concept', this.value)" placeholder="semantic concept">
      </div>
      <div class="rule-cell" style="display:flex;align-items:center;gap:8px">
        <input type="range" class="confidence-slider" min="0" max="1" step="0.01" value="${r.confidence}"
          oninput="updateRule(${r.id}, 'confidence', parseFloat(this.value)); this.nextElementSibling.textContent=parseFloat(this.value).toFixed(2)">
        <span style="font-family:var(--mono);font-size:11px;color:var(--muted);min-width:32px">${r.confidence.toFixed(2)}</span>
      </div>
      <div class="rule-cell rule-actions">
        <button class="icon-btn delete" onclick="deleteRule(${r.id})">✕</button>
      </div>
    </div>`).join('');
}

function updateRule(id, field, val) {
  const rule = RULES.find(r => r.id === id);
  if (rule) rule[field] = val;
}

function deleteRule(id) {
  RULES = RULES.filter(r => r.id !== id);
  renderRules();
  showToast('Rule deleted');
}

function resetRules() {
  RULES = [
    { id:1,  pattern:'.*_id$|^id$',                                           concept:'identifier',          confidence:0.95 },
    { id:2,  pattern:'.*_at$|.*_date$|^date.*|.*date$',                       concept:'date',                confidence:0.95 },
    { id:3,  pattern:'.*name.*',                                              concept:'name',                confidence:0.90 },
    { id:4,  pattern:'.*amount.*|.*price.*|.*salary.*|.*revenue.*|.*total.*', concept:'financial_value',     confidence:0.90 },
    { id:5,  pattern:'.*email.*',                                             concept:'email',               confidence:0.98 },
    { id:6,  pattern:'.*phone.*|.*mobile.*',                                  concept:'phone_number',        confidence:0.95 },
    { id:7,  pattern:'.*address.*|.*street.*|.*city.*|.*zip.*',               concept:'address',             confidence:0.88 },
    { id:8,  pattern:'.*status.*|.*state.*',                                  concept:'status',              confidence:0.85 },
    { id:9,  pattern:'.*count.*|.*qty.*|.*quantity.*',                        concept:'metric',              confidence:0.85 },
    { id:10, pattern:'.*customer.*|.*client.*|.*cust.*',                      concept:'customer_identifier', confidence:0.92 },
  ];
  ruleIdCounter = 11;
  renderRules();
  showToast('Rules reset to defaults', 'success');
}

function openAddRuleModal() {
  document.getElementById('newPattern').value = '';
  document.getElementById('newConcept').value = '';
  document.getElementById('rulePreview').textContent = '';
  document.getElementById('ruleModal').classList.add('open');
}

function closeModal() {
  document.getElementById('ruleModal').classList.remove('open');
}

function previewRule() {
  const pattern = document.getElementById('newPattern').value;
  const concept = document.getElementById('newConcept').value;
  const preview = document.getElementById('rulePreview');
  if (!pattern || !concept) { preview.textContent = ''; return; }
  try {
    const re = new RegExp(pattern, 'i');
    const tests = ['customer_id','email_address','total_amount','order_date','first_name','status','phone_number'];
    const matches = tests.filter(t => re.test(t));
    preview.innerHTML = matches.length
      ? `<span style="color:var(--success)">✓ Matches:</span> ${matches.map(m => `<span style="color:var(--accent)">${m}</span>`).join(', ')} → <span style="color:var(--success)">${concept}</span>`
      : `<span style="color:var(--muted)">No matches against test columns</span>`;
  } catch(e) {
    preview.innerHTML = `<span style="color:var(--danger)">Invalid regex: ${e.message}</span>`;
  }
}

function addRule() {
  const pattern = document.getElementById('newPattern').value.trim();
  const concept = document.getElementById('newConcept').value.trim();
  const confidence = parseFloat(document.querySelector('#ruleModal input[type=range]').value);
  if (!pattern || !concept) { showToast('Pattern and concept required'); return; }
  try { new RegExp(pattern); } catch(e) { showToast('Invalid regex pattern'); return; }
  RULES.push({ id: ruleIdCounter++, pattern, concept, confidence });
  closeModal();
  renderRules();
  showToast(`Rule added: ${concept}`, 'success');
}

function testRule() {
  const input = document.getElementById('testColInput').value.trim().toLowerCase();
  const result = document.getElementById('testResult');
  if (!input) { result.innerHTML = ''; return; }
  let matched = null;
  for (const rule of RULES) {
    try { if (new RegExp(rule.pattern, 'i').test(input)) { matched = rule; break; } } catch(e) {}
  }
  result.innerHTML = matched
    ? `<div style="display:flex;align-items:center;gap:12px;padding:10px 12px;background:rgba(0,230,118,0.05);border:1px solid rgba(0,230,118,0.15);border-radius:6px">
         <span style="color:var(--success)">✓</span>
         <span style="font-family:var(--mono);font-size:12px">
           <span style="color:var(--accent)">${input}</span>
           <span style="color:var(--muted)"> → </span>
           <span style="color:var(--accent2)">${matched.pattern}</span>
           <span style="color:var(--muted)"> → </span>
           <span style="color:var(--success)">${matched.concept}</span>
           <span style="color:var(--muted)"> (${matched.confidence.toFixed(2)})</span>
         </span>
       </div>`
    : `<div style="display:flex;align-items:center;gap:12px;padding:10px 12px;background:rgba(255,171,64,0.05);border:1px solid rgba(255,171,64,0.15);border-radius:6px">
         <span style="color:var(--warning)">⚠</span>
         <span style="font-family:var(--mono);font-size:12px;color:var(--muted)">No rule matched <span style="color:var(--accent)">${input}</span> — will fall back to LLM inference</span>
       </div>`;
}

function renderHistory() {
  const events = [
    { time:'2026-03-10 19:42', event:'Column <b>CREDIT_LIMIT</b> type changed', meta:'decimal → numeric · version 3', dot:'var(--warning)' },
    { time:'2026-03-09 14:22', event:'Column <b>LAST_NAME</b> type changed',    meta:'nvarchar → varchar · version 2', dot:'var(--warning)' },
    { time:'2026-03-08 09:11', event:'Column <b>OLD_SEGMENT</b> marked deleted', meta:'version 2 · orcl_testing_to_mssql', dot:'var(--danger)' },
    { time:'2026-03-07 18:05', event:'Pipeline <b>orcl_testing_to_mssql</b> first run', meta:'7 columns registered', dot:'var(--success)' },
  ];
  document.getElementById('historyList').innerHTML = events.map(e => `
    <div class="history-item">
      <div class="history-time">${e.time}</div>
      <div class="history-dot" style="background:${e.dot}"></div>
      <div class="history-content">
        <div class="history-event">${e.event}</div>
        <div class="history-meta">${e.meta}</div>
      </div>
    </div>`).join('');
}

function showColHistory(colName) {
  switchTab('history', document.querySelector('.tab-btn:nth-child(3)'));
  showToast(`Showing history for ${colName}`);
}

renderSidebar();
renderColumns();


 */