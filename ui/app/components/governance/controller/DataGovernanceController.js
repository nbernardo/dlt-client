import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { GovernanceMainComponent } from "../GovernanceMainComponent.js";

export class DataGovernanceController extends BaseController {

    /** @type { GovernanceMainComponent } */ obj;

	tables = [];
	fields = [];

	roles = ['admin','analyst','viewer','support'];
	access = {1:['admin','analyst','support'],2:['admin','analyst','support'],3:['admin','support'],4:['admin','analyst'],5:['admin','analyst','viewer'],6:['admin','analyst'],7:['admin','analyst','viewer','support'],8:['admin','analyst','viewer'],9:['admin','viewer']};
	nextId = 10;
	viewMode = 'flat';
	editingFieldId = null;
	editingAccessId = null;
    isAddingInline = false;
    addingInlineTable = '';
    pipeline;
    changedFields = new Map();
    columnExclusions = {};

    ROLE_COLORS = ['badge-blue','badge-teal','badge-amber','badge-gray'];

    /** @returns { HTMLElement } */ $ = (ref) => this.obj.container.querySelector(ref);
    /** @returns { HTMLElement } */ $$ = (ref) => this.obj.container.querySelectorAll(ref);

    roleColor(r) {
        let i = this.roles.indexOf(r) % this.ROLE_COLORS.length;
        return this.ROLE_COLORS[i < 0 ? 0 : i];
    }

    switchTab(t, el) {
        this.$$('.tab').forEach(b => b.classList.remove('active'));
        el.classList.add('active');
        this.$$('.section').forEach(s => s.classList.remove('active'));
        this.$('#sec-' + t).classList.add('active');
    }

    setView(v, btn) {
        this.viewMode = v;
        this.$$('.seg button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        this.renderDict();
    }

    populateTableSelects() {
        ['dict-table-filter','rbac-table-filter','f-table','b-table'].forEach(id => {
            const el = this.$('#' + id);
            if (!el) return;
            const isFilter = id.includes('filter') || id === 'b-table';
            const cur = el.value;
            const tablesList = this.tables.map(t => `<option value="${t}">${t}</option>`).join('');
            el.innerHTML = (isFilter ? '<option value="">All tables</option>' : '<option value="">— no table —</option>') + tablesList;
            el.value = cur;
        });

        const br = this.$('#b-role');
        if (br) br.innerHTML = this.roles.map(r => `<option value="${r}">${r}</option>`).join('');

        const rf = this.$('#rbac-role-filter');
        if (rf) rf.innerHTML = '<option value="">All roles</option>' + this.roles.map(r => `<option value="${r}">${r}</option>`).join('');
    }

    renderDict() {
        const q = (this.$('#dict-search').value || '').toLowerCase();
        const tf = this.$('#dict-table-filter').value;

        let filtered = this.fields.filter(f =>
            (!tf || f.table === tf) && (!q || f.name.toLowerCase().includes(q) || f.trans.toLowerCase().includes(q))
        );

        const body = this.$('#dict-body');

        if (this.viewMode === 'group') {
            let rows = '';
            const grouped = {};
            
            this.tables.forEach(t => { grouped[t] = []; });
            filtered.forEach(f => {
                const k = f.table || '(no table)';
                if (!grouped[k]) grouped[k] = [];
                grouped[k].push(f);
            });

            Object.keys(grouped).sort().forEach(t => {
                rows += this.obj.parseEvents(`<tr class="group-header">
                    <td colspan="5">
                        <i class="ti ti-table" aria-hidden="true" style="margin-right:6px"></i>${t}
                        <span style="font-weight:400;opacity:.7">(${grouped[t].length} fields)</span>
                        <button class="inline-add-btn" onclick="controller.setInlineAdd(true, '${t}')" style="float:right; background:none; border:none; color:#0C447C; cursor:pointer;"><i class="ti ti-plus"></i> Add row</button>
                    </td>
                </tr>`);
                
                grouped[t].forEach(f => { rows += this.fieldRow(f); });                
                if (this.isAddingInline && this.addingInlineTable === t)
                    rows += this.inlineInputRow(t);
                
            });
            body.innerHTML = rows;
        } else {
            let html = filtered.map(f => this.fieldRow(f)).join('');

            if (this.isAddingInline && !this.addingInlineTable)
                html += this.inlineInputRow('');

            if (!filtered.length && !this.isAddingInline) 
                return body.innerHTML = `<tr><td colspan="5" class="empty">No fields found</td></tr>`;

            body.innerHTML = html;
        }
    }

    setInlineAdd(status, tableGroup = '') {
        this.isAddingInline = status;
        this.addingInlineTable = tableGroup;
        this.renderDict();
        if (status) 
            setTimeout(() => this.$('#inline-f-name')?.focus(), 50);
    }

    inlineInputRow(assignedTable) {
        const tableOptions = this.tables.map(t => `<option value="${t}" ${t === assignedTable ? 'selected' : ''}>${t}</option>`).join('');
        const tableSelector = assignedTable 
            ? `<span class="badge badge-blue">${assignedTable}</span>`
            : `<select id="inline-f-table" class="inline-cell-select"><option value="">— no table —</option>${tableOptions}</select>`;

        return this.obj.parseEvents(`<tr class="inline-insert-row">
            <td><input type="text" id="inline-f-name" placeholder="Field name..." class="inline-input" onkeydown="if(event.key==='Enter') controller.saveInlineField()"></td>
            <td>${tableSelector}</td>
            <td><input type="text" id="inline-f-trans" placeholder="Translation..." class="inline-input" onkeydown="if(event.key==='Enter') controller.saveInlineField()"></td>
            <td><input type="text" id="inline-f-desc" placeholder="Description..." class="inline-input" onkeydown="if(event.key==='Enter') controller.saveInlineField()"></td>
            <td style="text-align: right;">
                <button class="icon-btn" style="color:green" onclick="controller.saveInlineField()"><i class="ti ti-check"></i></button>
                <button class="icon-btn" style="color:red" onclick="controller.setInlineAdd(false)"><i class="ti ti-x"></i></button>
            </td>
        </tr>`);
    }
    
    saveInlineField() {
        const name = this.$('#inline-f-name').value.trim();
        if (!name) return;

        const table = this.addingInlineTable || (this.$('#inline-f-table') ? this.$('#inline-f-table').value : '');
        const trans = this.$('#inline-f-trans').value.trim();
        const desc = this.$('#inline-f-desc').value.trim();

        const nf = { id: this.nextId++, name, table, trans, desc, disabled: false };
        this.fields.push(nf);
        this.access[nf.id] = [];

        this.isAddingInline = false;
        this.addingInlineTable = '';
        this.renderAll();
    }

    makeEditable(cell, fieldId) {
        if (cell.querySelector('input')) return;
        
        const originalText = cell.textContent === '—' ? '' : cell.textContent;
        cell.innerHTML = `<input type="text" class="inline-cell-input" value="${originalText}" style="width:100%; box-sizing:border-box; height:26px;">`;
        
        const input = cell.querySelector('input');
        input.focus();
        input.select();

        const saveChanges = () => {
            const val = input.value.trim();
            const f = this.fields.find(x => x.id === fieldId);
            if (f) {
                f.trans = val;
                this.changedFields.set(`${f.id}`, f);
            }
            cell.innerHTML = val || '—';
            this.renderRbac();
        };

        input.onblur = saveChanges;
        input.onkeydown = (e) => {
            if (e.key === 'Enter') saveChanges();
            if (e.key === 'Escape') cell.innerHTML = originalText || '—';
        };
    }

    fieldRow = (f) => {
        const isDisabled = f.disabled === true;
        const rowClass = isDisabled ? 'row-disabled' : '';
        
        return this.obj.parseEvents(`<tr class="${rowClass}" data-id="${f.id}">
            <td class="editable-cell text-mono" onclick="controller.makeCellEditable(this, '${f.id}', 'name')">${f.name || '—'}</td>
            
            <td class="editable-cell" onclick="controller.makeTableSelectable(this, '${f.id}')">
                ${f.table ? `<span class="badge badge-blue">${f.table}</span>` : '<span class="text-muted">—</span>'}
            </td>
            
            <td class="editable-cell" onclick="controller.makeCellEditable(this, '${f.id}', 'trans')">${f.trans || '<span class="text-muted">—</span>'}</td>
            <td class="editable-cell text-muted" onclick="controller.makeCellEditable(this, '${f.id}', 'desc')">${f.desc || '—'}</td>
            
            <td style="white-space:nowrap; text-align: right;">
                ${f.trans ? `<button class="icon-btn btn-warning" onclick="event.stopPropagation(); controller.clearTranslation('${f.id}')" title="Clear Translation"><i class="ti ti-text-clear-formatting"></i></button>` : ''}
                <button class="icon-btn ${!isDisabled ? 'btn-success' : 'btn-secondary'}" onclick="event.stopPropagation(); controller.toggleFieldStatus('${f.id}')" title="${isDisabled ? 'Enable Field' : 'Disable Field'}">
                    <i class="fas fa-power-off"></i>
                </button>
                <button class="icon-btn btn-danger" onclick="event.stopPropagation(); controller.deleteField('${f.id}')" title="Delete"><i class="fas fa-trash"></i></button>
            </td>
        </tr>`);
    }

    clearTranslation(fieldId) {
        const f = this.fields.find(x => x.id === fieldId);
        if (f) {
            f.trans = '';
            this.renderAll();
        }
    }

    toggleFieldStatus(fieldId) {
        const f = this.fields.find(x => x.id === fieldId);
        if (f) {
            f.disabled = !f.disabled;
            this.renderAll();
        }
    }

    makeCellEditable(cell, fieldId, property) {
        if (cell.querySelector('input, select')) return;
        
        const f = this.fields.find(x => x.id === fieldId);
        if (!f) return;

        const originalValue = f[property] || '';
        cell.innerHTML = `<input type="text" class="inline-cell-input" value="${originalValue}" style="width:100%; box-sizing:border-box;">`;
        
        const input = cell.querySelector('input');
        input.focus();
        input.select();

        const commitChanges = () => {
            const currentVal = input.value.trim();
            f[property] = currentVal;
            this.changedFields.set(`${f.id}`, f);
            
            this.renderDict(), this.renderRbac();
        };

        input.onblur = commitChanges;
        input.onkeydown = (e) => {
            if (e.key === 'Enter') commitChanges();
            if (e.key === 'Escape') this.renderDict();
        };
    }

    makeTableSelectable(cell, fieldId) {
        if (cell.querySelector('select, input')) return;

        const f = this.fields.find(x => x.id === fieldId);
        if (!f) return;

        let options = `<option value="">— no table —</option>`;
        options += this.tables.map(t => `<option value="${t}" ${t === f.table ? 'selected' : ''}>${t}</option>`).join('');

        cell.innerHTML = `<select class="inline-cell-select" style="width:100%; height:26px;">${options}</select>`;
        const select = cell.querySelector('select');
        select.focus();

        const commitTable = () => {
            f.table = select.value;
            this.renderDict();
            this.renderRbac();
        };

        select.onchange = commitTable, select.onblur = commitTable;
    }

    renderRbac() {
        const q = (this.$('#rbac-search').value || '').toLowerCase();
        const tf = this.$('#rbac-table-filter').value;
        const rf = this.$('#rbac-role-filter').value;

        let filtered = this.fields.filter(f => {
            if (tf && f.table !== tf) return false;
            if (q && !f.name.toLowerCase().includes(q) && !f.trans.toLowerCase().includes(q)) return false;
            if (rf && !(this.access[f.id] || []).includes(rf)) return false;
            return true;
        });

        const body = this.$('#rbac-body');
        if (!filtered.length)
            return body.innerHTML = `<tr><td colspan="4" class="empty">No fields found</td></tr>`;

        body.innerHTML = filtered.map(f => {
            const ar = this.access[f.id] || [];
            const hiddenRoles = this.columnExclusions[f.id] || [];
            
            const chips = ar.length
                ? ar.map(r => {
                    const isExcluded = hiddenRoles.includes(r);
                    return `
                        <span class="role-badge ${isExcluded ? 'badge-amber' : this.roleColor(r)}" 
                              style="${isExcluded ? 'text-decoration: line-through; opacity: 0.75;' : ''}" 
                              title="${isExcluded ? 'Role has table access but is explicitly blocked from this specific column' : 'Full visibility'}">
                            <i class="ti ti-${isExcluded ? 'eye-off' : 'user'}" style="font-size:11px;"></i> ${r} ${isExcluded ? '(Restricted)' : ''}
                        </span>
                    `;
                }).join('')
                : '<span style="color:var(--color-text-secondary);font-size:12px">No access</span>';

            return this.obj.parseEvents(`<tr>
                <td><span style="font-family:var(--font-mono);font-size:12px">${f.name}</span></td>
                <td>${f.table ? `<span class="badge badge-blue">${f.table}</span>` : '—'}</td>
                <td><div class="access-cell">${chips}</div></td>
                <td><button class="icon-btn" onclick="controller.openEditAccess('${f.id}')" title="Edit access"><i class="ti ti-shield-half"></i></button></td>
            </tr>`);
        }).join('');
    }

    renderAll() {
        (this.fields || []).forEach(f => {
            if (!this.access[f.id]) this.access[f.id] = [...this.roles];
            if (!this.columnExclusions[f.id]) this.columnExclusions[f.id] = [];
        });
        this.populateTableSelects(), this.renderDict(), this.renderRbac(); 
    }

    openModal(id) { this.$('#' + id).classList.add('open'); }
    closeModal(id) { this.$('#' + id).classList.remove('open'); }

    openAddField() {
        this.switchTab('dict', this.$$('.tab')[0]);
        this.setInlineAdd(true, '');
    }

    openAddGroup() {
        this.switchTab('dict', this.$$('.tab')[0]);
        const groupName = prompt("Enter new Table Group identifier:");
        if (groupName && !this.tables.includes(groupName.trim())) {
            this.tables.push(groupName.trim());
            this.renderAll();
        }
    }

    saveField() {
        const name = this.$('#f-name').value.trim();
        if (!name) return;

        if (this.editingFieldId) {
            const f = this.fields.find(x => x.id === this.editingFieldId);
            f.name = name, f.table = this.$('#f-table').value;
            f.trans = this.$('#f-trans').value.trim(), f.desc = this.$('#f-desc').value.trim();
        } else {
            const table = this.$('#f-table').value, trans = this.$('#f-trans').value.trim(), desc = this.$('#f-desc').value.trim()
            const nf = { id: this.nextId++, name, table, trans, desc };
            this.fields.push(nf);
            this.access[nf.id] = [];
        }

        this.closeModal('modal-field');
        this.renderAll();
    }

    deleteField(id) {
        this.fields = this.fields.filter(f => f.id !== id);
        delete this.access[id];
        this.renderAll();
    }

    openAddGroup() {
        this.$('#g-name').value = '';
        this.openModal('modal-group');
    }

    saveGroup() {
        const n = this.$('#g-name').value.trim();
        if (n && !this.tables.includes(n)) this.tables.push(n);
        this.closeModal('modal-group');
        this.renderAll();
    }

    openManageRoles() {
        this.renderRolesList(), this.openModal('modal-roles');
    }

    renderRolesList() {
        this.$('#roles-list').innerHTML = this.roles.map(r =>
            this.obj.parseEvents(`<span class="chip on">${r} <button onclick="controller.removeRole('${r}')" title="Remove">×</button></span>`)
        ).join('');
    }

    addRole() {
        const v = this.$('#r-new').value.trim();
        if (v && !this.roles.includes(v)) {
            this.roles.push(v);
            this.$('#r-new').value = '';
        }
        this.renderRolesList();
        this.populateTableSelects();
    }

    removeRole(r) {
        this.roles = this.roles.filter(x => x !== r);
        Object.keys(this.access).forEach(k => this.access[k] = (this.access[k] || []).filter(x => x !== r));
        this.renderRolesList();
        this.populateTableSelects();
    }

    openEditAccess(id) {
        this.editingAccessId = id;
        const f = this.fields.find(x => x.id === id);
        this.$('#modal-access-title').textContent = 'Edit access — ' + f.name;
        this.$('#a-field').value = f.name;

        const cur = this.access[id] || [];
        const currentExcluded = this.columnExclusions[id] || [];

        this.$('#a-roles-chips').innerHTML = this.roles.map(r => {
            const hasAccess = cur.includes(r);
            const isColumnHidden = currentExcluded.includes(r);
            
            return this.obj.parseEvents(`
                <div style="display:flex; align-items:center; justify-content:space-between; padding: 6px 0; border-bottom: 1px solid #f1f1f1; gap: 12px; width:100%;">
                    <span class="chip ${hasAccess ? 'on' : 'off'}" id="chip-${r}" onclick="controller.toggleChip('${r}')" style="margin:0; width:110px; justify-content:center;">
                        <i class="ti ti-${hasAccess ? 'eye' : 'eye-off'}" aria-hidden="true" style="font-size:12px"></i> ${r}
                    </span>
                    
                    <button class="btn ${isColumnHidden ? 'btn-warning' : 'btn-secondary'}" 
                            id="exclude-btn-${r}" 
                            onclick="controller.toggleColumnExclusion('${r}')" 
                            ${!hasAccess ? 'disabled style="opacity:0.4; cursor:not-allowed;"' : ''} 
                            style="font-size:11px; padding:2px 8px; height:24px; line-height:1;">
                        <i class="ti ti-${isColumnHidden ? 'lock' : 'lock-open'}"></i>
                        ${isColumnHidden ? 'Column Hidden' : 'Column Visible'}
                    </button>
                </div>
            `);
        }).join('');

        this.openModal('modal-access');
    }

    toggleChip(r) {
        const chip = this.$('#chip-' + r);
        chip.classList.toggle('on');
        chip.classList.toggle('off');
        const isNowOn = chip.classList.contains('on');
        chip.innerHTML = `<i class="ti ti-${isNowOn ? 'eye' : 'eye-off'}" aria-hidden="true" style="font-size:12px"></i> ${r}`;
        
        const exclBtn = this.$('#exclude-btn-' + r);
        if (exclBtn) {
            if (!isNowOn) {
                exclBtn.setAttribute('disabled', 'true');
                exclBtn.style.opacity = '0.4';
                exclBtn.style.cursor = 'not-allowed';
                exclBtn.classList.remove('btn-warning');
                exclBtn.classList.add('btn-secondary');
                exclBtn.innerHTML = `<i class="ti ti-lock-open"></i> Column Visible`;
            } else {
                exclBtn.removeAttribute('disabled');
                exclBtn.style.opacity = '1';
                exclBtn.style.cursor = 'pointer';
            }
        }
    }

    toggleColumnExclusion(r) {
        const btn = this.$('#exclude-btn-' + r);
        btn.classList.toggle('btn-warning');
        btn.classList.toggle('btn-secondary');
        const isNowExcluded = btn.classList.contains('btn-warning');
        btn.innerHTML = `<i class="ti ti-${isNowExcluded ? 'lock' : 'lock-open'}"></i> ${isNowExcluded ? 'Column Hidden' : 'Column Visible'}`;
    }

    saveAccess() {
        const sel = this.roles.filter(r => this.$('#chip-' + r)?.classList.contains('on'));
        const excludedRoles = this.roles.filter(r => {
            const btn = this.$('#exclude-btn-' + r);
            return btn && btn.classList.contains('btn-warning');
        });

        this.access[this.editingAccessId] = sel;
        this.columnExclusions[this.editingAccessId] = excludedRoles;
        
        this.closeModal('modal-access');
        this.renderRbac();
    }

    openBulkAccess() {
        this.populateTableSelects();        
        this.$('#b-role').value = '';
        const tableSelect = this.$('#b-table');
        tableSelect.value = '';

        const container = this.$('#bulk-fields-container');
        if (container) {
            container.innerHTML = '<p class="text-muted" style="font-size:12px; margin:4px 0; text-align:center;">Select a specific table above to view individual column rules...</p>';
        }

        tableSelect.onchange = () => this.renderBulkFieldsList(tableSelect.value);
        this.openModal('modal-bulk');
    }

    renderBulkFieldsList(tableName) {
        const container = this.$('#bulk-fields-container');
        if (!container) return;

        if (!tableName) {
            container.innerHTML = '<p class="text-muted" style="font-size:12px; margin:4px 0; text-align:center;">Select a specific table above to view individual column rules...</p>';
            return;
        }

        const targetFields = this.fields.filter(f => f.table === tableName);
        if (!targetFields.length) {
            container.innerHTML = '<p class="text-muted" style="font-size:12px; margin:4px 0; text-align:center;">No matching columns found under this table context.</p>';
            return;
        }

        const masterRowHtml = this.obj.parseEvents(`
            <div style="display:flex; align-items:center; justify-content:space-between; padding:8px; background:#eaeded; border:1px solid #bdc3c7; border-radius:4px; gap:8px; margin-bottom: 6px; font-weight: bold;">
                <span style="font-size:12px; color:#1a1a1a;"><i class="ti ti-settings"></i> ALL COLUMNS (Bulk Table Toggle)</span>
                <div style="display:flex; border:1px solid #1A3D5C; border-radius:4px; overflow:hidden; height:24px; box-sizing:border-box;">
                    <button type="button" class="bulk-toggle-btn active-allow" id="bulk-master-allow" data-table="${tableName}"
                            data-action="allow" onclick="controller.toggleAllBulkFields(this)" 
                            style="border:none; padding:0 12px; font-size:11px; font-weight:bold; cursor:pointer;">
                        Allow All
                    </button>
                    <button type="button" class="bulk-toggle-btn"  id="bulk-master-deny" data-table="${tableName}" data-action="deny"
                            onclick="controller.toggleAllBulkFields(this)" 
                            style="border:none; padding:0 12px; font-size:11px; font-weight:bold; cursor:pointer;">
                        Not Allow All
                    </button>
                </div>
            </div>
            <div style="height: 1px; background: #ddd; margin: 4px 0;"></div>
        `);

        const fieldsHtml = targetFields.map(f => this.obj.parseEvents(`
            <div style="display:flex; align-items:center; justify-content:space-between; padding:6px 8px; background:#fff; border:1px solid #e2e8f0; border-radius:4px; gap:8px;">
                <span style="font-family:var(--font-mono, monospace); font-size:12px; font-weight:600; color:#2d3748;">${f.name}</span>
                
                <div style="display:flex; border:1px solid #1A3D5C; border-radius:4px; overflow:hidden; height:24px; box-sizing:border-box;">
                    <button type="button" class="bulk-toggle-btn active-allow" 
                            id="bulk-allow-${f.id}" data-id="${f.id}" data-action="allow" onclick="controller.setBulkFieldAction(this)" 
                            style="border:none; padding:0 10px; font-size:11px; font-weight:bold; cursor:pointer; transition:all 0.1s ease;">
                        Allow
                    </button>
                    <button type="button" class="bulk-toggle-btn" 
                            id="bulk-deny-${f.id}" data-id="${f.id}" data-action="deny" onclick="controller.setBulkFieldAction(this)" 
                            style="border:none; padding:0 10px; font-size:11px; font-weight:bold; cursor:pointer; transition:all 0.1s ease;">
                        Not Allow
                    </button>
                </div>
            </div>
        `)).join('');

        container.innerHTML = masterRowHtml + fieldsHtml;
    }

    setBulkFieldAction(element) {
        if (!element) return;
        const action = element.dataset.action;
        const parent = element.parentElement;
        if (!parent) return;

        const allowBtn = parent.querySelector('[data-action="allow"]');
        const denyBtn = parent.querySelector('[data-action="deny"]');
        
        if (!allowBtn || !denyBtn) return;

        if (action === 'allow') {
            allowBtn.classList.add('active-allow');
            denyBtn.classList.remove('active-deny');
        } else {
            allowBtn.classList.remove('active-allow');
            denyBtn.classList.add('active-deny');
        }
    }

    toggleAllBulkFields(element) {
        if (!element) return;
        const tableName = element.dataset.table;
        const action = element.dataset.action;

        const masterAllow = this.$('#bulk-master-allow');
        const masterDeny = this.$('#bulk-master-deny');
        
        if (action === 'allow') {
            if (masterAllow) masterAllow.classList.add('active-allow');
            if (masterDeny) masterDeny.classList.remove('active-deny');
        } else {
            if (masterAllow) masterAllow.classList.remove('active-allow');
            if (masterDeny) masterDeny.classList.add('active-deny');
        }

        const targetFields = this.fields.filter(f => f.table === tableName);
        targetFields.forEach(f => {
            const targetBtn = this.$('#bulk-' + (action === 'allow' ? 'allow-' : 'deny-') + f.id);
            if (targetBtn) {
                this.setBulkFieldAction(targetBtn);
            }
        });
    }

    createBulkFieldsWrapper() {
        const tableFieldGroup = this.$('#b-table').closest('.form-group') || this.$('#b-table').parentElement;
        const wrapper = document.createElement('div');
        wrapper.className = 'form-group';
        wrapper.style.marginTop = '12px';
        wrapper.innerHTML = `
            <label style="display:block; font-weight:bold; margin-bottom:6px; font-size:13px;">Fields Allocation State:</label>
            <div id="bulk-fields-container" style="max-height:160px; overflow-y:auto; border:1px solid #ddd; padding:8px; border-radius:4px; background:#fafafa;"></div>
        `;
        tableFieldGroup.parentNode.insertBefore(wrapper, tableFieldGroup.nextSibling);
        return this.$('#bulk-fields-container');
    }

    applyBulk() {
        const t = this.$('#b-table').value;
        const r = this.$('#b-role').value;
        
        if (!r) return;
        if (!t) return;

        this.fields.filter(f => f.table === t).forEach(f => {
            const allowBtn = this.obj.container.querySelector(`.bulk-toggle-btn[data-field-id="${f.id}"][data-action="allow"]`);
            const isAllowed = allowBtn && allowBtn.classList.contains('active-allow');

            this.access[f.id] = this.access[f.id] || [];
            this.columnExclusions[f.id] = this.columnExclusions[f.id] || [];

            if (isAllowed) {
                if (!this.access[f.id].includes(r))
                    this.access[f.id].push(r);
                this.columnExclusions[f.id] = this.columnExclusions[f.id].filter(x => x !== r);
            } else {
                if (!this.columnExclusions[f.id].includes(r)) 
                    this.columnExclusions[f.id].push(r);
            }
        });

        this.closeModal('modal-bulk');
        this.renderRbac();
    }

}