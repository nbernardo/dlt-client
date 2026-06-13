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

    ROLE_COLORS = ['badge-blue','badge-teal','badge-amber','badge-gray'];

    $ = (ref) => this.obj.container.querySelector(ref);
    $$ = (ref) => this.obj.container.querySelectorAll(ref);

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
        if (cell.querySelector('input, select')) return; // Avoid double rendering
        
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
            const chips = ar.length
                ? ar.map(r => `<span class="role-badge ${this.roleColor(r)}">${r}</span>`).join('')
                : '<span style="color:var(--color-text-secondary);font-size:12px">No access</span>';
            return this.obj.parseEvents(`<tr>
                <td><span style="font-family:var(--font-mono);font-size:12px">${f.name}</span></td>
                <td>${f.table ? `<span class="badge badge-blue">${f.table}</span>` : '—'}</td>
                <td><div class="access-cell">${chips}</div></td>
                <td><button class="icon-btn" onclick="controller.openEditAccess('${f.id}')" title="Edit access"><i class="ti ti-shield-half"></i></button></td>
            </tr>`);
        }).join('');
    }

    renderAll() { this.populateTableSelects(), this.renderDict(), this.renderRbac(); }

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
        this.$('#a-roles-chips').innerHTML = this.roles.map(r => this.obj.parseEvents(`
            <span class="chip ${cur.includes(r) ? 'on' : 'off'}" id="chip-${r}" onclick="controller.toggleChip('${r}')">
                <i class="ti ti-${cur.includes(r) ? 'eye' : 'eye-off'}" aria-hidden="true" style="font-size:12px"></i> ${r}
            </span>`)
        ).join('');

        this.openModal('modal-access');
    }

    toggleChip(r) {
        const chip = this.$('#chip-' + r);
        chip.classList.toggle('on');
        chip.classList.toggle('off');
        chip.innerHTML = `<i class="ti ti-${chip.classList.contains('on') ? 'eye' : 'eye-off'}" aria-hidden="true" style="font-size:12px"></i> ${r}`;
    }

    saveAccess() {
        const sel = this.roles.filter(r => this.$('#chip-' + r)?.classList.contains('on'));
        this.access[this.editingAccessId] = sel;
        this.closeModal('modal-access');
        this.renderRbac();
    }

    openBulkAccess() {
        this.populateTableSelects();
        this.openModal('modal-bulk');
    }

    applyBulk() {
        const t = this.$('#b-table').value, r = this.$('#b-role').value;
        const grant = this.$('input[name="b-access"]:checked').value === 'grant';
        if (!r) return;

        this.fields.filter(f => !t || f.table === t).forEach(f => {
            this.access[f.id] = this.access[f.id] || [];
            if (grant && !this.access[f.id].includes(r)) this.access[f.id].push(r);
            if (!grant) this.access[f.id] = this.access[f.id].filter(x => x !== r);
        });
        this.closeModal('modal-bulk');
        this.renderRbac();
    }

}