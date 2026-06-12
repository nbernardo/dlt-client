import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { GovernanceMainComponent } from "../GovernanceMainComponent.js";

export class DataGovernanceController extends BaseController {

    /** @type { GovernanceMainComponent } */ obj;

	tables = ['customers','orders','products'];

	fields = [
		{id:1,name:'customer_id',table:'customers',trans:'Customer ID',desc:'Unique customer identifier'},
		{id:2,name:'full_name',table:'customers',trans:'Full name',desc:'Customer full name'},
		{id:3,name:'email',table:'customers',trans:'Email address',desc:'Contact email'},
		{id:4,name:'created_at',table:'customers',trans:'Registration date',desc:'Account creation timestamp'},
		{id:5,name:'order_id',table:'orders',trans:'Order ID',desc:'Unique order reference'},
		{id:6,name:'total_amount',table:'orders',trans:'Total amount',desc:'Order grand total'},
		{id:7,name:'status',table:'orders',trans:'Order status',desc:'Current fulfillment state'},
		{id:8,name:'product_sku',table:'products',trans:'Product SKU',desc:'Stock-keeping unit'},
		{id:9,name:'price',table:'products',trans:'Unit price',desc:'Listed price per unit'},
	];

	roles = ['admin','analyst','viewer','support'];
	access = {1:['admin','analyst','support'],2:['admin','analyst','support'],3:['admin','support'],4:['admin','analyst'],5:['admin','analyst','viewer'],6:['admin','analyst'],7:['admin','analyst','viewer','support'],8:['admin','analyst','viewer'],9:['admin','viewer']};
	nextId = 10;
	viewMode = 'flat';
	editingFieldId = null;
	editingAccessId = null;

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
        this.renderDict();  // FIX: was bare renderDict()
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
        if (!filtered.length) {
            body.innerHTML = `<tr><td colspan="5" class="empty">No fields found</td></tr>`;
            return;
        }

        if (this.viewMode === 'group') {
            let rows = '';
            const grouped = {};
            filtered.forEach(f => {
                const k = f.table || '(no table)';
                (grouped[k] = grouped[k] || []).push(f);
            });
            Object.keys(grouped).sort().forEach(t => {
                rows += `<tr class="group-header">
                    <td colspan="5">
                        <i class="ti ti-table" aria-hidden="true" style="margin-right:6px"></i>${t}
                        <span style="font-weight:400;opacity:.7">(${grouped[t].length} fields)</span>
                    </td>
                </tr>`;
                grouped[t].forEach(f => { rows += this.fieldRow(f); }); // FIX: explicit arrow keeps `this`
            });
            body.innerHTML = rows;
        } else {
            body.innerHTML = filtered.map(f => this.fieldRow(f)).join(''); // FIX: was .map(this.fieldRow) — loses `this`
        }
    }

    // FIX: converted to arrow function so `this` is always the controller instance
    fieldRow = (f) => {
        return `<tr>
            <td><span style="font-family:var(--font-mono);font-size:12px">${f.name}</span></td>
            <td>${f.table ? `<span class="badge badge-blue">${f.table}</span>` : '<span style="color:var(--color-text-secondary)">—</span>'}</td>
            <td>${f.trans || '<span style="color:var(--color-text-secondary)">—</span>'}</td>
            <td style="color:var(--color-text-secondary);font-size:12px">${f.desc || ''}</td>
            <td style="white-space:nowrap">
                <button class="icon-btn" onclick="controller.openEditField(${f.id})" title="Edit"><i class="ti ti-edit"></i></button>
                <button class="icon-btn btn-danger" onclick="controller.deleteField(${f.id})" title="Delete"><i class="ti ti-trash"></i></button>
            </td>
        </tr>`;
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
        if (!filtered.length) {
            body.innerHTML = `<tr><td colspan="4" class="empty">No fields found</td></tr>`;
            return;
        }

        body.innerHTML = filtered.map(f => {
            const ar = this.access[f.id] || [];
            const chips = ar.length
                ? ar.map(r => `<span class="role-badge ${this.roleColor(r)}">${r}</span>`).join('')
                : '<span style="color:var(--color-text-secondary);font-size:12px">No access</span>';
            return `<tr>
                <td><span style="font-family:var(--font-mono);font-size:12px">${f.name}</span></td>
                <td>${f.table ? `<span class="badge badge-blue">${f.table}</span>` : '—'}</td>
                <td><div class="access-cell">${chips}</div></td>
                <td><button class="icon-btn" onclick="controller.openEditAccess(${f.id})" title="Edit access"><i class="ti ti-shield-half"></i></button></td>
            </tr>`;
        }).join('');
    }

    renderAll() {
        this.populateTableSelects();
        this.renderDict();
        this.renderRbac();
    }

    openModal(id) { this.$('#' + id).classList.add('open'); }
    closeModal(id) { this.$('#' + id).classList.remove('open'); }

    openAddField() {
        this.editingFieldId = null;
        this.$('#modal-field-title').textContent = 'Add field';
        ['f-name','f-trans','f-desc'].forEach(i => this.$('#' + i).value = '');
        this.$('#f-table').value = '';
        this.populateTableSelects();
        this.openModal('modal-field');
    }

    openEditField(id) {
        this.editingFieldId = id;  // FIX: was bare editingFieldId
        const f = this.fields.find(x => x.id === id);  // FIX: was bare fields
        this.$('#modal-field-title').textContent = 'Edit field';
        this.$('#f-name').value = f.name;
        this.$('#f-trans').value = f.trans;
        this.$('#f-desc').value = f.desc;
        this.populateTableSelects();  // FIX: was bare populateTableSelects()
        this.$('#f-table').value = f.table || '';
        this.openModal('modal-field');  // FIX: was bare openModal()
    }

    saveField() {
        const name = this.$('#f-name').value.trim();
        if (!name) return;

        if (this.editingFieldId) {
            const f = this.fields.find(x => x.id === this.editingFieldId);
            f.name = name;
            f.table = this.$('#f-table').value;
            f.trans = this.$('#f-trans').value.trim();
            f.desc = this.$('#f-desc').value.trim();
        } else {
            const nf = {
                id: this.nextId++,  // FIX: was bare nextId
                name,
                table: this.$('#f-table').value,
                trans: this.$('#f-trans').value.trim(),
                desc: this.$('#f-desc').value.trim()
            };
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
        this.renderRolesList();
        this.openModal('modal-roles');
    }

    renderRolesList() {
        this.$('#roles-list').innerHTML = this.roles.map(r =>
            `<span class="chip on">${r} <button onclick="controller.removeRole('${r}')" title="Remove">×</button></span>`
            // FIX: was bare removeRole() — won't resolve in inline onclick
        ).join('');
    }

    addRole() {
        const v = this.$('#r-new').value.trim();
        if (v && !this.roles.includes(v)) {  // FIX: was bare roles.includes
            this.roles.push(v);
            this.$('#r-new').value = '';
        }
        this.renderRolesList();
        this.populateTableSelects();
    }

    removeRole(r) {
        this.roles = this.roles.filter(x => x !== r);
        Object.keys(this.access).forEach(k => {
            this.access[k] = (this.access[k] || []).filter(x => x !== r);
        });
        this.renderRolesList();
        this.populateTableSelects();
    }

    openEditAccess(id) {
        this.editingAccessId = id;
        const f = this.fields.find(x => x.id === id);
        this.$('#modal-access-title').textContent = 'Edit access — ' + f.name;
        this.$('#a-field').value = f.name;

        const cur = this.access[id] || [];
        this.$('#a-roles-chips').innerHTML = this.roles.map(r => `
            <span class="chip ${cur.includes(r) ? 'on' : 'off'}" id="chip-${r}" onclick="controller.toggleChip('${r}')">
                <i class="ti ti-${cur.includes(r) ? 'eye' : 'eye-off'}" aria-hidden="true" style="font-size:12px"></i> ${r}
            </span>`  // FIX: was bare toggleChip()
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
        // FIX: was document.getElementById — must scope to container; was bare editingAccessId
        this.access[this.editingAccessId] = sel;
        this.closeModal('modal-access');
        this.renderRbac();
    }

    openBulkAccess() {
        this.populateTableSelects();
        this.openModal('modal-bulk');
    }

    applyBulk() {
        const t = this.$('#b-table').value;
        const r = this.$('#b-role').value;
        const grant = this.$('input[name="b-access"]:checked').value === 'grant';
        if (!r) return;

        this.fields.filter(f => !t || f.table === t).forEach(f => {
            this.access[f.id] = this.access[f.id] || [];
            if (grant && !this.access[f.id].includes(r)) this.access[f.id].push(r);
            if (!grant) this.access[f.id] = this.access[f.id].filter(x => x !== r);  // FIX: was bare access[f.id]
        });
        this.closeModal('modal-bulk');
        this.renderRbac();
    }

}