import { BIService } from "../../../../services/BIService.js";
import { BIUserInterfaceComponent } from "../main/BIUserInterfaceComponent.js";

export class FilterUtil {

    /** @type { BIUserInterfaceComponent } */
    biUiObj;

    /** @type { HTMLElement } */
    objContainer;

    customGlobalFilters = {};

    activeFilterField;

    dataset = [];

    filters = {};

    /** @returns { HTMLElement } */
    $ = (id) => this.biUiObj.popup.querySelector(id)

    constructor(biUiObj, dataset){ 
        this.biUiObj = biUiObj;
        this.objContainer = this.biUiObj.container;
        this.dataset = dataset;
        //this.initDrawerPicker();
    }

    toggleFilterDrawer(hideIt = true) {
        const drawer = document.getElementById('global-filter-drawer');
        if(hideIt) drawer.classList.toggle('open');
    }

    initDrawerPicker() {
        const picker = this.$('#drawer-field-picker');
        BIService.pivotBaseFields.forEach(f => {
            const opt = document.createElement('option');
            opt.value = f;
            opt.textContent = f;
            picker.appendChild(opt);
        });
    }

    addCustomGlobalFilter(field) {
        if (!field || this.customGlobalFilters[field]) return;
        
        this.customGlobalFilters[field] = [...new Set(this.dataset.map(item => item[field]))];
        this.renderDrawerFilters();
        this.$('#drawer-field-picker').value = "";
    }

    renderDrawerFilters() {
        const list = this.$('#global-filters-list');
        const summaryContainer = this.$('#active-filters-summary');
        const badgeContainer = this.$('#summary-badges');
        
        if (!list || !summaryContainer || !badgeContainer) return;

        list.innerHTML = '';
        badgeContainer.innerHTML = '';
        
        const activeFields = Object.keys(this.customGlobalFilters);
        
        if (activeFields.length > 0) {
            summaryContainer.style.display = 'block';
            
            activeFields.forEach(field => {

                const badge = document.createElement('div');
                badge.style = "background: #4f6ef7; color: white; padding: 2px 8px; border-radius: 12px; font-size: 10px; display: flex; align-items: center; gap: 5px;";
                badge.innerHTML = `${field} <span style="cursor:pointer;" onclick="removeGlobalFilter('${field}')">×</span>`;
                badgeContainer.appendChild(badge);

                // 2. Detailed Card with visible values
                const div = document.createElement('div');
                div.className = 'filter-card';
                
                const selectedValues = this.customGlobalFilters[field];
                const totalValues = [...new Set(this.dataset.map(item => item[field]))];

                // Generate the chips for the specific values applied
                const valueChipsHtml = selectedValues.map(val => `
                    <span style="display:inline-block; background:#e2e8f0; color:#475569; padding:2px 6px; border-radius:4px; font-size:10px; margin:2px;">
                        ${val}
                    </span>
                `).join('');
                
                // The events (e.g. controller.openFilter) are being parsed against the BIController
                div.innerHTML = this.biUiObj.parseEvents(`
                    <div class="filter-card-header" style="display:flex; justify-content:space-between; align-items:center; margin-bottom:5px;">
                        <strong style="font-size:12px;">${field}</strong>
                        <span style="cursor:pointer; color:#ef4444; font-size:10px;" onclick="controller.removeGlobalFilter('${field}')">Remove</span>
                    </div>
                    
                    <div style="display:flex; flex-wrap:wrap; max-height:80px; overflow-y:auto; margin-bottom:10px; border:1px inset #f1f5f9; padding:5px; background:#fafafa; border-radius:4px;">
                        ${selectedValues.length === totalValues.length ? '<span style="font-size:10px; color:#94a3b8;">All values active</span>' : valueChipsHtml}
                    </div>

                    <button class="bi-filter-action-btn" style="width:100%; padding:5px; font-size:11px; border-radius:4px; cursor:pointer;" 
                            onclick="controller.openFilter(event, '${field}', true)">
                        Edit Values (${selectedValues.length})
                    </button>
                `);
                list.appendChild(div);
            });
        } else {
            summaryContainer.style.display = 'none';
            list.innerHTML = '<div style="padding:40px; color:#94a3b8; text-align:center; font-size:12px;">No active filters.</div>';
        }
    }

    removeGlobalFilter(field) {
        if (this.customGlobalFilters[field]) delete this.customGlobalFilters[field];
        this.renderDrawerFilters();    
        this.applyGlobalFilters();
    }

    clearAllGlobalFilters() {
        for (let key in customGlobalFilters) delete customGlobalFilters[key];
        this.renderDrawerFilters();
        this.applyGlobalFilters();
    }

    applyGlobalFilters() {
        this.toggleFilterDrawer(false);
        this.biUiObj.pivotTableProxy.controller.renderAll(this.customGlobalFilters); 
        //if(this.$('#tab-dash').classList.contains('active')) 
            this.biUiObj.pivotTableProxy.controller.renderDashboard(null, null, null, this.customGlobalFilters);
    }

    openFilter(e, f, isGlobal = false) {
        e.stopPropagation();
        this.activeFilterField = f;
        const modal = this.$('#filter-modal');
        const list = this.$('#modal-list');
        
        const targetSource = isGlobal ? this.customGlobalFilters : this.biUiObj.filters;
        
        modal.style.position = 'absolute';
        modal.style.display = 'block';

        if (isGlobal) {
            modal.style.top = "60px";
            modal.style.left = "310px";
        } else {
            const rect = e.currentTarget.getBoundingClientRect();
            const containerRect = this.$('.drawer-wrapper').getBoundingClientRect();
            
            modal.style.top = (rect.top - containerRect.top + 30) + "px";
            modal.style.left = (rect.left - containerRect.left) + "px";
        }

        const uniqueVals = [...new Set(this.dataset.map(item => item[f]))];
        // controller.toggleFilterValueBehavior if implemented in the BIController, 
        // which mirrors toggleFilterValueBehavior from FilterUtil
        list.innerHTML = uniqueVals.map(v => this.biUiObj.parseEvents(`
            <label style="display:block; font-size:12px; margin-bottom:4px; cursor:pointer;">
                <input type="checkbox" ${targetSource[f].includes(v) ? 'checked' : ''} 
                    onchange="controller.toggleFilterValueBehavior('${v}', ${isGlobal})"> ${v}
            </label>
        `)).join('');

        modal.querySelector('button').onclick = () => {
            modal.style.display = 'none';
            if (isGlobal) {
                this.renderDrawerFilters();
            } else {
                this.biUiObj.controller.renderAll();
            }
        };
    }

    toggleFilterValueBehavior(v, isGlobal) {
        const target = isGlobal ? this.customGlobalFilters[this.activeFilterField] : this.filters[this.activeFilterField];
        const idx = target.indexOf(v);
        if (idx > -1) target.splice(idx, 1);
        else target.push(v);
    }

}
