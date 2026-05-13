import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { BIUserInterfaceComponent } from "../bi/main/BIUserInterfaceComponent.js";

export class BIController2 extends BaseController {

    /** @type { BIUserInterfaceComponent } */
    obj;

    /** @returns { HTMLElement } */
    $ = (id) => this.obj.popup.querySelector(id)

    toggleTablesDrawer(hideIt = true) {
        if(hideIt) this.$('.tables-explorer-drawer').style.zIndex = 1000;

        const drawer = this.$('.tables-explorer-drawer').querySelector('.drawer-inner-container');
        
        if(hideIt) drawer.classList.toggle('open');

        if(!drawer.classList.contains('open'))
            this.$('.tables-explorer-drawer').style.zIndex = -1;
    }

    /**
     * Renders the results of your generic PostgreSQL query
     * @param {Array} tables - Array of { table_name, module_prefix }
     */
    renderExplorerTables(tables) {
        const listContainer = this.$('#generic-tables-list');
        
        listContainer.innerHTML = tables.map(table => `
            <div class="drawer-table-item" 
                style="padding: 10px; border: 1px solid var(--border); border-radius: var(--radius); cursor: pointer; display: flex; align-items: center; justify-content: space-between;"
                onclick="controller.handleExplorerTableClick('${table.table_name}')">
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="font-size: 16px;">📁</span>
                    <span style="font-size: 12px; font-weight: 500;">${table.table_name}</span>
                </div>
                <span style="font-size: 9px; color: var(--muted); background: var(--surface2); padding: 2px 6px; border-radius: 10px; text-transform: uppercase;">
                    ${table.table_name.split('_')[0]}
                </span>
            </div>
        `).join('');
    }

    filterExplorerTables(query) {
        const q = query.toLowerCase();
        this.obj.popup.querySelectorAll('.drawer-table-item').forEach(item => {
            item.style.display = item.innerText.toLowerCase().includes(q) ? 'flex' : 'none';
        });
    }

}