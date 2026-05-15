import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { BIUserInterfaceComponent } from "../bi/main/BIUserInterfaceComponent.js";
import { BIService } from "../services/BIService.js";
import { BIController } from "./BIController.js";
import { DBDiagramController } from "./DBDiagramController.js";

export class BIController2 extends BaseController {

    /** @type { BIUserInterfaceComponent } */
    obj;

    /** @returns { HTMLElement } */
    $ = (id) => this.obj.popup.querySelector(id)

    /** @returns { BIController2 } */
    static fromContext = () => BIController2.get();

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
                
        listContainer.innerHTML = tables.map(table => this.obj.parseEvents(`
            <div class="drawer-table-item" 
                style="padding: 10px 0px; border: 1px solid var(--border); cursor: pointer; display: flex; align-items: center; justify-content: space-between;"
                onclick="controller('BIController2').fetchRootAndLevel1Tables('${table[1]}')">
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="padding: 6px 2px; padding-right: 0px;">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color: var(--muted2)">
                            <path d="M3 3h18v18H3zM3 9h18M3 15h18M9 3v18M15 3v18"/>
                        </svg>
                    </span>
                    <span style="font-size: 12px; font-weight: 500;">${table[1]}</span>
                </div>
            </div>
        `)).join('');

        listContainer.style.display = '';
    }

    filterExplorerTables(query) {
        const q = query.toLowerCase();
        this.obj.popup.querySelectorAll('.drawer-table-item').forEach(item => {
            item.style.display = item.innerText.toLowerCase().includes(q) ? 'flex' : 'none';
        });
    }

    async fetchRootAndLevel1Tables(tableName){
        const container = BIService.getDBDiagramContainer();
        BIController.fromContext().addLoadingOnContainer(container, 'Loading database diagram');
        const result = await BIService.getTablesWhenOdoo(tableName.toLowerCase());
        //console.log(`THE TABLE NAME ARE: `, result.tables.filter(row => [0,1].includes(row[0])));
        //const summaryRows = (result.tables || []).filter(row => [0,1].includes(row[0]))
        BIController.fromContext().removeLoadingFromContainer(container);
        this.obj.dbDiagramProxy.updateGraphData(result, tableName);
        //DBDiagramController.fromContext().renderGraph(tableName);
    }

}