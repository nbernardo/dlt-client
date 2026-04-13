import { sleepForSec } from "../../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../../@still/component/super/ViewComponent.js";
import { PivotTableController } from "../../../../controller/PivotTableController.js";
import { BIService } from "../../../../services/BIService.js";
import { BIUserInterfaceComponent } from "../main/BIUserInterfaceComponent.js";

export class PivotCreateComponent extends ViewComponent {

	isPublic = true;

    /** @Prop */ dashWorker;

    /** @Prop */ dataset = [];
    
	/** @Prop */ baseFields = [];

    /** @Prop */ selection = { rows: [], cols: [], vals: [] };
    /** @Prop */ filters = {};
    /** @Prop */ activeFilterField = null;
    /** @Prop */ calculatedFields = [];
    /** @Prop */ savedConfigs = []; 
    /** @Prop */ dashboardTiles = []; 
    /** @Prop */ expandedPaths = new Set();
    /** @Prop */ modes = ['sum', 'avg', 'count', 'max'];

	/** @Prop @type { HTMLElement } */container;

	/** 
	 * @Controller 
	 * @Path controller/
	 * @type { PivotTableController } */ controller;

	/** @type { BIUserInterfaceComponent } */ $parent;

	stAfterInit(){
		this.controller.on('load', async () => {
			this.controller.obj = this;
			await sleepForSec(500);
			this.dashWorker =  new Worker('/app/components/dataviz/bi/pivot/worker.js');
			this.container = document.getElementsByClassName('bi-pivot-ui-container')[0];
			this.getData();
			this.controller.renderAll();
			this.setWorkerListiner();
		});
	}

    setData = (data) => {
        if(data[0]) BIService.pivotBaseFields = Object.keys(data[0]);
        this.dataset = data;
        this.controller.initSidebar();
        this.$parent.filterUtil.initDrawerPicker();
    }

	getData(){

        /*
		for (let i = 0; i < 50000; i++) {
			// this.dataset.push({ 
			// 	'__rowId': i + 1, 'Dept': dept,   	'Level': level, 'Region': this.regions[i % this.regions.length],
			// 	'Gender': this.genders[i % this.genders.length], 'Contract': this.contractTypes[i % this.contractTypes.length],
			// 	'Office': this.officeTypes[i % this.officeTypes.length], 'ProjStatus': this.projStatus[i % this.projStatus.length],
			// 	'PayTerms': this.payTerms[i % this.payTerms.length], 'Laptop': this.laptopTypes[i % this.laptopTypes.length],
			// 	'Shift': this.shiftTypes[i % this.shiftTypes.length], 'Cert': this.certs[i % this.certs.length],
			// 	'PerfRating': this.performance[i % this.performance.length], 'Rating': Math.floor(Math.random() * 5) + 1,
			// 	'Years': Math.floor(Math.random() * 20) + 1, 'Salary': salary, 
			// 	'Bonus': Math.floor(salary * (Math.random() * 0.18)), 'Overhead': Math.floor(Math.random() * 5000) + 2000,
			// 	'Efficiency%': 60 + Math.floor(Math.random() * 40), 'Util%': 50 + Math.floor(Math.random() * 50),
			// 	'TeamSize': 2 + Math.floor(Math.random() * 12), 'Insurance': 450 + Math.floor(Math.random() * 300),
			// 	'Commute_KM': this.officeTypes[i % 3] === "Remote" ? 0 : Math.floor(Math.random() * 45),
			// 	'Tax_Est': Math.floor(salary * 0.22), '401k_Contrib': Math.floor(salary * 0.05),
			// 	'Equity_Units': (this.levels.indexOf(level) > 3) ? 1000 + Math.floor(Math.random() * 5000) : 0,
			// 	'Satisfaction': Math.floor(Math.random() * 10) + 1
			// });
		}
        this.setData(this.dataset)
         */

	}

    toggleFilterValue(v) {
		const { filters, activeFilterField } = this;

        const idx = filters[activeFilterField].indexOf(v);
        if (idx > -1) filters[activeFilterField].splice(idx, 1);
        else filters[activeFilterField].push(v);
    }

    closeFilter() {
        this.container.querySelector('#filter-modal').style.display = 'none';
        this.renderAll();
    }

    switchTab(tab) {
        this.container.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        this.container.querySelector('#tab-' + tab).classList.add('active');
        this.container.querySelector('#lab-controls').style.display = tab === 'dash' ? 'none' : 'flex';
        this.container.querySelector('#table-canvas').style.display = tab === 'dash' ? 'none' : 'block';
        this.container.querySelector('#dashboard-canvas').style.display = tab === 'dash' ? 'flex' : 'none';
        if(tab === 'dash') this.renderDashboard();
    }

	setWorkerListiner(){
		const self = this;
		this.dashWorker.onmessage = function(e) {
			const { root, cols, tileIndex, cfg, type, progress } = e.data;

            console.log(`WILL GOD WITH NEW DATA: `, { root, cols, tileIndex, cfg, type, progress });
            
            if (type === 'PROGRESS') {
                console.log(`REGISTERING PROGRESS: `, progress);
                
                // Update your UI element here
                //const progressBar = document.getElementById('pivot-progress-bar');
                //const progressText = document.getElementById('pivot-progress-text');
                
                //if (progressBar) progressBar.style.width = `${progress}%`;
                //if (progressText) progressText.textContent = `Processing: ${progress}%`;

            }else{

                const targetDiv = document.getElementById(`tile-${tileIndex}`);
                const loader = document.getElementById(`loader-${tileIndex}`);
                
                if (targetDiv) {
    
                    const htmlString = self.controller.getTableHTML(root, cols, cfg.selection, cfg.heatmap);
                    self.controller.updateTableDOM(targetDiv, htmlString);
                    
                    if (loader) {
                        loader.style.transition = "opacity 0.3s ease";
                        loader.style.opacity = "0";
    
                        setTimeout(() => {
                            if (loader.parentNode) loader.remove();
                        }, 300);
                    }
                }

            }

		};
	}

    evalFormula(item, formula) {
        let f = formula; BIService.pivotBaseFields.forEach(k => f = f.replace(new RegExp(k, 'g'), item[k] || 0));
        try { return eval(f); } catch { return 0; }
    }

    closeAllModals() { 
        this.container.querySelector('#calc-modal').style.display='none'; 
        this.container.querySelector('#filter-modal').style.display='none';
    }

    exportToCSV() {
		const { selection, filters } = this;
        if (!selection.rows.length || !selection.vals.length) return alert("No data to export");
        const { root, cols } = this.controller.buildTree(selection, filters, this.container.querySelector('#show-all-rows-check').checked);
        let csv = [];
        let header = ["Dimensions"];
        cols.forEach(c => selection.vals.forEach(v => header.push(`${c} (${v.field} ${v.mode})`)));
        header.push("Grand Total");
        csv.push(header.join(","));
        const processNode = (node, label) => {
            let row = [`"${label.split('|').pop()}"`];
            cols.forEach(c => selection.vals.forEach(v => {
                const k = `${c}_${v.field}`;
                row.push(Math.round(this.controller.getVal(node.values[k], v.mode)));
            }));
            selection.vals.forEach(v => {
                row.push(Math.round(this.controller.getVal(node.values[`TOTAL_${v.field}`], v.mode)));
            });
            csv.push(row.join(","));
            Object.keys(node.children).sort().forEach(k => {
                processNode(node.children[k], k);
            });
        };

        processNode(root, "Grand Total");
        const blob = new Blob([csv.join("\n")], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.setAttribute('hidden', '');
        a.setAttribute('href', url);
        a.setAttribute('download', `pivot_export_${new Date().getTime()}.csv`);
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
    }

    clearWorkspace() {
        if (confirm("Are you sure you want to clear the current layout?")) {
            this.selection = { rows: [], cols: [], vals: [] };
            this.expandedPaths.clear(), this.controller.searchQuery = "";
            this.container.querySelector('#global-search').value = "";
            this.container.querySelector('#show-all-rows-check').checked = false;
            BIService.pivotBaseFields.forEach(f => {
                this.filters[f] = [...new Set(this.dataset.map(item => item[f]))];
            });
            this.controller.renderAll();
        }
    }
}