import { sleepForSec } from "../../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../../@still/component/super/ViewComponent.js";
import { PivotTableController } from "../../../../controller/PivotTableController.js";

export class PivotCreateComponent extends ViewComponent {

	isPublic = true;

    /** @Prop */ dashWorker;

    /** @Prop */ dataset = [];
    
    /** @Prop */ depts = ["Sales", "Engineering", "HR", "Marketing", "Finance", "Legal", "Ops", "CS"];
    /** @Prop */ regions = ["North America", "EMEA", "APAC", "LATAM"];
    /** @Prop */ levels = ["Junior", "Mid-Level", "Senior", "Lead", "Principal", "Director"];
    /** @Prop */ genders = ["F", "M", "Non-Binary", "Other"];
    /** @Prop */ contractTypes = ["Full-Time", "Part-Time", "Contractor", "Freelance"];
    /** @Prop */ officeTypes = ["Remote", "Hybrid", "Onsite"];
    
    /** @Prop */ projStatus = ["Active", "On Hold", "Completed", "Backlog"];
    /** @Prop */ payTerms = ["Net 30", "Net 60", "Due on Receipt"];
    /** @Prop */ laptopTypes = ["MacBook Pro", "Dell XPS", "ThinkPad", "HP Elite"];
    /** @Prop */ shiftTypes = ["Day", "Night", "Flexible"];
    /** @Prop */ certs = ["None", "AWS Certified", "PMP", "Azure Dev"];
    /** @Prop */ performance = ["Exceeds", "Meets", "Below", "N/A"];

	/** @Prop */ baseFields = [
        'Dept', 'Level', 'Region', 'Gender', 'Contract', 'Office', 
        'ProjStatus', 'PayTerms', 'Laptop', 'Shift', 'Cert', 'PerfRating',
        'Rating', 'Years', 'Salary', 'Bonus', 'Overhead', 'Efficiency%', 
        'Util%', 'TeamSize', 'Commute_KM', 'Tax_Est', 'Insurance', 
        '401k_Contrib', 'Equity_Units', 'Satisfaction'
    ];

    /** @Prop */ selection = { rows: [], cols: [], vals: [] };
    /** @Prop */ filters = {};
    /** @Prop */ activeFilterField = null;
    /** @Prop */ calculatedFields = [];
    /** @Prop */ savedConfigs = []; 
    /** @Prop */ dashboardTiles = []; 
    /** @Prop */ expandedPaths = new Set();
    /** @Prop */ searchQuery = "";
    /** @Prop */ modes = ['sum', 'avg', 'count', 'max'];

	/** @Prop */ searchTimer = null;

	/** @Prop @type { HTMLElement } */container;

	/** 
	 * @Controller 
	 * @Path controller/
	 * @type { PivotTableController } */ controller;

	stAfterInit(){
		this.controller.on('load', async () => {
			this.controller.obj = this;
			await sleepForSec(500);
			const blob = new Blob([dataHandlingWorker()], { type: 'application/javascript' });
			this.dashWorker = new Worker(URL.createObjectURL(blob));
			this.container = document.getElementsByClassName('bi-pivot-ui-container')[0];
			this.getData();
			this.initSidebar();
			this.controller.renderAll();
		});
	}

	getData(){

		for (let i = 0; i < 50000; i++) {
			const level = this.levels[i % this.levels.length];
			const dept = this.depts[i % this.depts.length];
			const levelMultiplier = (this.levels.indexOf(level) + 1) * 22000;
			const randomVariation = Math.floor(Math.random() * 15000);
			const salary = 35000 + levelMultiplier + randomVariation;			

			this.dataset.push({ 
				'__rowId': i + 1,
				'Dept': dept, 
				'Level': this.level, 
				'Region': this.regions[i % this.regions.length],
				'Gender': this.genders[i % this.genders.length],
				'Contract': this.contractTypes[i % this.contractTypes.length],
				'Office': this.officeTypes[i % this.officeTypes.length],
				'ProjStatus': this.projStatus[i % this.projStatus.length],
				'PayTerms': this.payTerms[i % this.payTerms.length],
				'Laptop': this.laptopTypes[i % this.laptopTypes.length],
				'Shift': this.shiftTypes[i % this.shiftTypes.length],
				'Cert': this.certs[i % this.certs.length],
				'PerfRating': this.performance[i % this.performance.length],
				'Rating': Math.floor(Math.random() * 5) + 1,
				'Years': Math.floor(Math.random() * 20) + 1,
				'Salary': salary, 
				'Bonus': Math.floor(salary * (Math.random() * 0.18)),
				'Overhead': Math.floor(Math.random() * 5000) + 2000,
				'Efficiency%': 60 + Math.floor(Math.random() * 40),
				'Util%': 50 + Math.floor(Math.random() * 50),
				'TeamSize': 2 + Math.floor(Math.random() * 12),
				'Commute_KM': this.officeTypes[i % 3] === "Remote" ? 0 : Math.floor(Math.random() * 45),
				'Tax_Est': Math.floor(salary * 0.22),
				'Insurance': 450 + Math.floor(Math.random() * 300),
				'401k_Contrib': Math.floor(salary * 0.05),
				'Equity_Units': (this.levels.indexOf(level) > 3) ? 1000 + Math.floor(Math.random() * 5000) : 0,
				'Satisfaction': Math.floor(Math.random() * 10) + 1
			});
		}

	}

    initSidebar() {

		const { filters, dataset } = this;
        const fieldList = this.container.querySelector('#source-fields');
        fieldList.innerHTML = '';
        
        [...this.baseFields, ...this.calculatedFields.map(cf => cf.name)].forEach(f => {
            const div = document.createElement('div');
            div.className = 'field-item' + (this.calculatedFields.find(c => c.name === f) ? ' calc-field' : '');
            div.textContent = f; 
            div.draggable = true;
            div.style.marginBottom = "8px";
            div.ondragstart = (e) => { 
                e.dataTransfer.setData("type", "field"); 
                e.dataTransfer.setData("text", f); 
            };
            fieldList.appendChild(div);
            if (filters[f] === undefined) filters[f] = [...new Set(dataset.map(item => item[f]))];
        });

        // Saved pivots section remains the same...
        const savedList = this.container.querySelector('#saved-pivots');
        savedList.innerHTML = '';
        this.savedConfigs.forEach((cfg, idx) => {
            const div = document.createElement('div');
            div.className = 'field-item saved-config';
            div.textContent = cfg.name; div.draggable = true;
            div.ondragstart = (e) => { e.dataTransfer.setData("type", "config"); e.dataTransfer.setData("index", idx); };
            savedList.appendChild(div);
        });

        // Initial check for the arrow
        setTimeout(() => this.controller.checkScroll(fieldList), 100);
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

    saveConfiguration() {
		const { selection, filters } = this;
        if (!selection.rows.length || !selection.vals.length) return alert("Empty Layout");
        const name = prompt("Name your layout:");
        if (name) {
            this.savedConfigs.push({
                name,
                selection: JSON.parse(JSON.stringify(selection)),
                filters: JSON.parse(JSON.stringify(filters)),
                heatmap: document.getElementById('heatmap-check').checked,
                showAllRows: document.getElementById('show-all-rows-check').checked
            });
            this.initSidebar();
        }
    }

	setWorkerListiner(){
		const self = this;
		this.dashWorker.onmessage = function(e) {
			const { root, cols, tileIndex, cfg } = e.data;
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
		};
	}

    addCalculatedField() {
        const n = this.container.querySelector('#calc-name').value; const f = this.container.querySelector('#calc-formula').value;
        if (n && f) { this.calculatedFields.push({ name: n, formula: f }); this.container.querySelector('#calc-modal').style.display='none'; this.initSidebar(); }
    }

    evalFormula(item, formula) {
        let f = formula; this.baseFields.forEach(k => f = f.replace(new RegExp(k, 'g'), item[k] || 0));
        try { return eval(f); } catch { return 0; }
    }

    closeAllModals() { 
        this.container.querySelector('#calc-modal').style.display='none'; 
        this.container.querySelector('#filter-modal').style.display='none';
    }

    handleSearch(v) {
        clearTimeout(this.searchTimer);
        this.searchTimer = setTimeout(() => {
            searchQuery = v.toLowerCase(); 
            if (!searchQuery) this.expandedPaths.clear(); 
            this.renderAll();
        }, 250);
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
                row.push(Math.round(this.getVal(node.values[k], v.mode)));
            }));
            selection.vals.forEach(v => {
                row.push(Math.round(getVal(node.values[`TOTAL_${v.field}`], v.mode)));
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
            this.expandedPaths.clear();
            this.searchQuery = "";
            this.container.querySelector('#global-search').value = "";
            this.container.querySelector('#show-all-rows-check').checked = false;
            this.baseFields.forEach(f => {
                this.filters[f] = [...new Set(this.dataset.map(item => item[f]))];
            });
            this.renderAll();
        }
    }

}



function dataHandlingWorker() {

	return `
			self.onmessage = function(e) {
				const { dataset, cfg, searchQuery, calculatedFields, tileIndex } = e.data;
				const { selection: sel, filters: fltrs, showAllRows } = cfg;
				
				const root = { children: {}, values: {}, label: 'Grand Total', depth: -1 };
				const allCols = new Set();
				const effectiveRows = showAllRows ? [...sel.rows, '__rowId'] : sel.rows;
	
				const filterSets = {};
				for (let f in fltrs) filterSets[f] = new Set(fltrs[f]);
	
				dataset.forEach(item => {

					let skip = false;
					for (let f in filterSets) {
						if (!filterSets[f].has(item[f])) { skip = true; break; }
					}
					if (skip) return;
					
					if (searchQuery) {
						if (!sel.rows.some(f => String(item[f]).toLowerCase().includes(searchQuery))) return;
					}
	
					const cKey = sel.cols.length > 0 ? sel.cols.map(f => item[f]).join(' | ') : "Value";
					allCols.add(cKey);
	
					const update = (node, key) => {
						sel.vals.forEach(v => {
							const k = \`\${key}_\${v.field}\`;
							if (!node.values[k]) node.values[k] = { sum: 0, count: 0, max: -Infinity };
							
							let val = item[v.field];
							const calc = calculatedFields.find(c => c.name === v.field);
							if (calc) {
								let f = calc.formula;
								for (let prop in item) { f = f.replace(new RegExp(prop, 'g'), item[prop] || 0); }
								try { val = eval(f); } catch { val = 0; }
							}
	
							const nVal = Number(val) || 0;
							node.values[k].sum += nVal; 
							node.values[k].count += 1;
							node.values[k].max = Math.max(node.values[k].max, nVal);
						});
					};
	
					update(root, cKey);  update(root, 'TOTAL');
					
					let curr = root;
					effectiveRows.forEach((f, i) => {
						const val = item[f];
						if (!curr.children[val]) curr.children[val] = { children: {}, values: {}, depth: i };
						curr = curr.children[val]; 
						update(curr, cKey); 
						update(curr, 'TOTAL');
					});
				});
	
				self.postMessage({ root, cols: Array.from(allCols).sort(), tileIndex,  cfg });
			};
		`;
}