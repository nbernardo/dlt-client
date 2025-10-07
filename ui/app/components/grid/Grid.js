import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";

export class Grid extends ViewComponent {

	isPublic = true;

	/** @Prop */ sortDirection = {};

	/** @Prop */ draggedColumn = null;

	/** @Prop */ isResizing = false;

	/** @Prop */ currentColumn = null;

	/** @Prop */ startX = 0;

	/** @Prop */ startWidth = 0;

	/** @Prop @type { HTMLDivElement } */ mainContainer;

	/** @Prop */ dataTable;

	/** @Prop */ fields = [];

	/** @Prop */ data = [];

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	async stBeforeInit(){
		await Assets.import({ path: '/app/assets/css/grid.css' });
	}

	stOnRender({ fields, data }){
		this.fields = fields, this.data = data;
	}

	stAfterInit() {			
		this.mainContainer = document.querySelector('#'+this.uniqueId);		
		this.loadGrid();
		this.dataTableHandlingSetup();
	}

	dataTableHandlingSetup() {

		this.dataTable = this.mainContainer.querySelector('.dataTable');
		const selfContainer = this.mainContainer;

		this.mainContainer.querySelector('.search-box').addEventListener('keyup', function () {
			const searchValue = this.value.toLowerCase();
			const rows = selfContainer.querySelectorAll(`.dataTable tbody tr`);

			for (let i = 0; i < rows.length; i++) {
				const text = rows[i].textContent.toLowerCase();
				rows[i].style.display = text.includes(searchValue) ? '' : 'none';
			}
		});

		this.columnSortingHandle();
		this.columnDraggingHandle();
		this.resizeColumnHandle();
	}

	columnSortingHandle() {
		const obj = this;
		const headers = this.mainContainer.querySelectorAll('th');
		for (let i = 0; i < headers.length; i++) {
			headers[i].setAttribute('data-column', i);
			headers[i].onclick = function (e) {
				if (e.target.classList.contains('resizer')) return;
				let columnIndex = parseInt(this.getAttribute('data-column'));
				obj.sortTable(columnIndex);
			};
		}
	}

	sortTable(columnIndex) {
		const tbody = this.dataTable.querySelector('tbody');
		const rows = Array.from(tbody.querySelectorAll('tr'));
		const headers = this.dataTable.querySelectorAll('th');
		const currentHeader = headers[columnIndex];

		this.sortDirection[columnIndex] = !this.sortDirection[columnIndex];
		const ascending = this.sortDirection[columnIndex];

		for (var i = 0; i < headers.length; i++) {
			headers[i].querySelector('.sort-indicator').textContent = '⇅';
		}

		currentHeader.querySelector('.sort-indicator').textContent = ascending ? '⇈' : '⇊';

		rows.sort(function (a, b) {
			const aValue = a.children[columnIndex].textContent.trim();
			const bValue = b.children[columnIndex].textContent.trim();

			if (aValue.includes('$')) {
				aValue = parseInt(aValue.replace(/[$,]/g, ''));
				bValue = parseInt(bValue.replace(/[$,]/g, ''));
			}

			if (aValue < bValue) return ascending ? -1 : 1;
			if (aValue > bValue) return ascending ? 1 : -1;
			return 0;
		});

		for (let i = 0; i < rows.length; i++) {
			tbody.appendChild(rows[i]);
		}
	}

	loadGrid(){
		this.loadHeader();
		this.loadData();
	}

	setGridData(fields, data){
		if(fields != null && fields != undefined)
			this.fields = fields
		this.data = data;
		return this;
	}

	loadHeader(){
		const headers = this.fields.map(field => `
			<th draggable="true">${field}<span class="sort-indicator">⇅</span>
				<div class="resizer"></div>
			</th>
		`).join('');
		this.mainContainer.querySelector('.header-fields').innerHTML = headers;
	}

	loadData(){

        const tableBody = this.data.map(row =>
            `<tr>${row.map(fieldVal => `<td class="right">${fieldVal}</td>`).join('')}</tr>`
        ).join('');

		this.mainContainer.querySelector('tbody').innerHTML = tableBody;

	}

	columnDraggingHandle() {

		const headers = this.dataTable.querySelectorAll('th');
		const self = this;

		for (let i = 0; i < headers.length; i++) {
			headers[i].ondragstart = function (e) {
				if (e.target.classList.contains('resizer')) {
					e.preventDefault();
					return;
				}
				self.draggedColumn = parseInt(this.getAttribute('data-column'));
				this.classList.add('dragging');
			};

			headers[i].ondragend = () => this.classList.remove('dragging');
			headers[i].ondragover = (e) => e.preventDefault();

			const obj = this;
			headers[i].ondrop = function (e) {
				e.preventDefault();
				let targetColumn = parseInt(this.getAttribute('data-column'));
				if (obj.draggedColumn !== null && obj.draggedColumn !== targetColumn) {
					obj.moveColumn(obj.draggedColumn, targetColumn);
					obj.updateColumnIndices();
				}
			};

		}

	}

	moveColumn(fromIndex, toIndex) {
		const headers = this.dataTable.querySelectorAll('th');
		const rows = this.dataTable.querySelectorAll('tbody tr');

		const headerRow = this.dataTable.querySelector('thead tr');
		const draggedHeader = headers[fromIndex],  targetHeader = headers[toIndex];

		if (fromIndex < toIndex) {
			headerRow.insertBefore(draggedHeader, targetHeader.nextSibling);
		} else headerRow.insertBefore(draggedHeader, targetHeader);
		

		for (let i = 0; i < rows.length; i++) {
			let cells = rows[i].children;
			let draggedCell = cells[fromIndex], targetCell = cells[toIndex];

			if (fromIndex < toIndex) {
				rows[i].insertBefore(draggedCell, targetCell.nextSibling);
			} else {
				rows[i].insertBefore(draggedCell, targetCell);
			}
		}
	}

	updateColumnIndices() {
		const headers = this.mainContainer.querySelectorAll('th');
		for (var i = 0; i < headers.length; i++) {
			headers[i].setAttribute('data-column', i);
		}
	}

	resizeColumnHandle() {
		const resizers = this.mainContainer.querySelectorAll('.resizer');
		const self = this;
		for (let i = 0; i < resizers.length; i++) {
			resizers[i].addEventListener('mousedown', function (e) {
				self.isResizing = true;
				self.currentColumn = e.target.parentElement;
				self.startX = e.clientX;
				self.startWidth = parseInt(getComputedStyle(self.currentColumn).width, 10);

				self.mainContainer.addEventListener('mousemove', self.doResize);
				self.mainContainer.addEventListener('mouseup', self.stopResize);
				e.preventDefault();
				e.stopPropagation();
			});
		}
	}

	doResize(e) {
		if (!this.isResizing) return;
		const width = Math.max(80, this.startWidth + e.clientX - this.startX);
		this.currentColumn.style.width = width + 'px';
		this.currentColumn.style.minWidth = width + 'px';
	}

	stopResize() {
		const self = this;
		this.isResizing = false;
		this.currentColumn = null;
		this.mainContainer.removeEventListener('mousemove', this.doResize);
		this.mainContainer.removeEventListener('mouseup', this.stopResize);
	}
}