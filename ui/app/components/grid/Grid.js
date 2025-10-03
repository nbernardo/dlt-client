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

	/** @Prop */ mainContainer;

	/** @Prop */ dataTable;

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	async stBeforeInit(){
		await Assets.import({ path: '/app/assets/css/grid.css' });
	}

	stAfterInit() {
		this.dataTableHandlingSetup();
	}

	dataTableHandlingSetup() {

		this.mainContainer = document.querySelector('#'+this.uniqueId);
		this.dataTable = this.mainContainer.querySelector('#dataTable');
		const selfContainer = this.mainContainer;

		this.mainContainer.querySelector('.search-box').addEventListener('keyup', function () {
			const searchValue = this.value.toLowerCase();
			const rows = selfContainer.querySelectorAll(`#dataTable tbody tr`);

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
			headers[i].addEventListener('click', function (e) {
				if (e.target.classList.contains('resizer')) return;
				let columnIndex = parseInt(this.getAttribute('data-column'));
				obj.sortTable(columnIndex);
			});
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

	columnDraggingHandle() {

		const headers = this.dataTable.querySelectorAll('th');
		const self = this;

		for (let i = 0; i < headers.length; i++) {
			headers[i].addEventListener('dragstart', function (e) {
				if (e.target.classList.contains('resizer')) {
					e.preventDefault();
					return;
				}
				self.draggedColumn = parseInt(this.getAttribute('data-column'));
				this.classList.add('dragging');
			});

			headers[i].addEventListener('dragend', function () {
				this.classList.remove('dragging');
			});

			headers[i].addEventListener('dragover', function (e) {
				e.preventDefault();
			});

			const obj = this;
			headers[i].addEventListener('drop', function (e) {
				e.preventDefault();
				let targetColumn = parseInt(this.getAttribute('data-column'));
				if (obj.draggedColumn !== null && obj.draggedColumn !== targetColumn) {
					obj.moveColumn(obj.draggedColumn, targetColumn);
					obj.updateColumnIndices();
				}
			});
		}

	}

	moveColumn(fromIndex, toIndex) {
		const headers = this.dataTable.querySelectorAll('th');
		const rows = this.dataTable.querySelectorAll('tbody tr');

		const headerRow = this.dataTable.querySelector('thead tr');
		const draggedHeader = headers[fromIndex];
		const targetHeader = headers[toIndex];

		if (fromIndex < toIndex) {
			headerRow.insertBefore(draggedHeader, targetHeader.nextSibling);
		} else {
			headerRow.insertBefore(draggedHeader, targetHeader);
		}

		for (let i = 0; i < rows.length; i++) {
			let cells = rows[i].children;
			let draggedCell = cells[fromIndex];
			let targetCell = cells[toIndex];

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
		for (let i = 0; i < resizers.length; i++) {
			resizers[i].addEventListener('mousedown', function (e) {
				this.isResizing = true;
				this.currentColumn = e.target.parentElement;
				this.startX = e.clientX;
				this.startWidth = parseInt(getComputedStyle(this.currentColumn).width, 10);

				this.mainContainer.addEventListener('mousemove', doResize);
				this.mainContainer.addEventListener('mouseup', stopResize);
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
		this.isResizing = false;
		this.currentColumn = null;
		this.mainContainer.removeEventListener('mousemove', doResize);
		this.mainContainer.removeEventListener('mouseup', stopResize);
	}
}