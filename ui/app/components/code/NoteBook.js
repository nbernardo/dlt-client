import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State } from "../../../@still/component/type/ComponentType.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { NoteBookController } from "../../controller/NoteBookController.js";
import { Workspace } from "../workspace/Workspace.js";

export class NoteBook extends ViewComponent {

	isPublic = true;

	/** 
	 * @Inject
	 * @Path controller/
	 * @type { NoteBookController }
	 * */
	controller;

	/** @Prop */ uniqueId = '_' + UUIDUtil.newId();
	/** @Prop */ newCellFileName;
	/** @Prop */ showNotebook = false;

	/** @type { State<String> } */ openFile;
	/** @type { Workspace } */ $parent;

	/** @Prop */ notebookContainer;
	/** @Prop */ monacoEditorWrap;
	/** @Prop */ showdownConverter;

	async stBeforeInit() {}

	async stOnRender(){}

	async stAfterInit() {

		this.notebookContainer = document.getElementById('notebook-container');
		this.monacoEditorWrap = document.querySelector('.monaco-editor-wrap');
		this.controller.on('load', () => {
			this.controller.noteBook = this;
			this.controller.userFolder = this.$parent.userEmail;
		});

		this.openFile.onChange(({fileName, code}) => this.controller.openFile(code, fileName));

		if (window.monaco) {
			window.monaco.languages.registerCompletionItemProvider('python', {
				provideCompletionItems: (model, position) => ({ suggestions: this.controller.pythonAutoCompletionSetup() })
			});
			this.notebookContainer.classList.remove('hidden');
			document.getElementById('loading-message').classList.add('hidden');
		}

		this.notebookContainer.addEventListener('click', (e) => {
			const cellElement = e.target.closest('[id^="cell-"]');
			if (!cellElement) return;

			const cellId = cellElement.id.replace('cell-', '');

			if (e.target.classList.contains('add-cell-above-btn')) {
				this.controller.addCell(window.monaco, cellId, 'above');
			} else if (e.target.classList.contains('add-cell-below-btn')) {
				this.controller.addCell(window.monaco, cellId, 'below');
			} else if (e.target.classList.contains('play-arrow-btn')) {
				return this.$parent.controller.showDialog(
					'Pipeline modification needs to be done through the diagram, not through code.',
					{ title: 'Pipeline alteration', type: 'ok' }
				);
				this.controller.runCell(cellId, e.target.getAttribute('filename'));
			} else if (e.target.classList.contains('delete-cell-btn')) {
				this.controller.deleteCell(cellId, e.target.getAttribute('filename'));
			} else if (e.target.classList.contains('minimize-cell-btn')) {
				this.controller.toggleMinimize(cellId);
			} else if (e.target.classList.contains('move-up-btn')) {
                this.controller.moveCell(cellId, 'up');
            } else if (e.target.classList.contains('move-down-btn')) {
                this.controller.moveCell(cellId, 'down');
            }
		});

		this.controller.dragSetup();

	}

	closeNoteBook(){
		this.$parent.showDrawFlow = true;
		this.showNotebook = false;
		this.controller.removeAllCells();		
	}

}