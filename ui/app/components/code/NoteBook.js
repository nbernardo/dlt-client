import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State } from "../../../@still/component/type/ComponentType.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { NoteBookController } from "../../controller/NoteBookController.js";

export class NoteBook extends ViewComponent {

	isPublic = true;

	/** 
	 * @Inject
	 * @Path controller/
	 * @type { NoteBookController }
	 * */
	controller;

	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	/** @type { State<String> } */ openFile;

	/** @Prop */ newCellFileName;

	/** @Prop */ showNotebook = false;

	stAfterInit(){

		this.openFile.onChange(({fileName, code}) => {
			if(this.controller.filesOpened.has(fileName)){
				return AppTemplate.toast.error('Selected file ('+fileName+ ') is already opened in the notebook');
			}
			this.controller.filesOpened.add(fileName);
			this.controller.createCodeCell(code,fileName);
		});

		this.controller.notebookCellsContainer = document.getElementById(this.uniqueId);
		//this.controller.createCodeCell('print("Hello, world!")');
	}

	newCodeCell = () => this.controller.createCodeCell('');
	
	closeNoteBook = () => this.showNotebook = false;

}