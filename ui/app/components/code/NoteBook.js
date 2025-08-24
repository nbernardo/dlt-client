import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { NoteBookController } from "../../controller/NoteBookController.js";

export class NoteBook extends ViewComponent {

	isPublic = true;

	/** 
	 * @Inject
	 * @Path controller/
	 * @type { NoteBookController }
	 * */
	controller;

	/** @Prop */
	uniqueId = '_'+UUIDUtil.newId();

	stAfterInit(){
		this.controller.notebookCellsContainer = document.getElementById(this.uniqueId);
		this.controller.createCodeCell('print("Hello, world!")');
	}

	newCodeCell(){
		this.controller.createCodeCell('');
	}

}