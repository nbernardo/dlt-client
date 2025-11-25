import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { Workspace } from "../../workspace/Workspace.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";

/** @implements { NodeTypeInterface } */
export class DLTCode extends ViewComponent {

	isPublic = true;

	/** @Prop */ nodeId;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;

	/** @Prop */ label = ' In - DLT code';
	/** @Prop */ showEditor = false;

	/** @type { Workspace } */ $parent;

	/** @Prop */ codeEditor;
	/** @Prop */ codeContent = '';

	stOnRender({ nodeId }){
		this.nodeId = nodeId;
		this.$parent.controller.loadMonacoEditorDependencies();
	}

	stAfterInit(){

		const container = document
			.querySelector(`.${this.cmpInternalId} .code-editor-placeholder`);

		this.codeEditor = this.$parent.controller.loadMonadoEditor(
			container, { lang: 'python', theme: 'vs-dark' }
		);

		this.codeEditor.onDidChangeModelContent(() => {
			this.codeContent = this.codeEditor.getValue();
		});

	}

	openEditor(){
		this.showEditor = !this.showEditor;
		if(this.codeContent.length > 0){
			this.codeEditor.setValue(this.codeContent);
		}
	}

}