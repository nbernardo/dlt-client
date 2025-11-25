import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../../@still/component/type/ComponentType.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { Workspace } from "../../workspace/Workspace.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { loadTemplate } from "../util/codeTemplateUtil.js";

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
	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ codeContent = '';

	/** @Prop */ templateMap = {
		kafka_tmpl: 'Kafka',
		mongo_tmpl: 'MongoDB'
	}

	/** @type { State } */
	selectedTemplate = '';
	templateName = '';

	stOnRender({ nodeId }){
		this.nodeId = nodeId;
		this.$parent.controller.loadMonacoEditorDependencies();
	}

	async stAfterInit(){
		
		const container = document
			.querySelector(`.${this.cmpInternalId} .code-editor-placeholder`);

		this.codeEditor = this.$parent.controller.loadMonadoEditor(
			container, { lang: 'python', theme: 'vs-dark' }
		);

		this.codeEditor.onDidChangeModelContent(() => {
			this.codeContent = this.codeEditor.getValue();
		});

		this.onTemplateSelect();

	}

	openEditor(){
		this.showEditor = !this.showEditor;
		this.showTemplateList = this.showEditor;
		if(this.codeContent.length > 0){
			this.codeEditor.setValue(this.codeContent);
		}
	}

	onTemplateSelect(){

		this.selectedTemplate.onChange(async templateName => {
			
			let code = '', codeName = '';
			if(templateName != ''){
				code = await loadTemplate(templateName);
				codeName = this.templateMap[templateName];
				this.templateName = ` - <b>${this.templateMap[templateName]}</b>`;
				this.codeContent = code;
				WorkSpaceController.getNode(this.nodeId).data['dltCode'] = code;
			}
			else
				this.templateName = '';

			this.codeEditor.setValue(code);

		});

	}

	async getCode(){
		this.showTemplateList = true;
		WorkSpaceController.getNode(this.nodeId).data['dltCode'] = this.codeContent; 
	}

}