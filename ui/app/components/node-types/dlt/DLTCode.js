import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { State, STForm } from "../../../../@still/component/type/ComponentType.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { CodeEditorUtil } from "../../../util/CodeEditorUtil.js";
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

	/** @Prop */ importData;

	stOnRender(data) {
		const { nodeId } = data;
		this.importData = data;
		this.nodeId = nodeId;
		this.$parent.controller.loadMonacoEditorDependencies();
		this.templateName = '';
	}

	async stAfterInit() {

		const container = document
			.querySelector(`.${this.cmpInternalId} .code-editor-placeholder`);

		await WorkspaceService.listSecrets(1, ({secretNames} = { secretNames: [] }) => {
			for(const secretName of secretNames){
				CodeEditorUtil.pythonSuggestions.push(
					{
						label: secretName,
						kind: monaco.languages.CompletionItemKind.Keyword,
						insertText: secretName,
						documentation: '',
						insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
					}
				);
			}
		});

		this.codeEditor = this.$parent.controller.loadMonacoEditor(
			container, {
				lang: 'python',
				theme: 'vs-dark',
				suggestions: CodeEditorUtil.pythonSuggestions,
				suggestionType: 'secret'
			}
		);

		this.codeEditor.onDidChangeModelContent(() => {
			this.codeContent = this.codeEditor.getValue();
		});
		this.onTemplateSelect();

		if (this.importData.isImport) {
			this.templateName = ` - <b>${this.templateMap[this.importData.templateName]}</b>`;
			this.codeEditor.setValue(this.importData.dltCode)
		}
	}

	async openEditor() {
		this.showEditor = !this.showEditor;
		this.showTemplateList = this.showEditor;
		if (this.codeContent.length > 0)
			this.codeEditor.setValue(this.codeContent);
	}

	onTemplateSelect() {
		this.selectedTemplate.onChange(async templateName => {

			let code = '# Type your DLT python script code bellow';
			if (templateName != '') {
				code = await loadTemplate(templateName);
				this.templateName = ` - <b>${this.templateMap[templateName]}</b>`;
				this.codeContent = code;
				WorkSpaceController.getNode(this.nodeId).data['templateName'] = templateName;
				WorkSpaceController.getNode(this.nodeId).data['dltCode'] = code;
			}
			else
				this.templateName = '';
			this.codeEditor.setValue(code);

		});
	}

	async getCode() {
		this.showTemplateList = true;
		WorkSpaceController.getNode(this.nodeId).data['dltCode'] = this.codeContent;
	}

}