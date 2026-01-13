import { destnationTemplatesMap } from "../../../assets/dlt-code-template/destination/known-destinations.js";
import { WorkSpaceController } from "../../../controller/WorkSpaceController.js";
import { UserService } from "../../../services/UserService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { AbstractNode } from "../abstract/AbstractNode.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { loadTemplate } from "../util/codeTemplateUtil.js";

/** @implements { NodeTypeInterface } */
export class DLTCodeOutput extends AbstractNode {

	isPublic = true;

	/** @Prop */ nodeId;
	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 0;
	/** @Prop */ aiGenerated;
	/** @Prop */ importFields;

	/** @Prop */ label = ' Out - DLT code';
	/** @Prop */ showEditor = false;

	//To trace to previous code template when changing one
	/** @Prop */ prevSelectedTemplate = null;

	/** @type { Workspace } */ $parent;

	/** @Prop */ codeEditor;
	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ codeContent = '';
	/** @Prop */ codeInitComment = '# Select a code template or Type your DLT python script code bellow'

	/** @Prop */ templateMap = {
		bigquery_tmpl: 'BigQuery',
		databricks_tmpl: 'Databricks',
		undefined: '',
	}

	/** @type { State } */
	selectedTemplate = '';
	templateName = '';

	/** @Prop */ importData;

	stOnRender(data) {
		const { nodeId, aiGenerated } = data;
		this.importData = data;
		this.nodeId = nodeId;
		this.$parent.controller.loadMonacoEditorDependencies();
		this.aiGenerated = aiGenerated;
		this.templateName = '';
		if(aiGenerated){
			if(![null, undefined].includes(data.codeTemplate))
				this.importData = { ...this.importData , template: data.codeTemplate }
		}
	}

	async stAfterInit() {

		const container = document
			.querySelector(`.${this.cmpInternalId} .code-editor-placeholder`);
		
		const codeSuggestions = [];
		await WorkspaceService.listSecrets(1, ({secretNames} = { secretNames: [] }) => {
			for(const secretName of secretNames){
				//CodeEditorUtil.pythonSuggestions.push(
				codeSuggestions.push(
					{
						label: secretName,
						kind: monaco.languages.CompletionItemKind.Snippet,
						insertText: secretName,
						insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet
					}
				);
			}
		});

		this.codeEditor = this.$parent.controller.loadMonacoEditor(
			container, {
				lang: 'python',
				theme: 'vs-dark',
				suggestions: codeSuggestions/*CodeEditorUtil.pythonSuggestions*/,
				suggestionType: 'secret',
				fontSize: 14
			}
		);

		this.codeEditor.onDidChangeModelContent(() => this.codeContent = this.codeEditor.getValue());
		this.onTemplateSelect();

		if(this.aiGenerated){
			if(this.importData.template.toLowerCase() in destnationTemplatesMap)
				this.selectedTemplate = destnationTemplatesMap[this.importData.template.toLowerCase()];
		}

		if (this.importData.isImport) {
			this.notifyReadiness();
			this.templateName = ` - <b>${this.templateMap[this.importData.templateName]}</b>`;
			this.codeEditor.setValue(this.importData.dltCode)
		}
	}

	async openEditor() {
		this.showEditor = !this.showEditor;
		this.showTemplateList = this.showEditor;
		WorkSpaceController.getNode(this.nodeId).data['namespace'] = await UserService.getNamespace();
		if (this.codeContent.length > 0) this.codeEditor.setValue(this.codeContent);
		else this.codeEditor.setValue(this.codeInitComment);
	}

	onTemplateSelect() {
		this.selectedTemplate.onChange(async templateName => {
			
			if(templateName === this.prevSelectedTemplate || templateName === 'dlt-code') return;
			
			const self = this;
			const prevCodeExists = WorkSpaceController.getNode(this.nodeId).data['dltCode'];
			if(prevCodeExists){
				this.$parent.controller.showDialog(
					`By changing the code template you'll lose any changes you might've done in the code.`, 
					{
						title: 'Changing the code!',
						onConfirm: async () => await handleTemplateSelection(),
						onCancel: () => self.selectedTemplate = this.prevSelectedTemplate,
					}
				)
			}else
				await handleTemplateSelection();
			
			async function handleTemplateSelection(){

				self.prevSelectedTemplate = templateName;
				let code = self.codeInitComment;
				if (templateName != '') {
					code = await loadTemplate(templateName, 'dest');
					self.templateName = ` - <b>${self.templateMap[templateName]}</b>`;
					self.codeContent = code;
					WorkSpaceController.getNode(self.nodeId).data['templateName'] = templateName;
					WorkSpaceController.getNode(self.nodeId).data['dltCode'] = code;
				}
				else
					self.templateName = '';
				self.codeEditor.setValue(code);
			}
		});
	}

	async getCode() {
		this.showTemplateList = true;
		WorkSpaceController.getNode(this.nodeId).data['dltCode'] = this.codeContent;
	}

	onOutputConnection(){
		DLTCodeOutput.handleOutputConnection(this);
		return { nodeCount: this.nodeCount.value };
	}

	/** @param { InputConnectionType<{}> } param0 */
	onInputConnection({ type, data }){
		DLTCodeOutput.handleInputConnection(this, data, type);
	}
	
}