import { $still } from "../../../../@still/component/manager/registror.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { State } from "../../../../@still/component/type/ComponentType.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { Components } from "../../../../@still/setup/components.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { switchActiveTab } from "../../../util/tabs.js";
import { BIService } from "../../dataviz/services/BIService.js";
import { DGServiceController } from "../../governance/controller/DGServiceController.js";
import { Workspace } from "../../workspace/Workspace.js";
import { ModelDeclarationController } from "../controller/ModelDeclarationController.js";
import { DataQualityDeclaration } from "../quality/DataQualityDeclaration.js";

export class ModelDeclaration extends ViewComponent {

  isPublic = false;

  /**
   * @Controller @Path components/declaration/controller/
   * @type { ModelDeclarationController }
   */
  controller;

  /** 
   * @Inject @Path components/governance/controller/
   * @type { DGServiceController }
   */
  serviceController;

  /** @Prop @type { HTMLElement } */ yamlInput;

  /** @Prop @type { HTMLElement } */ sqlOutput;
  
  /** @Prop @type { HTMLElement } */ errorBox;

  /** @Prop @type { HTMLElement } */ container;

  /** @Prop @type { String } */ selectedDW = '';

  /** @Prop @type { String } */ selectedSecred;

  /** @Prop @type { Boolean } */ loadingDQ = false;

  /** @Prop @type { DataQualityDeclaration } */ dataQualityInstance;

  /** @type { State<Array> } */ tableList;

  /** @type { Workspace } */ $parent;

  modelName;

  async stBeforeInit(){
    await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
  }

  async stOnRender(){ 
    await this.$parent.controller.loadMonacoEditorDependencies(); 
  }

  async stAfterInit() {
	this.tableList = [];
    this.controller.on('load', async () => {
      this.controller.obj = this;
      await this.controller.initEditor();
    });

	this.modelName.onChange(val => {
		if(this.controller.hashError === false && val.trim() !== '')
      		return document.querySelector(`.btn-primary.save-declaration`).disabled = false;
      	document.querySelector(`.btn-primary.save-declaration`).disabled = true;
	});
  }

  async switchTab(el, tab) { 
    switchActiveTab(this, tab, el);
	setTimeout(async () => {
		if(tab === 'quality-model'){
		  this.loadingDQ = true;
		  const { component: dQComponent, template: dQUI } = await Components.newView(DataQualityDeclaration, { modelDeclaration: this });
		  this.container.querySelector('#sec-quality-model-content').innerHTML = dQUI;
		  this.dataQualityInstance = dQComponent;
		  return document.querySelector('.model-declaration-section').classList.remove('model-declaration-stretch');
		}
		document.querySelector('.model-declaration-section').classList.add('model-declaration-stretch'); 
	})
  }

  /** @returns { HTMLElement } */ $ = (ref) => this.container.querySelector(ref);
  /** @returns { HTMLElement } */ $$ = (ref) => this.container.querySelectorAll(ref);

  async loadTablesByPipeline(ppline){
	this.loadingDQ = true;
	const pplinePath = ppline.split('.');
	const { fields, tables, secretName } = await this.serviceController.loadTablesByPipeline(pplinePath.slice(0,2).join('.'));
	this.selectedDW = ppline, this.selectedSecred = secretName; 
	const fieldsMap = fields.reduce((acc, { table, name: fieldName }) => {
		if(!acc[table]) acc[table] = [];
		acc[table].push(fieldName)
		return acc
	}, {});
	this.controller.schema = fieldsMap;
	this.tableList = tables;	
	if(this.dataQualityInstance) this.dataQualityInstance.updateDataSource(secretName);
	this.loadingDQ = false;
  }

  async saveModel(){
	const url = `/declaration/model/${(await BIService.getNamespace())}`;
	const [model, modelQuery, modelName] = [this.controller.editor.getValue(), this.sqlOutput.textContent, this.modelName.value];

	let result = await $still.HTTPClient.post(url, JSON.stringify({ model, modelName, modelQuery, dw: this.selectedDW }), HTTPHeaders.JSON);
	result = await result.json();
	if(result.result) AppTemplate.toast.success(`Model created successfully`);
	else{
		if(result.existing) AppTemplate.toast.warn(`There is already a model with name ${modelName} for the pipeline selected`, 10000);
		else AppTemplate.toast.error(`Erro while creating model "${modelName}" for the pipeline selcted`, 10000);
	}
  }

  async previewModel(){
	const content = {
		tableId: null, databaseParam: this.selectedDW, dbfile: this.currentDBFile, rawQuery: this.sqlOutput.textContent, queryTable: null, 
		tableMetadata: { dest_type: 'duckdb', ppline: this.selectedDW.split('.')[0]}, autoRun: true, rOnly: true
	};
	this.$parent.expandDataTableView(content);
	setTimeout(() => {
		document.querySelector(`.popup-windows-cmp-container`).style.background = 'rgba(240, 240, 240, .6)';
		this.controller.editor.updateOptions({ readOnly: true, readOnlyMessage: { value: 'Click "Unlock" to edit this file.' } });
	});
  }

}