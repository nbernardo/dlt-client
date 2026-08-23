import { $still } from "../../../../@still/component/manager/registror.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { switchActiveTab } from "../../../util/tabs.js";
import { BIService } from "../../dataviz/services/BIService.js";
import { DGServiceController } from "../../governance/controller/DGServiceController.js";
import { Workspace } from "../../workspace/Workspace.js";
import { ModelDeclarationController } from "../controller/ModelDeclarationController.js";

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

  /** @Prop @type { String } */ selectedDW;

  /** @type { Workspace } */ $parent;

  async stBeforeInit(){
    await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
  }

  async stOnRender(){ 
    await this.$parent.controller.loadMonacoEditorDependencies(); 
  }

  async stAfterInit() {
    this.controller.on('load', async () => {
      this.controller.obj = this;
      await this.controller.initEditor();
    });
  }

  switchTab(el) { 
    switchActiveTab(this, null, el); 
  }

  /** @returns { HTMLElement } */ $ = (ref) => this.container.querySelector(ref);
  /** @returns { HTMLElement } */ $$ = (ref) => this.container.querySelectorAll(ref);

  async loadTablesByPipeline(ppline){
	const pplinePath = ppline.split('.');
	const { fields, tables } = await this.serviceController.loadTablesByPipeline(pplinePath.slice(0,2).join('.'));
	this.selectedDW = ppline; 
	const fieldsMap = fields.reduce((acc, { table, name: fieldName }) => {
		if(!acc[table]) acc[table] = [];
		acc[table].push(fieldName)
		return acc
	}, {})
	this.controller.schema = fieldsMap;
  }

  async saveModel(){
	const url = `/declaration/model/${(await BIService.getNamespace())}`;
	const model = this.controller.editor.getValue(), modelQuery = this.sqlOutput.textContent;
	const result = await $still.HTTPClient.post(url, JSON.stringify({ model, modelQuery, dw: this.selectedDW }), HTTPHeaders.JSON);
	console.log(`Saving declaration response: `, result);

  }

}