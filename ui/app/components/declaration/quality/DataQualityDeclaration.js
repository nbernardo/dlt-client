import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { InputDropdown } from "../../../util/InputDropdownUtil.js";
import { BIService } from "../../dataviz/services/BIService.js";
import { QualityDeclarationController } from "../controller/QualityDeclarationController.js";
import { ModelDeclaration } from "../model/ModelDeclaration.js";

export class DataQualityDeclaration extends ViewComponent {

  /**
   * @Controller @Path components/declaration/controller/
   * @type { QualityDeclarationController }
   */
  controller;

  /** @Prop @type { HTMLElement } */ container;
  /** @Prop @type { HTMLElement } */ targetDatasetInput;
  /** @Prop @type { HTMLElement } */ primaryKeyInput;
  /** @Prop @type { HTMLElement } */ rulesContainer;
  /** @Prop @type { HTMLElement } */ codeOutput;
  /** @Prop @type { HTMLElement } */ sampleDataInput;
  /** @Prop @type { HTMLElement } */ sampleInputWrap;
  /** @Prop @type { HTMLElement } */ quarantineList;
  /** @Prop @type { HTMLElement } */ quarantineCountBadge;
  /** @Prop @type { InputDropdown } */ tableFilter;
  /** @Prop @type { ModelDeclaration } */ modelDeclaration;
  /** @Prop @type { Object } */ databaseSchema = {};

  async stBeforeInit() { await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' }); }

  async stOnRender({ modelDeclaration }){ this.modelDeclaration = modelDeclaration }

  async stAfterInit() {
    this.controller.on('load', async () => {
      this.controller.obj = this;
      this.controller.rules = [];
      await this.controller.initComponent();
    });

    this.tableFilter = InputDropdown.new({ 
      inputSelector: '.quality-declaration-table', dataSource: [], boundComponent: this,
      onSelect: async (val) => {
        this.controller.targetDataset = val;
        this.controller.renderRules();
        this.controller.compileAll();
      }
    });
    this.updateDataSource(this.modelDeclaration.selectedSecred);
    this.modelDeclaration.loadingDQ = false;
  }

  /** Helper scope selectors */
  $ = (ref) => this.container.querySelector(ref);
  $$ = (ref) => this.container.querySelectorAll(ref);

  switchTab(el) {
    const tabName = el.dataset.tab;
    this.controller.setActiveTab(tabName, el);
  }

  async updateDataSource(secretName){
    if(!secretName)
      return this.tableFilter.setDataSource([]);
    const data = await WorkspaceService.getConnectionDetails(secretName, true);
    this.databaseSchema = data.tables;
    this.tableFilter.setDataSource(Object.keys(data.tables));
  }

  async saveModel(){
    const url = `/declaration/model/${(await BIService.getNamespace())}`;
    const definition = this.controller.compileJSON();
    const qualityCheckQuery = this.controller.compileQuarantineSQL();
    const [model, modelQuery, modelName] = [definition, qualityCheckQuery, this.tableFilter.getValue()];

    const payload = { model, modelName, modelQuery, dw: this.modelDeclaration.selectedDW, quality: true };
    let result = await $still.HTTPClient.post(url, JSON.stringify(payload), HTTPHeaders.JSON);
    result = await result.json();
    if(result.result) AppTemplate.toast.success(`Data quality rules created successfully`);
    else{
      if(result.existing) AppTemplate.toast.warn(`There is already a Data quality rules with name ${modelName} for the pipeline data source`, 10000);
      else AppTemplate.toast.error(`Error while creating Data quality rules "${modelName}" for the pipeline data source`, 10000);
    }
  }
}