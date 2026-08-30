import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { InputDropdown } from "../../../util/InputDropdownUtil.js";
import { ModelDeclarationController } from "../controller/ModelDeclarationController.js";
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
  /** @Prop @type { HTMLElement } */ rulesContainer;
  /** @Prop @type { HTMLElement } */ codeOutput;
  /** @Prop @type { HTMLElement } */ sampleDataInput;
  /** @Prop @type { HTMLElement } */ sampleInputWrap;
  /** @Prop @type { HTMLElement } */ quarantineList;
  /** @Prop @type { HTMLElement } */ quarantineCountBadge;
  /** @Prop @type { InputDropdown } */ tableFilter;
  /** @Prop @type { ModelDeclaration } */ modelDeclaration;

  async stBeforeInit() { await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' }); }

  async stAfterInit() {
    this.controller.on('load', async () => {
      this.controller.obj = this;
      await this.controller.initComponent();
    });
	this.tableFilter = InputDropdown.new({ 
	  inputSelector: '.quality-declaration-table', dataSource: ModelDeclarationController.get().schema, boundComponent: this,
	  onSelect: async (val) => {
	    this.controller.targetDataset = val;
		this.controller.renderRules();
	    this.controller.compileAll();
	  }
	});
  }

  /** Helper scope selectors */
  $ = (ref) => this.container.querySelector(ref);
  $$ = (ref) => this.container.querySelectorAll(ref);

  switchTab(el) {
    const tabName = el.dataset.tab;
    this.controller.setActiveTab(tabName, el);
  }
}