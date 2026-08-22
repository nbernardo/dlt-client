import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { switchActiveTab } from "../../../util/tabs.js";
import { Workspace } from "../../workspace/Workspace.js";
import { ModelDeclarationController } from "../controller/ModelDeclarationController.js";

export class ModelDeclaration extends ViewComponent {

  isPublic = false;

  /**
   * @Controller @Path components/declaration/controller/
   * @type { ModelDeclarationController }
   */
  controller;

  /** @Prop @type { HTMLElement } */ yamlInput;

  /** @Prop @type { HTMLElement } */ sqlOutput;
  
  /** @Prop @type { HTMLElement } */ errorBox;

  /** @Prop @type { HTMLElement } */ container;

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

}