import { StillAppMixin } from "./@still/component/super/AppMixin.js";
import { Components } from "./@still/setup/components.js";
import { AppTemplate } from "./app-template.js";
import { Workspace } from "./app/components/workspace/Workspace.js";

export class StillAppSetup extends StillAppMixin(Components) {

    constructor() {
        super();
        this.setHomeComponent(Workspace);
    }

    async init() {
        return await AppTemplate.newApp();
    }

}
