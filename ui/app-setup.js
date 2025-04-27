import { StillAppMixin } from "./@still/component/super/AppMixin.js";
import { StillHTTPClient } from "./@still/helper/http.js";
import { Components } from "./@still/setup/components.js";
import { AppTemplate } from "./app-template.js";
import { Workspace } from "./app/components/workspace/Workspace.js";

export class StillAppSetup extends StillAppMixin(Components) {

    constructor() {
        super();
        this.setHomeComponent(Workspace);
        StillHTTPClient.setBaseUrl('http://localhost:5000');
    }

    async init() {
        return await AppTemplate.newApp();
    }

}
