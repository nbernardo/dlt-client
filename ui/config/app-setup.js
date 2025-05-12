import { StillAppMixin } from "../@still/component/super/AppMixin.js";
import { StillHTTPClient } from "../@still/helper/http.js";
import { Components } from "../@still/setup/components.js";
import { AppTemplate } from "./app-template.js";
import { Workspace } from "../app/components/workspace/Workspace.js";

export class StillAppSetup extends StillAppMixin(Components) {

    constructor() {
        super();
        this.setHomeComponent(Workspace);
        StillHTTPClient.setBaseUrl('http://localhost:5000');

        this.addPrefetch({
            component: '@codemirror/CodeMiror',
            assets: [
                "v5.65.19/lib/codemirror.js",
                "v5.65.19/mode/python/python.js",
                "v5.65.19/lib/codemirror.css",
                "v5.65.19/theme/monokai.css"
            ]
        });
        this.runPrefetch();

    }

    async init() {
        return await AppTemplate.newApp();
    }

}
