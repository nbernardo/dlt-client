import { StillAppMixin } from "../@still/component/super/AppMixin.js";
import { StillHTTPClient } from "../@still/helper/http.js";
import { Components } from "../@still/setup/components.js";
import { AppTemplate } from "./app-template.js";
import { Workspace } from "../app/components/workspace/Workspace.js";
import { Login } from "../app/components/auth/Login.js";
import { UserService } from "../app/services/UserService.js";

export class StillAppSetup extends StillAppMixin(Components) {

    constructor() {
        super();
        this.setHomeComponent(Workspace);
        StillHTTPClient.setBaseUrl('http://localhost:8000');
        //StillHTTPClient.setBaseUrl('https://dlt-client.onrender.com');

        this.addPrefetch({
            component: '@codemirror/CodeMiror',
            assets: [
                "v5.65.19/lib/codemirror.js",
                "v5.65.19/mode/python/python.js",
                "v5.65.19/mode/sql/sql.js",
                "v5.65.19/lib/codemirror.css",
                "v5.65.19/theme/monokai.css"
            ]
        });

        this.addPrefetch({
            component: '@treeview/StillTreeView',
            assets: ["tree-view.css"]
        });
        this.runPrefetch();

    }

    async init() {
        if((await UserService.isAuthenticated()))
            this.setAuthN(true);

        return this.isAuthN() ? await AppTemplate.newApp() : new Login();
    }

}
