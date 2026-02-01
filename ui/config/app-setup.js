import { StillAppMixin } from "../@still/component/super/AppMixin.js";
import { Components } from "../@still/setup/components.js";
import { AppTemplate } from "./app-template.js";
import { Workspace } from "../app/components/workspace/Workspace.js";
import { Login } from "../app/components/auth/Login.js";
import { UserService } from "../app/services/UserService.js";

export class StillAppSetup extends StillAppMixin(Components) {

    constructor() {
        super();
        this.setHomeComponent(Workspace);

        const isCloud = window.location.href.toString().startsWith('dlt-c.cloud') 
            || window.location.hostname.toString().startsWith('mvp2.e2e-data.com');

        if(isCloud) 
            this.cloudEnv();
        else 
            this.localEnv();
        
        this.prefetchComponent();
    }

    prefetchComponent(){
        this.addPrefetch({
            component: '@treeview/StillTreeView',
            assets: ["tree-view.css"]
        });
        this.addPrefetch({ component: 'LogQueryDisplay' })
        this.runPrefetch();
    }

    localEnv(){
        this.setConfigFile('dev.json');        
    }

    cloudEnv(){ /* Will use the default.json condigurations file */ }

    async init() {
        if((await UserService.isAuthenticated()))
            this.setAuthN(true);

        return this.isAuthN() ? await AppTemplate.newApp() : new Login();
    }

}