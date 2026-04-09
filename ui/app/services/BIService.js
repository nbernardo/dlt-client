import { $still } from "../../@still/component/manager/registror.js";
import { BaseService } from "../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../@still/helper/http.js";
import { StillAppSetup } from "../../config/app-setup.js";
import { AIUtil } from "../util/AIUtil.js";


export class BIService extends BaseService {

    static async saveDashboardConfig(charts, name, id){
        const namespace = await BIService.getNamespace();
        const url = `/analytics/dashboard/${namespace}`;
        const response = await $still.HTTPClient.post(
            url, JSON.stringify({ charts, name, id }), 
            HTTPHeaders.JSON
        );
        if (response.ok){
            const result = await response.json();
            return true;
        }
        return false;
    }

    static async saveChartConfig(config, pipeline, title, dataSource, chartId){

        const namespace = await BIService.getNamespace();
        const url = `/analytics/chart/${namespace}`;
        const response = await $still.HTTPClient.post(
            url, JSON.stringify({ config, context: pipeline, title, dataSource, chartId }), 
            HTTPHeaders.JSON
        );
        if (response.ok){
            const result = await response.json();
            return true;
        }
        return false;
    }

    static async getNamespace(){
        let namespace = StillAppSetup.config.get('clientNamespace');
        if(!StillAppSetup.config.get('runningOnOdoo')){
            const { UserUtil } = await import('../components/auth/UserUtil.js');
            const { UserService } = await  import('../services/UserService.js');
            namespace = StillAppSetup.config.get('anonymousLogin') ? UserUtil.email : await UserService.getNamespace();
        }
        return namespace;
    }

    static async getDashboardDetails() {
        const namespace = await BIService.getNamespace();
        const url = '/analytics/ppline/domains/' + namespace;
        const response = await $still.HTTPClient.get(url);
        if (response.ok)
            return await response.json();
        return [];
    }

    static async getDomainPipelineFields(pipeline) {
        const namespace = await BIService.getNamespace();
        const url = `/analytics/ppline/domains/catalog/${namespace}/${pipeline.split('.')[1]}`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok){
            const result = await response.json();
            return JSON.parse(result.result);
        }
        return [];
    }

    /** @returns { { result: { result } } } */
    static async sendAnalyticsRequest(fields, pipeline) {
        let namespace = await BIService.getNamespace();
        
        const url = `/workspace/analytics/${namespace}/${pipeline}`;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ fields }), HTTPHeaders.JSON);
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    /** @returns { { result: { result } } } */
    static async sendDataQueryAgentMessage(message) {
        const agentFlow = AIUtil.aiAgentFlow, namespace = await BIService.getNamespace();
        const url = '/workcpace/agent/' + namespace;

        const response = await $still.HTTPClient.post(url, JSON.stringify({ message, agentFlow }), HTTPHeaders.JSON);
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

}