import { $still } from "../../@still/component/manager/registror.js";
import { BaseService } from "../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../@still/helper/http.js";
import { StillAppSetup } from "../../config/app-setup.js";
import { AIUtil } from "../util/AIUtil.js";


export class BIService extends BaseService {

    static dashboardData = {};
    static dashboardDataPointer = {};
    static pivotBaseFields = [];
    static dashboardChartsMap = new Set();
    static activePipeline = null;

    static setDashboardDataPointer(data){
        const pointerId = Date.now() + Math.random().toString().slice(2);
        BIService.dashboardData[pointerId] = data;
        return pointerId;
    }

    static getDashboardDataFromPointer = (pointerId) => BIService.dashboardData[pointerId];

    static assigneDataSourcePerTable(tables, dashboardName, pointerId){
        for(const table of tables)
            BIService.dashboardDataPointer[`${dashboardName}.${table}`] = pointerId;
    }

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
        const url = `/analytics/ppline/domains/catalog/${namespace}/${pipeline.split('.')[1]}/${pipeline.split('.')[0]}`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok){
            const result = await response.json();
            const rangeFieldsData = {};

            for(const itm of Object.entries(result.result.range_fields_data[0])){

                const minOrMax = itm[0].split('_')[0];
                const fieldName = itm[0].replace(/min_|max_/,'');
                const preValues = rangeFieldsData[fieldName] || {};

                rangeFieldsData[fieldName] = { ...preValues, [minOrMax]: String(itm[1]).includes('T') ? itm[1].split('T')[0] : itm[1] };
            }

            return { allFields: JSON.parse(result.result.all_fields), rangeFieldsData };
        }
        return [];
    }

    static async getModulesWhenOdoo() {
        const namespace = await BIService.getNamespace();
        const url = `/analytics/integration/odoomodules/${namespace}/${BIService.activePipeline}`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok)
            return (await response.json())?.result?.modules;
        return [];
    }

    static async getTablesWhenOdoo(moduleName) {
        const namespace = await BIService.getNamespace();
        const url = `/analytics/integration/odootables/${moduleName}/${namespace}/${BIService.activePipeline}`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok)
            return (await response.json())?.result?.modules;
        return [];
    }

    /** @returns { { result: { result } } } */
    static async sendAnalyticsRequest(fields, pipeline, dataRange) {
        let namespace = await BIService.getNamespace();
        
        const url = `/workspace/analytics/${namespace}/${pipeline}`;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ fields, dataRange }), HTTPHeaders.JSON);
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    /** @returns { { result: { result } } } */
    static async getAnalyticsRangeFields(fields, pipeline) {
        let namespace = await BIService.getNamespace();
        
        const url = `/workspace/analytics/rangefields/${namespace}/${pipeline}`;
        const response = await $still.HTTPClient.get(url);
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

    static async getAppPath(){
        let cssPathPrefix = '';
        if(StillAppSetup.config.get('runningOnOdoo'))
            cssPathPrefix = `${location.origin}/odoo-e2e-bi/static/src/dashboard-app`;
        return cssPathPrefix;
    }

}