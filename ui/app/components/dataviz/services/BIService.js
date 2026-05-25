import { $still } from "../../../../@still/component/manager/registror.js";
import { BaseService } from "../../../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { StillAppSetup } from "../../../../config/app-setup.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { AIUtil } from "../../../util/AIUtil.js";
import { DBDiagramController } from "../controllers/DBDiagramController.js";
import { CacheService } from "./CacheService.js";


export class BIService extends BaseService {

    static dashboardData = {};
    static dashboardDataPointer = {};
    static pivotBaseFields = [];
    static dashboardChartsMap = new Set();
    static activePipeline = null;
    static selectedConnection;

    static getDBDiagramContainer = () => DBDiagramController.fromContext().obj.container.querySelector('#mountNode');
    
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
        const url = `/analytics/dashboard/${(await BIService.getNamespace())}`;
        const response = await $still.HTTPClient.post(
            url, JSON.stringify({ charts, name, id }), 
            HTTPHeaders.JSON
        );
        if (response.ok) return true;
        return false;
    }

    static async saveChartConfig(config, pipeline, title, dataSource, chartId){

        const url = `/analytics/chart/${(await BIService.getNamespace())}`;
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
            const { UserUtil } = await import('../../auth/UserUtil.js');
            const { UserService } = await  import('../../../services/UserService.js');
            namespace = StillAppSetup.config.get('anonymousLogin') ? UserUtil.email : await UserService.getNamespace();
        }
        return namespace;
    }

    static async getDashboardDetails() {
        const url = '/analytics/ppline/domains/' + (await BIService.getNamespace());
        const response = await $still.HTTPClient.get(url);
        if (response.ok) return await response.json();
        return [];
    }

    static async getDWPipelines() {
        const url = '/analytics/ppline/dwh/' + (await BIService.getNamespace());
        const response = await $still.HTTPClient.get(url);
        if (response.ok) return await response.json();
        return [];
    }

    static async getStagedData() {
        const url = '/analytics/ppline/dwh/staged' + (await BIService.getNamespace());
        const response = await $still.HTTPClient.get(url);
        if (response.ok) return await response.json();
        return [];
    }

    static async getDomainPipelineFields(pipeline) {
        const namespace = await BIService.getNamespace();
        const url = `/analytics/ppline/domains/catalog/${namespace}/${pipeline.split('.')[1]}/${pipeline.split('.')[0]}`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok){
            const result = await response.json();
            const rangeFieldsData = {}, rengeFields = (result.result.range_fields_data[0] || {})
            for(const itm of Object.entries(rengeFields)){

                const minOrMax = itm[0].split('_')[0];
                const fieldName = itm[0].replace(/min_|max_/,'');
                const preValues = rangeFieldsData[fieldName] || {};

                rangeFieldsData[fieldName] = { ...preValues, [minOrMax]: String(itm[1]).includes('T') ? itm[1].split('T')[0] : itm[1] };
            }

            return { allFields: JSON.parse(result.result.all_fields), rangeFieldsData };
        }
        return [];
    }

    static async getModulesWhenOdoo(connectioName) {
        let url = `/analytics/integration/odoomodules/${(await BIService.getNamespace())}`;

        if(connectioName){
            var response = await $still.HTTPClient.post(url, JSON.stringify({ connectioName }), HTTPHeaders.JSON);
        }else
            var response = await $still.HTTPClient.get(`${url}/${BIService.activePipeline}`);

        if (response.ok)
            return (await response.json())?.result;
        return [];
    }

    static async getTablesWhenOdoo(moduleName) {
        const isCached = await CacheService.hasKey(moduleName);
        if(isCached)
            return await CacheService.get(moduleName);
        
        const namespace = await BIService.getNamespace();
        let url = `/analytics/integration/odootables/${moduleName}/${namespace}`;

        if(BIService.selectedConnection){
            var response = await $still.HTTPClient.post(url, JSON.stringify({ connectioName: BIService.selectedConnection }), HTTPHeaders.JSON);
        }else
            var response = await $still.HTTPClient.get(`${url}/${BIService.activePipeline}`);
      
        if (response.ok){
            const result = (await response.json())?.result;
            if(!result.error){
                await CacheService.add(moduleName, result);
                return result;
            }
        }
        return [];
    }

    /** @returns { { result: { result } } } */
    static async sendAnalyticsRequest(fields, pipeline, dataRange) {
        
        const url = `/workspace/analytics/${(await BIService.getNamespace())}/${pipeline}`;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ fields, dataRange }), HTTPHeaders.JSON);
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    /** @returns { { result: { result } } } */
    static async getAnalyticsRangeFields(fields, pipeline) {
        
        const url = `/workspace/analytics/rangefields/${(await BIService.getNamespace())}/${pipeline}`;
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

    /** @returns { { result: { result } } } */
    static async runSQLQuery(query) {
        const url = '/analytics/sql_query/' + (await BIService.getNamespace());

        const response = await $still.HTTPClient.post(url, JSON.stringify({ query, connectionName: BIService.selectedConnection }), HTTPHeaders.JSON);
        if(response.error) 
            return { result: response.result, error: true }
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

    static async listSecrets() {
        try {
            
            const response = await $still.HTTPClient.get('/secret/' + (await BIService.getNamespace()));    
            if (response.ok && !response.error){
                
                const snakeToCamel = (val='') => val.split('_').map(c => c.charAt(0).toUpperCase()+`${c.slice(1)}`).join(' ');

                let secretList = (await response.json()).result, secretAndServerList = [];
                if(Array.isArray(secretList?.secret_names?.db_secrets)){
                    const secretNames = [];
                    secretAndServerList = secretList.secret_names.db_secrets.map(secret => {
                        if(!secretList.secret_names.metadata[secret]) secretNames.push(secret);
                        return { name: secret, host: secretList.secret_names.metadata[secret] || 'None',id: secret };
                    });
                }
                
                const stagedData = secretList?.staged_data?.map(itm => ({ name: snakeToCamel(itm[0]), id: `${itm[0]}.${itm[1]}` })) || [];
                const result = (secretAndServerList || []).length > 0 ? secretAndServerList : [];
                return [...result, ...stagedData];
    
            } else {
                const result = await response.json();
                AppTemplate.toast.error(result.result);
            }

        } catch (error) {
            return [];
        }
    }

}