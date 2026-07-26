import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../@still/helper/http.js";
import { AppTemplate } from "../../config/app-template.js";
import { DataCatalogUI } from "../components/data-catalog/DataCatalogUI.js";
import { BIService } from "../components/dataviz/services/BIService.js";
import { WorkSpaceController } from "../controller/WorkSpaceController.js";
import { StringUtil } from "../util/StringUtil.js";
import { UserService } from "./UserService.js";

export class PipelineService extends BaseService {

    table = new ServiceEvent([]);
    pipelineRun = new ServiceEvent({ fails: {}, success: {} });
    static tableListStore;
    static jsonHeaders = { 'Content-Type': 'application/json' };
    static pipelineSourcesAndSestinationsMap = {};
    static sqlEditorDestType = null;
    static sqlEditorDestSecretName = null;
    static pipelineReferencedSecrets = null;
    static pipelineDestinationConfig = null;
    static pipelineDestinationDB = null;
    static storePipelineShortList = [];
    static storePipelineTriggers = [];

    async createOrUpdatePipeline(content = null, update = false, actionType = '') {

        const payload = content || {};
        payload.usedExistingDW = WorkSpaceController.usedExistingDW;
        payload.pipelinePlanId = WorkSpaceController.pipelinePlanId;

        const headers = PipelineService.jsonHeaders;
        if(update === true){
            return $still.HTTPClient.put('/pipeline/create', JSON.stringify(payload), { headers });
        }else{
            return $still.HTTPClient.post('/pipeline/create', JSON.stringify({ ...payload, actionType }), { headers });
        }
    }

    static async getPipelinesNames(describe){
        const namespace = await UserService.getNamespace();
        const url = '/workspace/pipelines/list/' + namespace;
        const response = await $still.HTTPClient.post(url, null, {
            headers: { 'Content-Type': 'application/json' }
        });
        const { db_path: _, pipeline_sources_and_destinations, ...tables } = await response.json();
        PipelineService.tableListStore = tables;

        if(describe) 
            return Object.keys(tables).map(name => ({ name, lbl: StringUtil.snakeToCamel(name) }));
        return Object.keys(tables).map(name => ({name}));
    }

    static async getPipelinesShortList(){
        const namespace = await UserService.getNamespace();
        const url = '/ppline/shortlist/' + namespace;
        const response = await $still.HTTPClient.get(url);
        const { result } = await response.json();

        const getRelatedParent = (dsName) => String(dsName).includes('.') ? dsName.split('.')[0] : null;
        PipelineService.storePipelineShortList = result.result.map(it => ({ 
            name: it.pipeline, lbl: StringUtil.snakeToCamel(it.pipeline), relateTo: getRelatedParent(it.dataset_name) 
        }));

        return PipelineService.storePipelineShortList;
    }

    static async getPipelinesRunHistory(status){
        const namespace = await UserService.getNamespace();
        const url = `/ppline/run/history/${namespace}${status ? `/${status}` : ''}`;
        const response = await $still.HTTPClient.get(url);
        const { result } = await response.json();
        return result.result;
    }

    static immediatePipelineRun = async(pipeline, execId) => await PipelineService.handlePipelineCheckpoint(pipeline, execId)
        
    static async handlePipelineCheckpoint(pipeline, execId, archive){
        let namespace = await UserService.getNamespace(), url;
        if(archive)
            url = `/ppline/archive/${namespace}/${pipeline}${execId ? `/${execId}` : ''}`;
        else
            url = `/ppline/run/${namespace}/${pipeline}${execId ? `/${execId}` : ''}`;
        const response = await $still.HTTPClient.post(url);
        
        if(archive) return await response.json();

        const { result } = await response.json();
        return result.result;

    }

    static async getDataCatalog(pipeline){
        const namespace = await UserService.getNamespace();
        
        const url = `/datacatalog/${pipeline}/catalog/${namespace}`;
        const response = await $still.HTTPClient.get(url);
        const result = await response.json();

        return result;
    }

    /** @param { DataCatalogUI } catalogUI */
    static async updateDataCatalogByPipelineTable(pipeline, table, payload, catalogUI){
        catalogUI.catalogSaveInProgress = true;
        const namespace = await UserService.getNamespace();
        const url = `/datacatalog/${pipeline}/${table}/catalog/${namespace}`;
        const response = await $still.HTTPClient.post(url, JSON.stringify(payload), { headers: PipelineService.jsonHeaders });
        const result = await response.json();

        if(!result.error){
            catalogUI.showToast(result.result, 'success');
            AppTemplate.toast.success(result.result)
            catalogUI.unsavedState = false;
        }else{
            catalogUI.showToast(result.result, 'failed');
            AppTemplate.toast.error(result.result)
        }
        catalogUI.catalogSaveInProgress = false;

    }

    static checkUnsavedStatusAlert({ confirm, cancel }){
		let message = 'You have unsaved Semantic concept. Do you which to leave without save?';
		let title = 'Unsave changes!';
        const onConfirm = async () => await confirm();
        const onCancel = async () => await cancel();
        return WorkSpaceController.get().showDialog(message, { type: 'confirm', title, onConfirm, onCancel })
    }

    /** @returns { { result, error } | undefined } */
    static async runSQLQuery(query, database, connectionName = null, destType = 'duckdb') {
        const payload = { 
            query, 
            database,
            namespace: await UserService.getNamespace(),
            connection_name: connectionName,
            dest_type: destType,
            referencedSecrets: PipelineService.pipelineReferencedSecrets,
            destinationConfig: PipelineService.pipelineDestinationConfig,
            destinationDB: PipelineService.pipelineDestinationDB,
        };
        const url = '/workcpace/sql_query';
        const response = await $still.HTTPClient.post(url, JSON.stringify(payload), {
            headers: { 'content-type': 'Application/json' }
        });

        const result = await response.json();

        if (result.error){
            if(result.code === 'err')
                AppTemplate.toast.error('Error while querying the DB: ' + result.result, 10000);
            AppTemplate.toast.warn('Exception while querying the DB: ' + result.result);
            return { error: result.result };
        }
        return { ...result, error: null };
    }

    static async addTrigger(code) {
        const user = await UserService.getNamespace();
        let response = await $still.HTTPClient.post('/trigger/' + user, JSON.stringify(code), HTTPHeaders.JSON);
        response = await response.json();

        if (!response.error){
            AppTemplate.toast.success(response.result);
            return true;
        }
        AppTemplate.toast.error(response.result);
    }

    static async getPipelinesForGernanceView(){
        const pplineList = await BIService.getDWPipelines();
        return (pplineList || []).map(([pipeline, dtset]) => {
            const path = dtset.split('.');
            const hasPath = path.length > 1;
            return { pipeline, srcPipeline: hasPath ? path[1] : pipeline, dwName: hasPath ? path[0] : dtset }
        });
    }

}