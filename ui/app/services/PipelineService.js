import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { AppTemplate } from "../../config/app-template.js";
import { DataCatalogUI } from "../components/data-catalog/DataCatalogUI.js";
import { WorkSpaceController } from "../controller/WorkSpaceController.js";
import { UserService } from "./UserService.js";

export class PipelineService extends BaseService {

    table = new ServiceEvent([]);
    static tableListStore;
    static jsonHeaders = { 'Content-Type': 'application/json' };

    async createOrUpdatePipeline(content = null, update = false, actionType = '') {

        const payload = content || {};

        const headers = PipelineService.jsonHeaders;
        if(update === true){
            return $still.HTTPClient.put('/pipeline/create', JSON.stringify(payload), { headers });
        }else{
            return $still.HTTPClient.post('/pipeline/create', JSON.stringify({ ...payload, actionType }), { headers });
        }
    }

    static async getPipelinesNames(){
        const namespace = await UserService.getNamespace();
        const url = '/workspace/pipelines/list/' + namespace;
        const response = await $still.HTTPClient.post(url, null, {
            headers: { 'Content-Type': 'application/json' }
        });
        const { db_path: _, ...tables } = await response.json();
        PipelineService.tableListStore = tables;

        return Object.keys(PipelineService.tableListStore).map( name => ({name}));
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

}