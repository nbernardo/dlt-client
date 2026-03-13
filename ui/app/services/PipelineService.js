import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { UserService } from "./UserService.js";

export class PipelineService extends BaseService {

    table = new ServiceEvent([]);
    static tableListStore;

    async createOrUpdatePipeline(content = null, update = false, actionType = '') {

        const payload = content || {};

        const headers = { 'Content-Type': 'application/json' };
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
        
        const url = `/ppline/${pipeline}/catalog/${namespace}`;
        const response = await $still.HTTPClient.get(url);
        const result = await response.json();

        return result;
    }

}