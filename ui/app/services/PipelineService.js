import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";

export class PipelineService extends BaseService {

    table = new ServiceEvent([]);

    async createOrUpdatePipeline(content = null, update = false, actionType = '') {

        const payload = content || {};

        const headers = { 'Content-Type': 'application/json' };
        if(update === true){
            return $still.HTTPClient.put('/pipeline/create', JSON.stringify(payload), { headers });
        }else{
            return $still.HTTPClient.post('/pipeline/create', JSON.stringify({ ...payload, actionType }), { headers });
        }
    }

}