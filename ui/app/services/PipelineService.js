import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";

export class PipelineService extends BaseService {

    table = new ServiceEvent([]);

    async createOrUpdatePipeline(content = null, update = false) {

        const payload = content || {
            "backet_url": "/home/nakassony/dlt-project/z/",
            "file": "encounters*.csv",
            "table_name": "encounters",
            "pipeline": "hosp_test_pipeline",
            "schema": "hospital_data"
        };

        const headers = { 'Content-Type': 'application/json' };
        if(update === true){
            return $still.HTTPClient.put('/pipeline/create', JSON.stringify(payload), { headers });
        }else{
            return $still.HTTPClient.post('/pipeline/create', JSON.stringify(payload), { headers });
        }
    }

}