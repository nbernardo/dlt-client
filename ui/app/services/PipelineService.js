import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";

export class PipelineService extends BaseService {

    table = new ServiceEvent([]);

    async createPipeline() {

        const payload = {
            "backet_url": "/home/nakassony/dlt-project/z/",
            "file": "encounters*.csv",
            "table_name": "encounters",
            "pipeline": "hosp_test_pipeline",
            "schema": "hospital_data"
        };

        return $still.HTTPClient.post('/pipeline/create', JSON.stringify(payload), {
            headers: {
                'Content-Type': 'application/json'
            }
        });
    }

}