import { $still } from "../../../../@still/component/manager/registror.js";
import { BaseController } from "../../../../@still/component/super/service/BaseController.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { BIService } from "../../dataviz/services/BIService.js";

export class DGServiceController extends BaseController {

    async loadTablesByPipeline(pipelineName){
        const result = await BIService.getDomainPipelineFields(pipelineName);
        const generateField = (f) => {
            const name = f['column_name'], table = f['table_name'], disabled = f['translation_active'];
            return { id: `${table}.${name}`, name, table, trans: f['translation'], desc: f['description'], disabled }
        }
        const fields = (Object.values(result.allFields) || []).flatMap(itm => itm).map(f => generateField(f));
        return { fields, tables: Object.keys(result.allFields) };
        
    }

    async savePipelineDisctionary(pipelineName, values){
        
        const namespace = await BIService.getNamespace();
        const url = `/pipeline/dictionary/${namespace}/${pipelineName}`
        const request = await $still.HTTPClient.post(url, JSON.stringify({ values }), HTTPHeaders.JSON)
        return await request.json();
        
    }

}