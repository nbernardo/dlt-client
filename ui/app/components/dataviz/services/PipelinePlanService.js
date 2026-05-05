import { $still } from "../../../../@still/component/manager/registror.js";
import { BaseService } from "../../../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { pipelinePlanContent } from "../samples/pipelinePlan.js";
import { BIService } from "./BIService.js";

export class PipelinePlanService extends BaseService {

    /**  @type { Object } */ settings;

    /**  @param { PipelinePlanPayload } settings */
    constructor(settings){
        super();
        pipelinePlanContent.pipeline_lbl = settings.planPipelineLabel;
        pipelinePlanContent.content.Home.data[2].data.tables = settings.tables;
        pipelinePlanContent.content.Home.data[2].data.primaryKeys = settings.primaryKeys;
        pipelinePlanContent.goldQuery = settings.goldTableQuery;
        this.settings = pipelinePlanContent;
    }

    async save() {
        const namespace = await BIService.getNamespace();
        const url = `/analytics/${namespace}/pipeline/plan`;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ settings: this.settings }), HTTPHeaders.JSON);
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    static async getPipelinePlans(){
        const namespace = await BIService.getNamespace();
        const url = `/analytics/${namespace}/pipeline/plan`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

}


export class PipelinePlanPayload {
    /** @type { Object } */ tables = {};
    /** @type { Object } */ primaryKeys = {};
    /** @type { String } */ goldTableQuery;
    /** @type { String } */ planPipelineLabel = '';
}