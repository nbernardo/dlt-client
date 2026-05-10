import { $still } from "../../../../@still/component/manager/registror.js";
import { BaseService } from "../../../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../../../@still/helper/http.js";
import { AppTemplate } from "../../../../config/app-template.js";
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
        pipelinePlanContent.content.Home.data[2].data.database = settings.sourceDatabase;
        pipelinePlanContent.content.Home.data[2].data.connectionName = settings.sourceDBConnection;
        pipelinePlanContent.content.Home.data[2].data.dbengine = settings.sourceDBEngine;
        pipelinePlanContent.content.Home.data[2].data.host = settings.sourceDBHost;
        pipelinePlanContent.content.Home.data[2].data.namespace = settings.planNamespace;
        pipelinePlanContent.goldQuery = settings.goldTableQuery;
        pipelinePlanContent.content.Home.data[3].data.database = settings.planPipelineLabel.toLowerCase().replace(/(\s|\_|\-)/g,'');
        this.settings = pipelinePlanContent;
    }

    async save(update, id) {
        const namespace = await BIService.getNamespace();
        const url = `/analytics/${namespace}/pipeline/plan`;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ settings: this.settings, update, id }), HTTPHeaders.JSON);
        const result = await response.json();

        if (response.ok && !result.error)
            return AppTemplate.toast.success('Plan saved successfully')

        AppTemplate.toast.error('Plan saving error: '+result.result)
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
    /** @type { String } */ sourceDatabase = '';
    /** @type { String } */ sourceDBHost = '';
    /** @type { String } */ sourceDBConnection = '';
    /** @type { String } */ sourceDBEngine = '';
    /** @type { String } */ planNamespace = '';
}