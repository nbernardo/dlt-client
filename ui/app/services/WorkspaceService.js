import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { AppTemplate } from "../../config/app-template.js";
import { UserUtil } from "../components/auth/UserUtil.js";
import { Bucket } from "../components/node-types/Bucket.js";
import { DuckDBOutput } from "../components/node-types/DuckDBOutput.js";
import { SqlDBComponent } from "../components/node-types/SqlDBComponent.js";
import { Transformation } from "../components/node-types/Transformation.js";
import { UserService } from "./UserService.js";

export class ObjectDataTypes {
    typeName;
    icon;
    label;
    imgIcon;
    source;//Can it be a source of stream
    dest;//Can it be a dest of stream
};

export class WorkspaceService extends BaseService {

    table = new ServiceEvent([]);
    schedulePipelinesStore = new ServiceEvent([]);
    
    static DISCONECT_DB = 'DISCONECT';
    static CONNECT_DB = 'CONNECT';

    dataSourceFieldsMap = new Map();

    /** @type { Array<ObjectDataTypes> } */
    objectTypes = [
        { icon: 'far fa-circle', label: 'Start', typeName: 'Start', source: 0, dest: 1 },
        { icon: 'fas fa-circle', label: 'End', typeName: 'End', source: 1, dest: 0 },
        { icon: 'fab fa-bitbucket', label: 'Input - Bucket', typeName: Bucket.name },
        { imgIcon: 'app/assets/imgs/sql-server-2.png', label: 'Input - SQL DB', typeName: SqlDBComponent.name },
        { icon: 'fas fa-file-alt', label: 'Input File', typeName: 'slack' },
        { icon: 'fas fa-cogs', label: 'Transformation', typeName: Transformation.name },
        {
            imgIcon: 'app/assets/imgs/duckdb-icon.svg',
            label: 'Out-DBFile (.duckdb)',
            typeName: DuckDBOutput.name
        },
    ]

    async runCode(code, user){

        const url = '/workcpace/code/run/'+user;
        const result = await $still.HTTPClient.post(url, JSON.stringify(code), {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        return result;

    }

    async getDuckDbs(user, socketId){
        const url = '/workcpace/duckdb/list/'+user+'/'+socketId;
        const result = await $still.HTTPClient.post(url, null, {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        return result;
    }

    async handleDuckdbConnect(payload, action = WorkspaceService.CONNECT_DB){

        let url = '/workcpace/duckdb/connect';
        if(action == WorkspaceService.DISCONECT_DB)
            url = '/workcpace/duckdb/disconnect';

        const result = await $still.HTTPClient.post(url, JSON.stringify(payload), {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        return result;
    }

	async listPplineFiles(user){
        const response = await $still.HTTPClient.get('/scriptfiles/'+user+'/');
        if(response.status === 404){
            AppTemplate.toast.error('No pipeline(s) found under '+user);
        } else if (response.ok)
			return await response.json();
		
		return null;
	}

	async readScriptFile(user, fileName){
		const response = await $still.HTTPClient.get('/scriptfiles/'+user+'/'+fileName);
        if(response.ok)
			return await response.text();
		return null;
	}

	async updatePpline(user, fileName, code){
		const response = await $still.HTTPClient.post('/scriptfiles/'+user+'/'+fileName, code);
		if(response.ok)
			return await response.text();
		return null;
	}

	async readDiagramFile(user, fileName){
		const response = await $still.HTTPClient.get('/ppline/diagram/'+user+'/'+fileName);
        if(response.ok)
			return await response.text();
		return null;
	}

	async listFiles(){
		let filesList = null;
		const response = await $still.HTTPClient.get('/files/'+UserUtil.email);
		if(response.status === 404){
            AppTemplate.toast.error('No data file found under '+UserUtil.email);
        } else if(response.ok){
			filesList = await response.json();
		}
		return filesList;
	}

	async getCsvFileFields(filename){
		let filesList = null;
		const response = await $still.HTTPClient.get(`/ppline/data/csv/${UserUtil.email}/${filename}`);
		if(response.status === 404)
            AppTemplate.toast.error('No data file found under '+UserUtil.email);
        else if(response.ok)
			filesList = (await response.text());
		
		return filesList;
	}

	async handleCsvSourceFields(selectdFile){

		if(!this.dataSourceFieldsMap.has(selectdFile)){
			const fields = await this.getCsvFileFields(selectdFile);
			if(fields != null) {
				// API Response will be something like Index(['ID', 'Name', 'Age', 'Country'], dtype='object')
				// hence bellow we're clearing things up so to have an array with the proper field names
				const fieldList = fields.split('[')[1].split(']')[0].replace(/\'|\s/g,'').split(',')
					.map((name, id) => ({ name, id, type: 'string' }));

				this.dataSourceFieldsMap.set(selectdFile, fieldList);
			}
		}
	}
    
    getCsvDataSourceFields(sourceName){
        return this.dataSourceFieldsMap.get(sourceName);
    }

	async updateSocketId(socketId){
		const response = await $still.HTTPClient.post('/workcpace/socket_id/'+UserUtil.email+'/'+socketId);
		if(response.ok)
			return await response.text();
		return null;
	}

	async schedulePipeline(payload){
        const namespace = UserUtil.email;
        const url = '/workcpace/ppline/schedule/'+namespace;
        const headers = { 'Content-Type': 'application/json' };
		const response = await $still.HTTPClient.post(url,payload,{ headers });
		if(response.ok)
			return await response.text();
		return false;
	}

	static async getPipelineSchedules(){
        const namespace = await UserService.getNamespace();
        const url = '/workcpace/ppline/schedule/'+namespace;
		const response = await $still.HTTPClient.get(url);
		if(response.ok && !response.error)
			return await response.json();
		return null;
	}

	static async startChatConversation(){
        const namespace = await UserService.getNamespace();
        const url = '/workcpace/agent/'+namespace;
		const response = await $still.HTTPClient.get(url);
		if(response.ok && !response.error)
			return await response.json();
		return null;
	}

	static async sendAgentMessage(message){
        const namespace = await UserService.getNamespace();
        const url = '/workcpace/agent/'+namespace;
		const response = await $still.HTTPClient.post(url, JSON.stringify({ message }), {
            headers: { 'content-type': 'Application/json' }
        });
		if(response.ok && !response.error)
			return await response.json();
		return null;
	}

}