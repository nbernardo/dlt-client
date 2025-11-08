import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { StillHTTPClient } from "../../@still/helper/http.js";
import { StillAppSetup } from "../../config/app-setup.js";
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
    tableListStore = new ServiceEvent(null);
    fieldsByTableMap = {};
    aiAgentNamespaceDetails = {};
    totalPipelines = 0;
    dbPath = null;
    parsedTableListStore = new ServiceEvent([]);
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
        { imgIcon: 'app/assets/imgs/dlt-logo-colored.png', label: 'Input - DLT code', typeName: SqlDBComponent.name, disable: 'yes', name: 'DLT-class' },
        { 
            imgIcon: 'app/assets/imgs/language-python-text-svgrepo-com.svg', 
            label: 'Code Transformation', 
            typeName: 'none', 
            disable: 'yes' 
        },
        { icon: 'fas fa-cogs', label: 'Transformation', typeName: Transformation.name },
        {
            imgIcon: 'app/assets/imgs/duckdb-icon.svg',
            label: 'Out-DBFile (.duckdb)',
            typeName: DuckDBOutput.name
        },
    ]

    static async getNamespace(){
        return StillAppSetup.config.get('anonymousLogin')
            ? UserUtil.email : await UserService.getNamespace();
    }

    async getParsedTables(namespace, socketId) {

        if(this.parsedTableListStore.value.length == 0){

            const result = await this.getDuckDbs(namespace, socketId);
            const data = Object.entries(result);
            const tables = [];
    
            for (const [database, ppline] of data) {
                const tablesDetails = Object.values(ppline);
                for (const tableDetail of tablesDetails) {
                    const tablePath = `${database}.${tableDetail.dbname}.${tableDetail.table}`
                    tables.push({ database, table: `${tableDetail.dbname}.${tableDetail.table}`, tablePath});
                    this.fieldsByTableMap[tablePath] = tableDetail.fields;
                }
            }

            this.parsedTableListStore = tables;
        }

        return this.parsedTableListStore.value;

    }

    async runCode(code, user) {

        const url = '/workcpace/code/run/' + user;
        const result = await $still.HTTPClient.post(url, JSON.stringify(code), {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        return result;

    }

    async getDuckDbs(user, socketId) {

        //if (this.tableListStore.value == null) {
            const url = '/workcpace/duckdb/list/' + user + '/' + socketId;
            const response = await $still.HTTPClient.post(url, null, {
                headers: { 'Content-Type': 'application/json' }
            });
            const { db_path, ...tables } = await response.json();
            this.dbPath = db_path;
            this.tableListStore = tables;
        //}
        return this.tableListStore.value;
        
    }

    async downloadfile(fileName, downloadType) {
        const baseUrl = StillHTTPClient.getBaseUrl();
        window.location.href = `${baseUrl}/download/${downloadType}/${UserUtil.email}/${fileName}`;
    }

    async deletefile(fileName) {
        const user = UserUtil.email;
        const response = await $still.HTTPClient.delete('/file/' + user + '/' + fileName);
        if (response.ok)
            return await response.json();
        return null;
    }

    async handleDuckdbConnect(payload, action = WorkspaceService.CONNECT_DB) {

        let url = '/workcpace/duckdb/connect';
        if (action == WorkspaceService.DISCONECT_DB)
            url = '/workcpace/duckdb/disconnect';

        const result = await $still.HTTPClient.post(url, JSON.stringify(payload), {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        return result;
    }

    async listPplineFiles(user) {
        const response = await $still.HTTPClient.get('/scriptfiles/' + user + '/');
        if (response.status === 404) {
            AppTemplate.toast.warn('No pipeline(s) found under ' + user);
        } else if (response.ok)
            return await response.json();

        return null;
    }

    async readScriptFile(user, fileName) {
        const response = await $still.HTTPClient.get('/scriptfiles/' + user + '/' + fileName);
        if (response.ok)
            return await response.text();
        return null;
    }

    async updatePpline(user, fileName, code) {
        const response = await $still.HTTPClient.post('/scriptfiles/' + user + '/' + fileName, code);
        if (response.ok)
            return await response.text();
        return null;
    }

    async readDiagramFile(user, fileName) {
        const response = await $still.HTTPClient.get('/ppline/diagram/' + user + '/' + fileName);
        if (response.ok)
            return await response.text();
        return null;
    }

    async listFiles() {
        let filesList = null;
        const response = await $still.HTTPClient.get('/files/' + UserUtil.email);
        if (response.status === 404) {
            AppTemplate.toast.warn('No data file found under ' + UserUtil.email);
        } else if (response.ok) {
            filesList = await response.json();
        }
        return filesList;
    }

    async getCsvFileFields(filename) {
        let filesList = null;
        const response = await $still.HTTPClient.get(`/ppline/data/csv/${UserUtil.email}/${filename}`);
        if (response.status === 404)
            AppTemplate.toast.warn('No data file found under ' + UserUtil.email);
        else if (response.ok)
            filesList = (await response.text());

        return filesList;
    }

    async handleCsvSourceFields(selectdFile) {

        if (!this.dataSourceFieldsMap.has(selectdFile)) {
            const fields = await this.getCsvFileFields(selectdFile);
            
            if (fields != null) {
                // API Response will be something like Index(['ID', 'Name', 'Age', 'Country'], dtype='object')
                // hence bellow we're clearing things up so to have an array with the proper field names
                const fieldList = fields.split('[')[1].split(']')[0].replace(/\'\s{0,}/g, '').split(',')
                    .map((name, id) => ({ name: name.trim(), id, type: 'string' }));

                this.dataSourceFieldsMap.set(selectdFile, fieldList);
            }
        }
    }

    getCsvDataSourceFields = (sourceName) => this.dataSourceFieldsMap.get(sourceName);
    
    async updateSocketId(socketId) {
        const response = await $still.HTTPClient.post('/workcpace/socket_id/' + UserUtil.email + '/' + socketId);
        if (response.ok)
            return await response.text();
        return null;
    }

    async schedulePipeline(payload) {
        const namespace = UserUtil.email;
        const url = '/workcpace/ppline/schedule/' + namespace;
        const headers = { 'Content-Type': 'application/json' };
        const response = await $still.HTTPClient.post(url, payload, { headers });
        if (response.ok)
            return await response.text();
        return false;
    }

    static async getPipelineSchedules() {
        const namespace = await UserService.getNamespace();
        const url = '/workcpace/ppline/schedule/' + namespace;
        const response = await $still.HTTPClient.get(url);
        
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    static async getPipelineInitialData() {
        const namespace = await UserService.getNamespace();
        const url = '/workcpace/init/' + namespace;
        const response = await $still.HTTPClient.get(url);
        
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    static async startChatConversation() {

        const namespace = StillAppSetup.config.get('anonymousLogin')
            ? UserUtil.email : await UserService.getNamespace();

        const url = '/workcpace/agent/' + namespace;
        try {
            const response = await $still.HTTPClient.get(url);
            if (response.ok && !response.error)
                return await response.json();
        } catch (error) {
            console.log(`Error on starting the chat: `, error);
        }
        return null;
    }

    /** @returns { { result: { result, fields, actual_query, db_file } } } */
    static async sendAgentMessage(message) {

        const namespace = StillAppSetup.config.get('anonymousLogin')
            ? UserUtil.email : await UserService.getNamespace();

        const url = '/workcpace/agent/' + namespace;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ message }), {
            headers: { 'content-type': 'Application/json' }
        });
        if (response.ok && !response.error)
            return await response.json();
        return null;
    }

    /** @returns { { result, fields } | undefined } */
    async runSQLQuery(query, database) {
        const payload = { query, database };
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


    /** @returns { { result: { result, fields, actual_query, db_file } } } */
    static async createSecret(secret) {

        const namespace = await UserService.getNamespace();
        const url = '/secret/' + namespace;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ ...secret }), {
            headers: { 'content-type': 'Application/json' }
        });
        
        const result = await response.json();
        
        if (response.ok && !result.error)
            return AppTemplate.toast.success('Secrete created successfully');
        else
            AppTemplate.toast.error(result.result);
    }


    /** @returns { Array<string> } */
    static async listSecrets(type) {

        const namespace = await UserService.getNamespace();
        const url = '/secret/' + namespace;
        const response = await $still.HTTPClient.get(url);
        if (response.ok && !response.error){

            const secretList = (await response.json()).result;
            let secretAndServerList;
            
            if(type == 2 && Array.isArray(secretList?.api_secrets))
				secretAndServerList = secretList.api_secrets.map(secret => ({ name: secret, host: 'to.be.def' }))
            
            if(type == 1 && Array.isArray(secretList?.db_secrets))
				secretAndServerList = secretList.db_secrets.map(secret => ({ name: secret, host: secretList.metadata[secret] || 'None' }));
			
            return secretAndServerList.length > 0 ? secretAndServerList : [];

        } else {
            const result = await response.json();
            AppTemplate.toast.error(result.result);
        }

    }

    /** @returns { Object } */
    static async fetchSecret(secretName, type) {

        const namespace = await UserService.getNamespace();
        const url = `/secret/${namespace}/${type}/${secretName}`;
        const response = await $still.HTTPClient.get(url);
        if (response.ok && !response.error)
            return (await response.json()).result;
        else{
            const result = await response.json();
            AppTemplate.toast.error(result.result);
        }

    }

    /** @returns { {tables, secret_details} } */
    static async getConnectionDetails(connectionName) {

        const namespace = await UserService.getNamespace();
        const url = `/${namespace}/db/connection/${connectionName}/tables`;

        const response = await $still.HTTPClient.get(url);
        const result = await response.json();
        
        if (response.ok && !result.error){
            if(result.result.tables?.schema_based){
                delete result.result.tables?.schema_based;
                const allTables = [], schemas = Object.entries(result.result.tables);
                for(const [schema, tables] of schemas){
                    for(const table of tables){
                        allTables.push(`${schema}.${table}`);
                    }
                }
                result.result['tables'] = allTables;
            }
            return result.result;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    /** @returns { { fields } | undefined } */
    static async getDBTableDetails(connectionName, tableName) {

        const namespace = await UserService.getNamespace();
        const url = `/${namespace}/db/${connectionName}/${tableName}`;

        const response = await $still.HTTPClient.get(url);
        const result = await response.json();
        
        if (response.ok && !result.error)
            return result.result;
        else
            AppTemplate.toast.error(result.result);
    }

}