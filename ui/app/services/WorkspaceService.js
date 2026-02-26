import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { HTTPHeaders, StillHTTPClient } from "../../@still/helper/http.js";
import { UUIDUtil } from "../../@still/util/UUIDUtil.js";
import { StillAppSetup } from "../../config/app-setup.js";
import { AppTemplate } from "../../config/app-template.js";
import { UserUtil } from "../components/auth/UserUtil.js";
import { parseJSON } from "../components/catalog/util/CatalogUtil.js";
import { InputAPI } from "../components/node-types/api/InputAPI.js";
import { Bucket } from "../components/node-types/Bucket.js";
import { DLTCodeOutput } from "../components/node-types/destination/DLTCodeOutput.js";
import { DLTCode } from "../components/node-types/dlt/DLTCode.js";
import { DuckDBOutput } from "../components/node-types/DuckDBOutput.js";
import { DatabaseOutput } from "../components/node-types/output/DatabaseOutput.js";
import { SqlDBComponent } from "../components/node-types/SqlDBComponent.js";
import { Transformation } from "../components/node-types/Transformation.js";
import { Workspace } from "../components/workspace/Workspace.js";
import { UserService } from "./UserService.js";

export class ObjectDataTypes {
    typeName;
    icon;
    label;
    imgIcon;
    source;//Can it be a source of stream
    dest;//Can it be a dest of stream
    name;
    isNodeGroup;
    groupType;
    dropDownIcon;
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
    static currentSelectedPpeline = null;
    static currentSelectedPpelineStatus = null;
    /** @type { Workspace } */
    component;

    static DISCONECT_DB = 'DISCONECT';
    static CONNECT_DB = 'CONNECT';

    dataSourceFieldsMap = new Map();

    /** @type { Array<ObjectDataTypes> } */
    objectTypes = [
        { icon: 'far fa-circle', label: 'Start', typeName: 'Start', source: 0, dest: 1 },
        { icon: 'fas fa-circle', label: 'End', typeName: 'End', source: 1, dest: 0 },
        //Group of nodes concerning Source types
        { 
            isNodeGroup: 'yes', imgIcon: 'app/assets/imgs/input.png', label: 'Sources', 
            typeName: 'none', name: 'SourceGroup', dropDownIcon: '<i class="fas fa-chevron-circle-right"></i>' 
        },
        { groupType: 'SourceGroup', icon: 'fab fa-bitbucket', label: 'Input - Bucket', typeName: Bucket.name },
        { groupType: 'SourceGroup', imgIcon: 'app/assets/imgs/sql-server-2.png', label: 'Input - SQL DB', typeName: SqlDBComponent.name, tmplt: 'SqlDBComponent_old.html' },
        { groupType: 'SourceGroup', imgIcon: 'app/assets/imgs/sql-server-v2.png', label: 'Input - SQL DB - V2', typeName: SqlDBComponent.name },
        { groupType: 'SourceGroup', imgIcon: 'app/assets/imgs/api-source.svg', label: 'Input - API', typeName: InputAPI.name },
        { groupType: 'SourceGroup', imgIcon: 'app/assets/imgs/dltlogo.png', label: 'Input - DLT code', typeName: DLTCode.name, disable: 'false', name: 'DLT-class' },
        //Group of nodes concerning Transformation types
        { 
            isNodeGroup: 'yes', imgIcon: 'app/assets/imgs/adaptation.png', label: 'Transformations', 
            typeName: 'none', name: 'TransformationGroup', dropDownIcon: '<i class="fas fa-chevron-circle-right"></i>' 
        },
        { groupType: 'TransformationGroup', icon: 'fas fa-cogs', label: 'Transformation', typeName: Transformation.name },
        { groupType: 'TransformationGroup', imgIcon: 'app/assets/imgs/py.svg',  label: 'Code Transformation',  typeName: 'none',  disable: 'yes' },
        //Group of nodes concerning Destination types
        { 
            isNodeGroup: 'yes' ,imgIcon: 'app/assets/imgs/output_.png', label: 'Outputs/Destinations', 
            typeName: 'none', name: 'OutputsGroup', dropDownIcon: '<i class="fas fa-chevron-circle-right"></i>' 
        },
        { groupType: 'OutputsGroup', imgIcon: 'app/assets/imgs/duckdb-icon.svg', label: 'Duckdb (.duckdb)', typeName: DuckDBOutput.name },
        { groupType: 'OutputsGroup', imgIcon: 'app/assets/imgs/writetodatabase.png', label: 'Database', typeName: DatabaseOutput.name, name: 'Out-SQL' },
        { groupType: 'OutputsGroup', imgIcon: 'app/assets/imgs/dltlogo.png', label: 'Out - DLT code', typeName: DLTCodeOutput.name, name: 'DLTOutput' },
        //fas fa-chevron-circle-right
    ];

    /** @type { WorkspaceService } */
    static self;

    constructor(){
        super();
        WorkspaceService.self = this;
    }

    static async getNamespace(){
        return StillAppSetup.config.get('anonymousLogin')
            ? UserUtil.email : await UserService.getNamespace();
    }

    async getParsedTables(socketId) {

        //if(this.parsedTableListStore.value.length == 0){

            const result = await this.getPipelines(socketId);
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
        //}

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

    static getPipelineList = async (socketId) => await WorkspaceService.self.getPipelines(socketId)

    async getPipelines(socketId){
        const user = await UserService.getNamespace();
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
        const user = await UserService.getNamespace();
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

    async readDiagramFile(namespace, fileName) {
        const response = await $still.HTTPClient.get('/ppline/diagram/' + namespace + '/' + fileName);
        if (response.ok)
            return await response.text();
        return null;
    }

    async listFiles() {
        let filesList = null, namespace = await UserService.getNamespace();
        const response = await $still.HTTPClient.get('/files/' + namespace);
        if (response.status === 404) {
            AppTemplate.toast.warn('No data file found under ' + namespace);
        } else if (response.ok) {
            filesList = await response.json();
        }
        return filesList;
    }

    async getCsvFileFields(filename) {
        let filesList = null, namespace = await UserService.getNamespace();;
        const response = await $still.HTTPClient.get(`/ppline/data/csv/${namespace}/${filename}`);
        if (response.status === 404)
            AppTemplate.toast.warn('No data file found under ' + namespace);
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
        const namespace = await UserService.getNamespace();
        const response = await $still.HTTPClient.post('/workcpace/socket_id/' + namespace + '/' + socketId);
        if (response.ok)
            return await response.text();
        return null;
    }

    async schedulePipeline(payload) {
        const namespace = await UserService.getNamespace();
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
        
        if (response.ok && !response.error){
            const result = await response.json();
            return Object.values(result?.data || {});
        }
        return null;
    }

    static async getPipelineInitialData() {
        const namespace = await UserService.getNamespace();
        const url = '/workcpace/init/' + namespace;
        const response = await $still.HTTPClient.get(url);
        
        if (response.ok && !response.error){
            return await response.json();
        }
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

    /** @returns { { result: { result } } } */
    static async sendDataQueryAgentMessage(message) {

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

    /** @returns { { result: { result } } } */
    static async sendPipelineAgentMessage(message) {

        const namespace = StillAppSetup.config.get('anonymousLogin')
            ? UserUtil.email : await UserService.getNamespace();

        const url = '/pipeline/agent/' + namespace;
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

        if(secret.apiSettings){
            if(String(secret.apiSettings.apiBaseUrl).endsWith('/'))
                secret.apiSettings.apiBaseUrl = secret.apiSettings.apiBaseUrl.slice(0,-1);
        }

        const namespace = await UserService.getNamespace();
        const url = '/secret/' + namespace;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ ...secret }), {
            headers: { 'content-type': 'Application/json' }
        });
        
        const result = await response.json();
        
        if (response.ok && !result.error){
            AppTemplate.toast.success('Secrete created successfully');
            return true;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    static async testDbConnection(secret, existing) {

        const url = existing ? '/workspace/connection/exists/test' : '/workspace/connection/test';
        const response = await $still.HTTPClient.post(url, JSON.stringify({ ...secret }), {
            headers: { 'content-type': 'Application/json' }
        });
        
        const result = await response.json();
        
        if (response.ok && !result.error){
            AppTemplate.toast.success('DB Connection was successful');
            return true;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    static async testAPIConnection(payload, existing) {

        const namespace = StillAppSetup.config.get('anonymousLogin')
            ? UserUtil.email : await UserService.getNamespace();
            
        const url = Object.keys(existing).length > 0 ? `/workspace/${namespace}/api/exists/test` : `/workspace/${namespace}/api/test`;
        const response = await $still.HTTPClient.post(url, JSON.stringify({ ...payload }), HTTPHeaders.JSON);
        
        let result = await response.json(), errors = 0;
        
        if (response.ok && !result.error){

            const totalEndpoints = result.length;
            result = result.map(it => {

                let css_class = 'api-response-is_ok';
                if(it['3-Success'] === false) 
                    css_class = 'api-response-not_ok', errors++;
                
                const contentId = 'apiresp_data'+UUIDUtil.newId();
                let data = JSON.stringify(Array.isArray(it.data) ? it.data.slice(0,1) : it.data,null,'\t');
                data = parseJSON(data).slice(0,-2).replace('<br>','');
                data = WorkspaceService.self.component.parseEvents(
                    `<apiresp-data>
                        &nbsp;<span onclick="inner.showAPIData('${contentId}')">Show data</span>
                        &nbsp;<span id="${contentId}" style="display:none;">${data}</span>
                    </apiresp-data>`.replaceAll('\n','')
                );              
                let value = { ...it, data };
                
                // Because the Data in the API fresponse is converted to String hence some disturbig
                // characters will be there to clean up from bellow parsing and replace statements
                value = parseJSON(JSON.stringify(value,null,'\t'))
                            .replace(' "<apiresp-data','<apiresp-data')
                            .replace('</apiresp-data>"','</apiresp-data>');
                
                return `<span class="${css_class}">${value}</span>`;
            })
            .join("<br>");

            let content = JSON.stringify(result)
            content = content.trim()
                            .replace('[','').slice(0,-1)
                            .replaceAll('\\','')
                            .replace('"','');

            if(errors > 0) AppTemplate.toast.error(`Error in ${errors} out of ${totalEndpoints} endpoints!`);
            else AppTemplate.toast.success('API Connection was successful');
            return { errors, content, totalEndpoints };
        }
        else
            AppTemplate.toast.error(result.result);
        return { errors: 1 };
    }

    /** @param { CatalogForm } catalogForm */
    static async testConnectWithKVSecret(catalogForm) {

        const obj = catalogForm;
        const keyType = obj.kvSecretType.value, 
              firstValue = obj.firstValue.value, 
              bucketUrl = (obj.bucketUrl.value.split('//')[1] || '').replace('/','');

        let data = {}, secretKey = document.querySelectorAll(`.${keyType}`)[2].querySelector('input').value;

        if(keyType === 's3-access-and-secret-keys')
            data = { 's3Config' : { 'access_key_id': firstValue, 'secret_access_key': secretKey, 'bucket_name': bucketUrl } };
        
        const url = '/workspace/s3/connection/test';
        const response = await $still.HTTPClient.post(url, JSON.stringify(data), {
            headers: { 'content-type': 'Application/json' }
        });
        
        const result = await response.json();
        if (response.ok && !result.error){
            AppTemplate.toast.success('DB Connection was successful');
            return true;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    static async getOracleDN(host, port) {

        const url = `/db/connection/${host}/${port}`;
        const response = await $still.HTTPClient.get(url);
        
        const result = await response.json();
        
        if (response.ok && !result.error){
            AppTemplate.toast.success('Oracle DN loaded successfully');
            return result?.result;
        }
        else
            AppTemplate.toast.error(result.result);
    }


    /** @returns { Array<string> | {} } */
    static async listSecrets(type, cb = () => {}) {
        try {
            
            const namespace = await UserService.getNamespace();
            const url = '/secret/' + namespace, allSecrets = {};
            const response = await $still.HTTPClient.get(url);
    
            if (response.ok && !response.error){
    
                const secretList = (await response.json()).result;
                let secretAndServerList;
    
                if((type == 2 || type == 'all') && Array.isArray(secretList?.api_secrets)){
                    secretAndServerList = secretList.api_secrets.map(secret => 
                        ({ 
                            name: secret, host: secretList.metadata[secret].host, 
                            totalEndpoints: secretList.metadata[secret].totalEndpoints 
                        })
                    );
                    if(type == 'all') allSecrets['api'] = secretAndServerList;
                }
                
                let bucketSecrets = [];
                if(([1,3].includes(type) || type == 'all') && Array.isArray(secretList?.db_secrets)){
                    const secretNames = [];
                    secretAndServerList = secretList.db_secrets.map(secret => {
                        let bucket = 'no';
                        if(!secretList.metadata[secret]) secretNames.push(secret);
                        if((secretList.metadata[secret] ||'').startsWith('s3://')) {
                            bucket = 'yes', bucketSecrets.push({ name: secret, bucket });
                        }
                        return { name: secret, host: secretList.metadata[secret] || 'None', bucket };
                    });
                    cb({dbSecrets: secretAndServerList, secretNames});
                    if(type == 'all') allSecrets['db'] = secretAndServerList;
                }
                
                if(type === 3) return bucketSecrets;

                if(Object.keys(allSecrets).length > 0) return allSecrets;
                return (secretAndServerList || []).length > 0 ? secretAndServerList : [];
    
            } else {
                const result = await response.json();
                AppTemplate.toast.error(result.result);
            }

        } catch (error) {
            return [];
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
                result.result.schema_based = true;
                const allTables = {}, schemas = Object.entries(result.result.tables);

                for(const [schema, tableList] of schemas){
                    const tables = Object.entries(tableList);
                    for(const [table, fields] of tables)
                        allTables[`${schema}.${table}`] = fields;
                }
                result.result['tables'] = allTables;
            }
            return result.result;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    /** @returns { { fields } | undefined } */
    static async getDBTableDetails(dbEngine, connectionName, tableName) {

        const namespace = await UserService.getNamespace();
        const url = `/${namespace}/db/${dbEngine}/${connectionName}/${tableName}`;

        const response = await $still.HTTPClient.get(url);
        const result = await response.json();
        
        if (response.ok && !result.error)
            return result.result;
        else
            AppTemplate.toast.error(result.result);
    }

    /** @returns { { fields } | undefined } */
    static async getTransformationPreview(
        connectionName, previewScript, dbEngine = null, 
        sourceType = null, fileSource = null
    ) {
        
        const namespace = await UserService.getNamespace();
        const url = `/${namespace}/db/transformation/preview`;

        try {
            const response = await $still.HTTPClient.post(url, JSON.stringify(
                { connectionName, previewScript, dbEngine, sourceType, fileSource }
            ),{
                headers: { 'content-type': 'Application/json' }
            });
            const result = await response.json();
            
            if (response.ok && !result.error)
                return result.result;
            else{
                if(result.result.code === '\n')
                    AppTemplate.toast.error('No row transformation was processed');
                else
                    AppTemplate.toast.error(result.result.msg);

                return { ...result.result, error: true }
            }
        } catch (error) { return null; }

    }

    async pausePipelineScheduledJob(){
        const namespace = await UserService.getNamespace();
        const pipeline = WorkspaceService.currentSelectedPpeline;
        let pplineJobStatus = WorkspaceService.currentSelectedPpelineStatus;

        if(pplineJobStatus !== 'paused') pplineJobStatus = 'paused';
        else pplineJobStatus = 'active'

        const url = `/ppline/schedule/${namespace}/${pipeline}/${pplineJobStatus}`;

        const response = await $still.HTTPClient.post(url);
        const result = await response.json();
        
        if (response.ok && !result.error){
            AppTemplate.toast.success(`Pipeline scheduled job ${pplineJobStatus === 'paused' ? pplineJobStatus : 'resumed'}`);
            return true;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    static async getBucketObjects(secretName){
        const namespace = await UserService.getNamespace();

        const url = `/workspace/${namespace}/s3/${secretName}/objects/`;

        const response = await $still.HTTPClient.post(url);
        const result = await response.json();
        
        if (response.ok && !result.error){
            return result.result.map(obj => {
                const filenamePieces = obj?.key?.split('.');
                return { label: `${filenamePieces.slice(0,-1)}*.${filenamePieces.slice(-1)}`, name: obj?.key }
            });
        }
        else
            AppTemplate.toast.error(result.result);
    }

    static async getBucketObjectFields(secretName, object){
        const namespace = await UserService.getNamespace();
        if(String(secretName).length == 0) return;
        const url = `/workspace/${namespace}/s3/${secretName}/${object}/preview`;

        const response = await $still.HTTPClient.post(url);
        const result = await response.json();
        
        if (response.ok && !result.error){
            return result.result;
        }
        else
            AppTemplate.toast.error(result.result);
    }

    static async getLogsExecutionsId(){
        const namespace = await UserService.getNamespace();
        const url = `/logs/execution-ids/${namespace}`;
        const response = await $still.HTTPClient.get(url);
        const result = await response.json();        

        if (response.ok && !result.error) return result;
    }

    /** @returns { Array } */
    static async getLogs(filters){
        const namespace = await UserService.getNamespace();
        const url = `/logs/${namespace}`;
        const response = await $still.HTTPClient.post(url,JSON.stringify({ filters }), {
                headers: { 'content-type': 'Application/json' }
        });
        const result = await response.json();
        try {
            if (response.ok && !result.error) {
                
                return {
                    'all_logs': result['all_logs'].length > 0 ? result['all_logs'] : [],
                    'logs_summary': result['logs_summary'].length > 0 ? result['logs_summary'] : [],
                    'stats': result['stats'],
                }
            }
        } catch (error) {
            return { all_logs: [], logs_summary: [] }
        }
    }

}