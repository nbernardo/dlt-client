import { $still } from "../../@still/component/manager/registror.js";
import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";
import { Bucket } from "../components/node-types/Bucket.js";
import { DuckDBOutput } from "../components/node-types/DuckDBOutput.js";
import { SqlDBComponent } from "../components/node-types/SqlDBComponent.js";

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

    /** @type { Array<ObjectDataTypes> } */
    objectTypes = [
        { icon: 'far fa-circle', label: 'Start', typeName: 'Start', source: 0, dest: 1 },
        { icon: 'fas fa-circle', label: 'End', typeName: 'End', source: 1, dest: 0 },
        { icon: 'fab fa-bitbucket', label: 'Input - Bucket', typeName: Bucket.name },
        { imgIcon: 'app/assets/imgs/sql-server-2.png', label: 'Input - SQL DB', typeName: SqlDBComponent.name },
        { icon: 'fas fa-file-alt', label: 'Input File', typeName: 'slack' },
        { icon: 'fas fa-cogs', label: 'Transformation', typeName: 'github' },
        {
            imgIcon: 'app/assets/imgs/duckdb-icon.svg',
            label: 'Out-DBFile (.duckdb)',
            typeName: DuckDBOutput.name
        },
    ]

    async runCode(code){

        const url = '/workcpace/code/run';
        const result = await $still.HTTPClient.post(url, JSON.stringify(code), {
            headers: {
                'Content-Type': 'application/json'
            }
        });

        return result;

    }

    async getDuckDbs(){
        const url = '/workcpace/duckdb/list';
        const result = await $still.HTTPClient.post(url, null, {
            headers: {
                'Content-Type': 'application/json'
            }
        });
        return result;
    }

}