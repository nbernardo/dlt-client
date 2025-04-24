import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";

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
        { icon: 'fab fa-bitbucket', label: 'Input - Bucket', typeName: 'Bucket' },
        { icon: 'fas fa-file-alt', label: 'Input File', typeName: 'slack' },
        { icon: 'fas fa-cogs', label: 'Transformation', typeName: 'github' },
        {
            imgIcon: 'app/assets/imgs/duckdb-icon.svg',
            label: 'Out-DBFile (.duckdb)',
            typeName: 'DuckDBOutput'
        },
        { icon: 'fab fa-aws', label: 'Save in aws', typeName: 'aws' },
        { icon: 'fas fa-file-signature', label: 'File Log', typeName: 'log' },
        { icon: 'fas fa-fill', label: 'Personalized', typeName: 'personalized' },
        { icon: 'fas fa-mouse', label: 'DBClick!', typeName: 'dbclick' },

    ]

}