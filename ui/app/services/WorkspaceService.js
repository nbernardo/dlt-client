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
        { icon: 'fab fa-bitbucket', label: 'Input - Bucket', typeName: 'Bucket', source: 0, dest: 1 },
        { icon: 'fas fa-file-alt', label: 'Input File', typeName: 'slack', source: 0, dest: 1 },
        { icon: 'fas fa-cogs', label: 'Transformation', typeName: 'github', source: 1, dest: 1 },
        {
            imgIcon: 'app/assets/imgs/File-Database--Streamline-Nova.png',
            label: 'Out-DBFile (.duckdb)',
            typeName: 'telegram', source: 1, dest: 1
        },
        { icon: 'fab fa-aws', label: 'Save in aws', typeName: 'aws' },
        { icon: 'fas fa-file-signature', label: 'File Log', typeName: 'log' },
        { icon: 'fas fa-fill', label: 'Personalized', typeName: 'personalized' },
        { icon: 'fas fa-mouse', label: 'DBClick!', typeName: 'dbclick' },

    ]

}