import { BaseService, ServiceEvent } from "../../@still/component/super/service/BaseService.js";

export class ObjectDataTypes {
    typeName;
    icon;
    label;
    imgIcon;
};

export class WorkspaceService extends BaseService {

    table = new ServiceEvent([]);

    /** @type { Array<ObjectDataTypes> } */
    objectTypes = [
        { icon: 'fab fa-bitbucket', label: 'Data Source - Bucket', typeName: 'bucket' },
        { icon: 'fas fa-file-alt', label: 'Data Source File', typeName: 'slack' },
        { icon: 'fas fa-cogs', label: 'Transformation', typeName: 'github' },
        {
            imgIcon: '<img src="app/assets/imgs/File-Database--Streamline-Nova.png" style="width: 25px;">',
            label: 'Destination - DBFile (.duckdb)',
            typeName: 'telegram'
        },
        { icon: 'fas fa-file-signature', label: 'File Log', typeName: 'log' },
        { icon: 'fab fa-google-drive', label: 'Google Drive save', typeName: 'google' },
        { icon: 'fas fa-at', label: 'Email send', typeName: 'email' },
        { icon: 'fas fa-code', label: 'Template', typeName: 'template' },
        { icon: 'fas fa-code-branch', label: 'Multiple inputs/outputs', typeName: 'multiple' },
        { icon: 'fas fa-fill', label: 'Personalized', typeName: 'personalized' },
        { icon: 'fas fa-mouse', label: 'DBClick!', typeName: 'dbclick' },

    ]

}