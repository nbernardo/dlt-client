import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { Components } from "../../../@still/setup/components.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { Bucket } from "./Bucket.js";
import { NodeTypeInterface } from "./mixin/NodeTypeInterface.js";
import { TRANFORM_ROW_PREFIX, TransformRow } from "./transform/TransformRow.js";

/** @implements { NodeTypeInterface } */
export class Transformation extends ViewComponent {

	isPublic = true;

	/** @Prop */ inConnectors = 1;
	/** @Prop */ outConnectors = 1;
	/** @Prop */ remainTransformRows = [];
	/** @Prop @type { STForm } */ formRef;
	/** @Prop */ uniqueId = '_'+UUIDUtil.newId();

	/** This will hold all applied transformations
	 * @Prop @type { Map<TransformRow> } */ 
	transformPieces = new Map();

	databaseList = [{ name: '' }];

	/** This will hold all added transform
	 * @Prop @type { Map<TransformRow> } */
	fieldRows = new Map();

	async stAfterInit(){
		await this.addNewField();
	}

	onInputConnection({ data, type }){
		if(type === Bucket.name) {
			this.databaseList = data;
			[...this.fieldRows].forEach(([_,row]) => row.dataSourceList = data);
		}
	}

	async addNewField(){

		const parentId = this.cmpInternalId;
		const rowId = TRANFORM_ROW_PREFIX+''+UUIDUtil.newId();
		const initialData = { dataSources: this.databaseList.value, rowId };

		// Create a new instance of TransformRow component
		const { component, template } = await Components.new('TransformRow', initialData, parentId);
		
		// Add component to the DOM tree as part of transformation
		document.querySelector('.transform-container-'+this.uniqueId).insertAdjacentHTML('beforeend', template);
		this.fieldRows.set(rowId, component);

	}

	removeField(rowId){
		this.transformPieces.delete(rowId);
		this.fieldRows.delete(rowId);
		document.getElementById(rowId).remove();
	}

	
	
}