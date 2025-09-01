import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { Transformation } from "../Transformation.js";

export const TRANFORM_ROW_PREFIX = 'transformRow';

export class TransformRow extends ViewComponent {

	isPublic = true;

	/**
	 * @Inject @Path services/
	 * @type { WorkspaceService } */
	wspaceService;

	/** @Prop */ rowId;
	/** @Prop */ transformType;

	dataSourceList;
	selectedSource;
	selectedField;
	selectedType;
	transformation;
	fieldList = [{ name: '' }]; //Setting initial value

	/** @type { Transformation } */
	$parent;

	stOnRender({ dataSources, rowId }){
		this.dataSourceList = dataSources;
		this.rowId = rowId;
	}

	async stAfterInit(){
		
		this.selectedSource.onChange(async (newValue) => {
			const fileName = newValue.trim();
			await this.wspaceService.handleCsvSourceFields(fileName)
			const fieldList = await this.wspaceService.getCsvDataSourceFields(fileName);
			this.fieldList = fieldList;
		});

		this.selectedField.onChange(fieldName => {
			const curVal = this.$parent.transformPieces.get(this.rowId) || {};
			this.$parent.transformPieces.set(this.rowId, {...curVal, field: fieldName});
		});

		this.selectedType.onChange(value => {
			this.transformType = value.trim();
		});

	}

	removeMe(){
		this.unload();
		this.$parent.removeField(this.rowId);
	}

}