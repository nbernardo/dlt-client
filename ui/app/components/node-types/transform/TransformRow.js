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
	separatorChar;
	fieldList = [{ name: '' }]; //Setting initial value

	/** @type { Transformation } */
	$parent;

	stOnRender({ dataSources, rowId, importFields }){
		this.dataSourceList = dataSources;
		this.rowId = rowId;
		console.log(`NEW ROW CREATED: `, importFields);
		
	}

	async stAfterInit(){
		
		this.$parent.transformPieces.set(this.rowId, {})

		this.selectedSource.onChange(async (newValue) => {
			const dataSource = newValue.trim(); //If it's file will be filename, id DB it'll be table name
			await this.wspaceService.handleCsvSourceFields(dataSource)
			const fieldList = await this.wspaceService.getCsvDataSourceFields(dataSource);
			this.fieldList = fieldList;
			this.updateTransformValue({ dataSource });
		});

		this.selectedField.onChange(field => this.updateTransformValue({ field }));
		this.transformation.onChange(value => this.updateTransformValue({ transform: value.trim() }));
		this.separatorChar.onChange(value => this.updateTransformValue({ sep: value.trim() }));

		this.selectedType.onChange(value => {
			this.transformType = value.trim();
			this.updateTransformValue({ type: this.transformType });
		});

	}

	updateTransformValue(value){
		const curVal = this.$parent.transformPieces.get(this.rowId);
		this.$parent.transformPieces.set(this.rowId, {...curVal, ...value});
	}

	removeMe(){
		this.unload();
		this.$parent.removeField(this.rowId);
	}

}