import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { Transformation } from "../Transformation.js";

export const TRANFORM_ROW_PREFIX = 'transformRow';

/** @implements { NodeTypeInterface } */
export class TransformRow extends ViewComponent {

	isPublic = true;

	/**
	 * @Inject @Path services/
	 * @type { WorkspaceService } */
	wspaceService;

	/** @Prop */ rowId;
	/** @Prop */ transformType;
	/** @Prop */ databaseFields; //This is in particular when the data source is DB (e.g. SQL)
	/** @Prop */ configData = null; // This is only used when importing/reviewing a previous created node

	dataSourceList;
	selectedSource;
	selectedField;
	selectedType;
	transformation;
	separatorChar;
	fieldList = [{ name: '' }]; //Setting initial value

	/** @type { Transformation } */
	$parent;

	stOnRender({ dataSources, rowId, importFields }) {
		this.dataSourceList = dataSources;
		this.rowId = rowId;

		if (importFields) this.configData = importFields;

	}

	async stAfterInit() {

		this.$parent.transformPieces.set(this.rowId, {})

		this.selectedSource.onChange(async (newValue) => {

			let fieldList = null, dataSource;

			dataSource = newValue.trim().replace('*',''); //If it's file will be filename, id DB it'll be table name
			if(this.$parent.dataSourceType !== 'SQL'){
				await this.wspaceService.handleCsvSourceFields(dataSource)
				fieldList = await this.wspaceService.getCsvDataSourceFields(dataSource);
			}else{				
				fieldList = this.databaseFields[newValue].map(itm => ({ name: itm.column }))
			}
			
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

		if (this.configData !== null) this.handleConfigData();

	}

	handleConfigData() {
		const { dataSource, field, type, transform } = this.configData;
		this.selectedSource = dataSource;
		this.selectedField = field;
		this.selectedType = type;

		if (type === 'CODE') document.getElementById(`${this.rowId}-code`).value = transform;
		this.transformation = transform;
	}

	updateTransformValue(value) {
		const curVal = this.$parent.transformPieces.get(this.rowId);
		this.$parent.transformPieces.set(this.rowId, { ...curVal, ...value });
	}

	removeMe() {
		const obj = this;
		function handleDeletion() {
			obj.unload();
			obj.$parent.removeField(obj.rowId);
		}

		if (this.configData !== null && this.$parent.confirmModification === false) 
			return this.$parent.confirmActionDialog(handleDeletion);
		handleDeletion(this);
	}

	async getSQLTableFields(){
		const data = await WorkspaceService.getDBTableDetails(this.selectedDbEngine.value, this.selectedSecret.value ,table);
		const pkRelatedField = self.relatedFields[0];
		pkRelatedField.setDataSource(data.fields);
	}
}