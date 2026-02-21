import { sleepForSec } from "../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { NodeTypeInterface } from "../mixin/NodeTypeInterface.js";
import { SqlDBComponent } from "../SqlDBComponent.js";
import { Transformation } from "../Transformation.js";
import { TransformExecution } from "../util/tranformation.js";
import { Aggreg } from "./aggregation/Aggreg.js";
import { addAggregation, aggregationRemNotify, handleConfigData, onDataSourceSelect, showHideAggregations } from "./util.js";

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
	/** @Prop */ isImport;
	/** @Prop */ databaseFields; //This is in particular when the data source is DB (e.g. SQL)
	/** @Prop */ configData = null; // This is only used when importing/reviewing a previous created node
	/** @Prop */ isNewField = false; // This is for shiwing input text for new field name creation
	/** @Prop */ isSourceSQL = false;

	dataSourceList;
	selectedSource;
	selectedField;
	selectedType;
	transformation = '';
	separatorChar;
	fieldList = []; //Setting initial value
	/** @Prop @type { Map<String, Aggreg> } */ aggregations = new Map();
	/** @Prop */ tableSource;

	/** @type { Transformation } */
	$parent;

	stOnRender({ dataSources, rowId, importFields, tablesFieldsMap, isImport, isNewField }) {
		this.fieldList = Array.isArray(tablesFieldsMap) ? [{name: '- No Field -'}, ...tablesFieldsMap] : tablesFieldsMap;
		this.databaseFields = tablesFieldsMap, this.rowId = rowId, this.isImport = isImport;
		this.configData = { ...importFields, dataSources };		
		this.isNewField = (isNewField === true || importFields?.isNewField === true) ? true : false;
	}

	async stAfterInit() {
		
		this.$parent.transformPieces.set(this.rowId, { isNewField : this.isNewField });
		this.tableSource = this.$parent.$parent.controller.importingPipelineSourceDetails?.tables;
		
		this.dataSourceList = this.configData.dataSources;
		this.isSourceSQL = this.$parent.dataSourceType === 'SQL' || this.$parent?.sourceNode?.getName() === SqlDBComponent.name;
		if(this.isImport && this.isSourceSQL) await sleepForSec(20);

		if(this.isImport === true && String(this.configData.table).indexOf('.') > 0 && this.isSourceSQL){ 
			let schemas = Object.keys(this.tableSource), tablePaths = [];
			for(const schema of schemas){
				const tables = Object.keys(this.tableSource[schema]);
				for(const table of tables) tablePaths.push({ name: `${schema}.${table}` })
			}
			this.dataSourceList = tablePaths;
			await sleepForSec(500);
		}

		this.onChangeSelectedSource();
		this.selectedField.onChange(field => this.updateTransformValue({ field }));
		this.transformation.onChange(value => this.updateTransformValue({ transform: value?.trim() }));
		this.separatorChar.onChange(value => this.updateTransformValue({ sep: value.trim() }));
		this.selectedSource.onChange(table => this.updateTransformValue({ table }));

		this.selectedType.onChange(value => {
			if((value?.trim() === 'FILTER') && this.selectedField.value !== '- No Field -')
				this.selectedField = '- No Field -';

			this.transformType = value?.trim();
			this.updateTransformValue({ type: this.transformType });
		});

		if (this.configData !== null) await this.handleConfigData();

	}

	addAggregation = async () => addAggregation(this);
	aggregationRemNotify = () => aggregationRemNotify(this);
	showHideAggregations = () => showHideAggregations(this);
	handleConfigData = async () => handleConfigData(this); 
	onChangeSelectedSource = () => this.selectedSource.onChange(async (val) => onDataSourceSelect(this, val));

	updateTransformValue(value) {
		const curVal = this.$parent.transformPieces.get(this.rowId);
		this.$parent.transformPieces.set(this.rowId, { ...curVal, ...value });
		if('field' in value) {
			[...this.aggregations].forEach(([_, aggegRow]) => {
				aggegRow.updateAggregField(value.field)
			})
		}
	}

	removeMe() {
		const obj = this;
		function handleDeletion() {
			obj.unload(), obj.$parent.removeField(obj.rowId);
		}
		if ((this.configData !== null && this.$parent.confirmModification === false && this.isImport) && this.$parent.$parent.isAnyDiagramActive) 
			return this.$parent.confirmActionDialog(handleDeletion);
		handleDeletion(this);
		document.getElementById(`aggreg_group_${this.rowId}`).innerHTML = '';
		[...obj.aggregations].forEach(([_, aggreg]) => aggreg.unload());
	}

	displayTransformationError = (error) => TransformExecution.validationErrDisplay(this.rowId, error);

}