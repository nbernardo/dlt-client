import { ViewComponent } from "../../../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../../../@still/component/type/ComponentType.js";
import { TransformRow } from "../TransformRow.js";

export class Aggreg extends ViewComponent {

	isPublic = false;
	fieldsList = [];
	aggregationType;
	selectedField;
	aggregationAlias;

	/** @Prop */ configData;

	/** @Prop @type { STForm } */ formRef;

	/** @type { TransformRow } */ $parent;

	trueAggregations = [
		// Numeric
		"sum",
		"mean",
		"median",
		"min",
		"max",
		"std",
		"var",
		"quantile",

		// Count
		"len",          // counts rows in group
		"count",        // non-null count
		"n_unique",
		"approx_n_unique",

		// Boolean
		"any",
		"all",

		// Positional
		"first",
		"last",

		// Statistical
		"skew",
		"kurtosis",
		"corr",
		"cov",

		// List reduction
		"implode"
	];

	stAfterInit(){
		this.selectedField.onChange(aggregField => this.updateTransformation(
			{ aggregField, field: this.$parent.selectedField.value, table: this.$parent.selectedSource.value }
		));
		this.aggregationType.onChange(aggregation => this.updateTransformation({ aggregation }));
		this.aggregationAlias.onChange(fieldAlias => this.updateTransformation({ fieldAlias }));

		if(this.configData){
			if(this.configData.aggregField){
				this.selectedField = this.configData.aggregField;
				this.aggregationType = this.configData.aggregation;
				this.aggregationAlias = this.configData.fieldAlias;
			}
		}
	}

	stOnRender = ({ fieldList, ...configs }) => {
		this.fieldsList = fieldList, this.configData = configs?.configs;
	}

	updateAggregField = (aggregField) => 
		this.updateTransformation({ aggregField });

	removeMe(){
		//Traces all aggregation in a given field/TransformRow
		this.$parent.aggregations.delete(this.cmpInternalId);
		//Add the agregation transformation to the Transform component
		this.$parent.$parent.transformPieces.delete(this.cmpInternalId);
		this.$parent.$parent.unregisterAggregation(this.$parent.rowId, this.cmpInternalId);
		this.$parent.aggregationRemNotify();
		try {
			document.getElementById(`aggreg_row_${this.cmpInternalId}`).remove()
		} catch (error) { }
	}

	updateTransformation(value){
		const curVal = this.$parent.$parent.transformPieces.get(this.cmpInternalId) || {};
		this.$parent.$parent.transformPieces.set(this.cmpInternalId, { ...curVal, ...value });
		this.$parent.$parent.registerAggregation(this.$parent.rowId, this.cmpInternalId, { ...curVal, ...value });
	}

}