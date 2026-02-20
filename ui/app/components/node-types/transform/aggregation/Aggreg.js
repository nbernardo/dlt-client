import { ViewComponent } from "../../../../../@still/component/super/ViewComponent.js";
import { TransformRow } from "../TransformRow.js";

export class Aggreg extends ViewComponent {

	isPublic = false;
	fieldsList = [];

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

	stOnRender = ({ fieldList }) => this.fieldsList = fieldList;

	removeMe(){
		document.getElementById(`aggreg_row_${this.cmpInternalId}`).remove();
		this.$parent.aggregations.delete(this.cmpInternalId);
		this.$parent.aggregationRemNotify();
		this.unload();
	}

}