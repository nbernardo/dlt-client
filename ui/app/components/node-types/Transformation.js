import { sleepForSec } from "../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { STForm } from "../../../@still/component/type/ComponentType.js";
import { Components } from "../../../@still/setup/components.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { Workspace } from "../workspace/Workspace.js";
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
	/** @Prop */ uniqueId = '_' + UUIDUtil.newId();
	/** @Prop */ showLoading = false;
	/** @Prop */ confirmModification = false;

	/** This will hold all applied transformations
	 * @Prop @type { Map<TransformRow> } */
	transformPieces = new Map();

	databaseList = [{ name: '' }];

	/** This will hold all added transform
	 * @Prop @type { Map<TransformRow> } */
	fieldRows = new Map();

	/** This will only hold the row when importive/viewing 
	 *  previously created pipeline which has transformation
	 *  as one of its steps, and it's filled automatically by 
	 * the the WorkspaceController at processImportingNodes
	 * @Prop @type { Array } */
	rows = null;

	/** @Prop */
	reserved = [' and', ' or', ' not', ' None', ' else', ' if'];

	/** @Prop */ nodeId;
	/** @Prop */ isImport;

	/** @type { Workspace } */ $parent;

	stOnRender({ nodeId, isImport }) {
		this.nodeId = nodeId;
		this.isImport = isImport;
		if (isImport === true) {
			this.showLoading = true;
		}
	}

	async stAfterInit() {

		if (this.isImport && this.rows !== null) {
			for (const rowConfig of this.rows)
				await this.addNewField(rowConfig, true);
			await sleepForSec(500);
			this.showLoading = false;
			return
		}
		if (this.showLoading === true) this.showLoading = true;
		await this.addNewField();

	}

	onInputConnection({ data, type }) {
		if (type === Bucket.name) {
			this.databaseList = data;
			[...this.fieldRows].forEach(([_, row]) => row.dataSourceList = data);
		}
	}

	async addNewField(data = null, inTheLoop = false) {

		const obj = this;

		if(this.isImport === true && inTheLoop === false && this.confirmModification === false){
			this.confirmActionDialog(handleAddField);
		}else handleAddField();
		
		async function handleAddField() {
			const parentId = obj.cmpInternalId;
			const rowId = TRANFORM_ROW_PREFIX + '' + UUIDUtil.newId();
			const initialData = { dataSources: obj.databaseList.value, rowId, importFields: data };

			// Create a new instance of TransformRow component
			const { component, template } = await Components.new('TransformRow', initialData, parentId);

			// Add component to the DOM tree as part of transformation
			document.querySelector('.transform-container-' + obj.uniqueId).insertAdjacentHTML('beforeend', template);
			obj.fieldRows.set(rowId, component);
		}

	}

	removeField(rowId) {
		this.transformPieces.delete(rowId);
		this.fieldRows.delete(rowId);
		document.getElementById(rowId).remove();
	}

	parseTransformationCode() {
		let finalCode = "";
		const rowsConfig = [];		
		for (const [_, code] of [...this.transformPieces]) {
			
			let { type, field, transform } = code;
			
			rowsConfig.push(code);
			field = `df['${field}']`;

			if (type === 'CODE')
				finalCode += `\n${this.parseCodeOnDf(transform, code.field)}`;

			if (type === 'CASING')
				finalCode += `\n${this.parseCasing(transform, code.field)}`;

			if (type === 'CALCULATE')
				finalCode += `\n${field} = ${this.parseCalculate(transform)}`;

			if (type === 'SPLIT')
				finalCode += `\n${this.parseSplit(field, code.sep, transform)}`;

			if (type === 'DEDUP')
				finalCode += `\ndf.drop_duplicates(subset=['${code.field}'], inplace=True)`;

			if (type === 'CONVERT') {
				if (transform === 'date') finalCode += `\n${field} = pd.to_datetime(${field})`;
				else finalCode += `\n${field} = ${field}.astype(${transform})`;
			}

		}

		console.log(`VALUE IS: `, finalCode);

		const data = WorkSpaceController.getNode(this.nodeId).data;
		data['code'] = finalCode;
		data['rows'] = rowsConfig;

		return finalCode;

	}

	parseCasing(transform, field) {
		return 'UPPER' === transform
			? `df['${field}'] =  df['${field}'].str.upper()`
			: 'LOWER' === transform ? `df['${field}'] = df['${field}'].str.lower()`
				: `# df['${field}'] Unchenged case`;
	}

	parseCodeOnDf(transform, field) {

		let matchCount = 0, addSpace = '';
		const isMultiConditionCode = [' and ', ' or '].includes(transform);
		const regex = /.split\([\s\S]{1,}\)\[[0-9]{1,}\]|\.[A-Z]{1,}\(|s{0,}\'[\sA-Z]{1,}\'|\s{0,}[A-Z]{1,}/ig;
		const hasSplit = transform.indexOf('.split(') > 0;

		const parsePieces = (wrd, pos, transform, side = null) => {
			matchCount++;

			// In case split is being used
			if (wrd.indexOf('.split(') == 0 && wrd.endsWith(']')) {
				return wrd.replace('.', '.str.').replace(')[', ').str[');
			}


			if (wrd.startsWith('.') && wrd.endsWith('('))
				return wrd.replace('.', '.str.');

			if (wrd.trim() == "' '") return wrd

			const isWordBitwise = ['and', 'or'].includes(wrd.trim());
			if (isWordBitwise) return `) ${wrd} (`;
			if (matchCount > 1) addSpace = ' ';

			if (
				pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\"") && !transform[pos - 1]?.startsWith("'")
				|| pos > 0 && !transform[pos - 1]?.startsWith("'") && !transform[pos - 1]?.startsWith("\"")
			)
				return isMultiConditionCode ? `(${addSpace}df['${wrd.trim()}']` : `${addSpace}df['${wrd.trim()}']`;
			else {
				const isLiteral = (wrd.startsWith("'") && wrd.endsWith("'")) || (wrd.endsWith('"') && wrd.startsWith("'"))

				if (hasSplit && side == 'right' && !isLiteral)
					return `df.loc[condition, '${wrd}']`;
				else if (isLiteral || (transform[pos - 1]?.startsWith("'") || transform[pos - 1]?.startsWith("\"")))
					return wrd
				return `df['${wrd}']`;
			}
		}

		let [leftSide, rightSide] = transform.split(' then ');
		leftSide = leftSide?.trim()?.replace(regex, (wrd, pos) => {
			return parsePieces(wrd, pos, transform);
		});

		matchCount = 0, addSpace = '';
		rightSide = rightSide?.trim()?.replace(regex, (wrd, pos) => {
			return parsePieces(wrd, pos, rightSide, 'right');
		});

		const condition = `condition = ${leftSide.replaceAll(' and ', ' & ').replaceAll(' or ', ' | ').replaceAll(`''`,`'`)}`;
		return `${condition}\ndf.loc[condition, '${field}'] = ${rightSide}`;
	}

	parseCode(transform, field) {

		transform = transform.replace(/\s{0,}[A-Z]{1,}/ig, (wrd, pos) => {

			if (this.reserved.includes(wrd)) return `${wrd} `;

			if (pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\"") && !transform[pos - 1].startsWith("'"))
				return `df['${wrd.trim()}']`;
			else if (pos > 0 && !transform[pos - 1].startsWith("'") && !transform[pos - 1].startsWith("\""))
				return `df['${wrd.trim()}']`;
			else return wrd;

		});

		return `${field} = ${transform}`
	}

	parseCalculate(transform) {
		return transform.replace(/\s{0,}[A-Z]{1,}/ig, (wrd, pos) => {
			if (pos === 0 && !wrd.startsWith("'") && !wrd.startsWith("\""))
				return ` df['${wrd.trim()}'] `;
			else if (pos > 0 && !transform[pos - 1].startsWith("'") && !transform[pos - 1].startsWith("\""))
				return ` df['${wrd.trim()}'] `;
			else return wrd;

		});
	}

	parseSplit(field, sep, vars) {
		vars = vars.replace(/\[|\]/g, '').split(',');
		let content = `${vars} = ${field}.str.split('${sep}', n=${vars.length})\n`;

		for (const field of vars)
			content += `df['${field.trim()}'] = ${field}\n`;

		return `${content}`;
	}

	/** @param { Function } confirmEvent */
	confirmActionDialog(confirmEvent) {

		const message = `Removing/Adding a new transformation rule will override the existing data structure, and create new version of the pipeline. <br><br>Do you whish to proceed?`;
		this.$parent.controller.showDialog(message, {
			onConfirm: async () => {
				this.confirmModification = true;
				await confirmEvent(this);
			}
		});

	}

}