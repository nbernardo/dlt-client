import { TransformRow } from "../transform/TransformRow.js";
import { IsObject } from "../transform/util.js";
import { Transformation } from "../Transformation.js";

export function parseAggregation(script, transformation, prevAggreg){
    
    // Bellow if statement starts the aggregator scope, which is closed outside this function
	if(transformation.field !== prevAggreg)
		script += `lf = lfquery.group_by('${transformation.field}').agg(`;

	script += `\n\t\t\t${transformation.aggreg},`;
	return [script, transformation.field];
}

export function parseScript({
	script, tableName, transformType, count, transformCount, finalScript,
	transformation, transformations, totalTransform, isFile
}){

	script = script.replace(transformation,'pl.all()');
    // In case the previous transformation was aggregation then the lf will
    // only consider the fields in the groupby, hence we're reseting it here
    if(IsObject(transformations[count - 1]))
        script += `lf = lfquery.with_columns(pl.all())\n\t`;
    

	if(transformType.isDedupTransform)
		script += transformation.replace('df.unique(subset=','lf = lf.unique(subset=')+'\n\t';

	if(transformType.isDropTransform)
		script += transformation.replace('df.drop([','lf = lf.drop([')+'\n\t';
					
	if(transformType.isFilterTransform)
		script += transformation.replace('df.filter(','lf = lf.filter(')+'\n\t';

	[script, count] = Transformation.parseTransformResult(script, count, tableName, transformType.isDropTransform);

	finalScript += script;
	if(totalTransform > 1 && !IsObject(transformations[transformCount + 1]) && transformations[transformCount + 1] != undefined){
		script = `\n\ntry:\n\t`;
        
        if(!IsObject(transformations[count])){ //Check current transformation if it's not aggregation
            if(isFile){
                transformCount++;
		        script += `lf = lfquery.with_columns(${transformations[count]})\n\t`;
            }else
		        script += `lf = lfquery.with_columns(${transformations[++transformCount]})\n\t`;
        }

		script = script
				.replace(transformation+',','')
				.replace(transformation,'')
	}else{
		script = '\n\ntry:\n\t';
    }

	return [ script, count, transformCount, finalScript ]

}

export function parseTransformResult(script, count, tableName, isDropTransform){
	script += `result = lf.${isDropTransform ? 'limit(2)' : 'limit(20)'}.collect()\n\t`;
	script += `results.append({ 'columns': result.columns, 'data': result.rows(), 'table': '${tableName}' })\n`;
	script += `except Exception as err:\n\t`;
	script += `print(f'Error #${count}#: {str(err)}')\n\t`;
	script += `raise Exception(f'Error #${count++}#: {str(err)}')\n\n`;
	return [script, count];
}

export function parseFilter(
	{ script, count, tableName, otherValidTransform, isSplitTransform, isNewField, finalScript, isFile,
		newFieldRE, isCalcTransform, transformation, transformations, transformCount, totalTransform
	 }
){
	let filterInstruction = '';
	if(!otherValidTransform){
		filterInstruction = transformation.split('pl.when(')[1];
		filterInstruction = filterInstruction.split(').then')[0];
	}else{
		if(isSplitTransform)
			filterInstruction = `${transformation.split('.str')[0]}.is_not_null()`;
		else if(isCalcTransform)
			filterInstruction = `pl.col${transformation.split('alias')[1]}.is_not_null()`;
	}

	script += `result = (lf.filter(${filterInstruction}).limit(5).collect())\n\t`;
	script += `results.append({ 'columns': result.columns, 'data': result.rows(), 'table': '${tableName}' })\n`;
	script += `except Exception as err:\n\t`;
	script += `print(f'Error #${count}#: {str(err)}')\n\t`;
	script += `raise Exception(f'Error #${count++}#: {str(err)}')`;
	//In case there is any deduplicate transformation in place
	script = script.replace(/\,{0,1}(\n|\t|\n\t){0,}df\.unique\(subset\=\[[A-Z0-9\']{0,}\]\)\,{0,1}(\n|\t|\n\t){0,}/ig, '');
			
	if(!isNewField)
		script = script.replace(newFieldRE, (mt, $1) =>  mt.replace(`${$1}`,`${$1}_New`));
			
	finalScript += script;

	script = '';
	if(totalTransform > 1 && !IsObject(transformations[transformCount + 1]) && !isFile) {
		//Reinstate things for next transformation
		script = '\n\ntry:\n\t';
        
		const isNextFilterTransform = transformations[transformCount + 1]?.indexOf('df.filter') > -1;

		if(!IsObject(transformations[count]) && !isNextFilterTransform) //Check current transformation if it's not aggregation
		    script += `lf = lfquery.with_columns(${transformations[++transformCount]})\n\t`;
	}else{
		script = '\n\ntry:\n\t', transformCount++;
    }
	return [ script, count, transformCount, finalScript ]
}

/** @returns { TransformRow } */
export function parseTransformException(previewResult, transformRowMapping, fieldRows){
	let transfomRowIndex;
	if(previewResult.msg.search(/Error\s{1}\#(\d)\#/) >= 0) 
		transfomRowIndex = Number(previewResult.msg.split('#')[1]) - 1;
	else{
		let codeRow = previewResult.code.replace('\tlf = ','');
		codeRow = codeRow.replace(newFieldRE, (mt, $1) =>  mt.replace(`${$1}_New`,`${$1}`));
		if(codeRow.endsWith('\n')) codeRow = codeRow.slice(0, codeRow.length - 1);
		transfomRowIndex = Number(transformRowMapping[codeRow]) - 1;
	}
	return [...fieldRows][transfomRowIndex][1];
}