import { SqlDBComponent } from "../SqlDBComponent.js";
import { Aggreg } from "./aggregation/Aggreg.js";
import { TransformRow } from "./TransformRow.js";

/** @param { TransformRow } obj  */
export async function onDataSourceSelect(obj, newValue){

    let fieldList = null, dataSource;
    if(![null,undefined].includes(obj.tableSource) && obj.configData !== null){
    
        let table = null, schema = null;
        table = tableSource[newValue];
    
        if(String(newValue).indexOf('.') > 0){
            [schema, table] = newValue.split('.');
            table = tableSource[schema][table];
        }
    
        fieldList = table.map(itm => ({ name: itm.column }));
    }else{
        if(!newValue) return;
        if(obj.$parent?.sourceNode?.getName() === SqlDBComponent.name) obj.isSourceSQL = true;
        if(!obj.isSourceSQL){
            dataSource = newValue.length > 0 && newValue.trim().replace('*',''); //If it's file will be filename, id DB it'll be table name
            await obj.wspaceService.handleCsvSourceFields(dataSource)
            fieldList = await obj.wspaceService.getCsvDataSourceFields(dataSource);
        }else{
            if(obj.databaseFields[newValue])
                fieldList = obj.databaseFields[newValue].map(itm => ({ name: itm.column }));
            else
                fieldList = '';
        }
    }
    
    fieldList = fieldList.map(itm => ({ ...itm, name: itm.name.replace(/\"/g,'') }));
    const allFields = [{name: '- No Field -'}, ...fieldList];
    
    obj.fieldList = allFields;
    [...obj.aggregations].forEach(([_, aggreg]) => aggreg.fieldsList = allFields);
    obj.updateTransformValue({ dataSource });

}

/** @param { TransformRow } obj  */
export async function handleConfigData(obj) {
    const { table: dataSource, dataSources, field, type, transform } = obj.configData;
    if(dataSource || dataSources){
        if(dataSources?.length == 1)
            if(dataSources[0].name === '') return
        
        dataSource !== undefined ? obj.selectedSource = dataSource.replace('*','') : '';// || dataSources;
        await sleepForSec(100);
    }
    field !== undefined ? obj.selectedField = field : '';
    type !== undefined ? obj.selectedType = type : '';

    if (type === 'CODE') document.getElementById(`${obj.rowId}-codeTransform`).value = transform;
    if (type === 'FILTER') document.getElementById(`${obj.rowId}-filterTransform`).value = transform;
    if (type === 'CALCULATE') document.getElementById(`${obj.rowId}-calcTransform`).value = transform;
    obj.transformation = (transform || '');
}


export async function addAggregation(obj){

    const { component, template: aggregInstance } = await Components.new(Aggreg, { fieldList: obj.fieldList.value });
    const htmlNode = document.createElement('tr');
    htmlNode.id = `aggreg_row_${component.cmpInternalId}`; 
    htmlNode.innerHTML = aggregInstance, component.$parent = obj;;
    document.getElementById(`aggreg_group_${obj.rowId}`).appendChild(htmlNode);
    obj.aggregations.set(component.cmpInternalId, component);
    if(obj.aggregations.size > 0) document.getElementById(`aggreg_group_${obj.rowId}`).parentElement.style.display = '';
}

export const aggregationRemNotify = (obj) => 
    obj.aggregations.size == 0 ? document.getElementById(`aggreg_group_${obj.rowId}`).parentElement.style.display = 'none' : '';
    
export const showHideAggregations = (obj) => {
    const { display } = document.getElementById(`aggreg_group_${obj.rowId}`).style;
    document.getElementById(`aggreg_group_${obj.rowId}`).style.display = (display === 'none' ? '' : 'none');
}

export function onChangeSelectedSource(obj){
    obj.selectedSource.onChange(async (val) => onDataSourceSelect(this, val));
}