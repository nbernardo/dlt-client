import { FormHelper } from "../../../../@still/helper/form.js";
import { SqlDBComponent } from "../SqlDBComponent.js";

/**
 * @param { SqlDBComponent } self 
 */
export function addSQLComponentTableField(self, tableId, value = '', disabled = false, isOld = false){
    
    let tblFieldName = `tableName` + tableId, placeholder = 'Enter table ' + tableId + ' name', pkFieldName;
    const table = FormHelper
        .newField(self, self.formRef, tblFieldName, value)
        .input({ required: true, placeholder, validator: 'text', value, disabled, className: `dynamic-db-table-field ${tblFieldName}` })
        //Add will add in the form which reference was specified (2nd param of newField)
        //.add((inpt) => `<div style="padding-top:5px;">${inpt}</div>`);
        .element;

    pkFieldName = 'primaryKey' + tableId, placeholder = 'PK Field';
    const pkField = FormHelper
        .newField(self, self.formRef, pkFieldName, value)
        .input({ required: true, placeholder, validator: 'text', value, disabled, className: `dynamic-db-table-field ${pkFieldName}` })
        .element;

    let div = document.createElement('div'), delBtn;
    
    if(!isOld){
        delBtn = document.createElement('div');
        delBtn.className = 'remove-dyn-field';
        delBtn.innerHTML = 'x';
        delBtn.onclick = () => removeMe();
        function removeMe() {
            FormHelper.delField(self, self.formRef, pkFieldName);
            FormHelper.delField(self, self.formRef, tblFieldName);
            delete self.selectedTablesName[tblFieldName];
            div.remove();
        }
        if(!Array.isArray(self.removeAddedTableCallbacks)) self.removeAddedTableCallbacks = [];
        self.removeAddedTableCallbacks.push(removeMe);
    }

    div.style.marginTop = '3px';
    div.className = 'table-detailes';
    div.innerHTML = `${table}${pkField}`;

    if(!isOld) div.insertAdjacentElement('beforeend', delBtn);

    document.querySelector(`.${self.formWrapClass} form`).appendChild(div);

    //Add the filter result list
    setTimeout(() => {
        self.handleTableFieldsDropdown(`.${tblFieldName}`, `.${pkFieldName}`);
    }, 600);

}