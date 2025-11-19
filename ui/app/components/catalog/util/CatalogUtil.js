import { FormHelper, InParams } from "../../../../@still/helper/form.js";
import { CatalogForm } from "../CatalogForm.js";

export class CatalogEndpointType {

    /** @type { String } */
	apiEndpointPath;
    
    /** @type { String } */
	apiEndpointPathPK;
    
    /** @type { String } */
	paginationStartField;
    
    /** @type { String } */
	paginationLimitField;
    
    /** @type { String } */
	paginationRecPerPage;

}

export class EndpointGroupType {
    /** @type { Array } */
    fieldList;

    /** @type { CatalogForm } */
    component;
    
    /** @type { CatalogForm } */
    endpointCounter;
    
    /** @type { Array } */
    paginateFields;
    
    /** @type { CatalogEndpointType } */
    details;
}


function getApiSecretFields() {
    const apiFormContainer = 'catalog-form-secret-api';
    let fields = `.${apiFormContainer} .api-tkn-field, .${apiFormContainer} .hidden-tkn-secret-value-api, .api-tkn-lbl`;
    const bearerTokenFields = document.querySelectorAll(fields);

    fields = `.${apiFormContainer} .api-key-field, .${apiFormContainer} .hidden-secret-value-api, .api-key-lbl`;
    const apiKeyFields = document.querySelectorAll(fields);
    return { bearerTokenFields, apiKeyFields };
}

export function onAPIAuthChange(type = null) {

    const { bearerTokenFields, apiKeyFields } = getApiSecretFields();
    if (type == null || type == "") {
        [...bearerTokenFields, ...apiKeyFields].forEach(field => {
            field.style.display = 'none';
            if (field.nodeName === 'INPUT') field.removeAttribute('required');
        });
        return;
    }

    if (type === 'bearer-token') {
        bearerTokenFields.forEach(field => {
            field.style.display = '';
            if (field.nodeName === 'INPUT') field.setAttribute('required', true);
        });
        apiKeyFields.forEach(field => {
            field.style.display = 'none';
            if (field.nodeName === 'INPUT') field.removeAttribute('required');
        });
    } else {
        bearerTokenFields.forEach(field => {
            field.style.display = 'none';
            if (field.nodeName === 'INPUT') field.removeAttribute('required');
        });
        apiKeyFields.forEach(field => {
            field.style.display = '';
            if (field.nodeName === 'INPUT') field.setAttribute('required', true);
        });
    }
    return type;
}

export function viewSecretValue(fieldContainer, dataBaseSettingType, selectedSecretType) {

    let type = 'catalog-form-secret-api', secretType = '-api';
    if (dataBaseSettingType !== null && selectedSecretType != 2) {
        type = dataBaseSettingType == 1 ? 'catalog-form-db-fields' : 'catalog-form-secret-group';
        secretType = dataBaseSettingType == 1 ? '-db' : '';
    }

    const clsName = fieldContainer == 'initial' ? `.initial-secret-field${secretType}` : `.${type} .${fieldContainer}`;
    const currentVal = document.querySelector(`${clsName} input`).type;

    if (currentVal == 'text') {
        document.querySelector(`${clsName} input`).type = 'password';
        document.querySelector(`${clsName} img`).src = 'app/assets/imgs/view-svgrepo-com.svg';
    } else {
        document.querySelector(`${clsName} input`).type = 'text';
        document.querySelector(`${clsName} img`).src = 'app/assets/imgs/view-hide-svgrepo-com.svg';
    }

}

export function markOrUnmarkAPICatalogRequired() {
    document.querySelectorAll('.catalog-form-secret-api .use-pagination-field')
        .forEach(field => {
            if (value) // Mark fields as required
                field.querySelector('input')?.setAttribute('required', true);
            else // Unmark fields as required
                field.querySelector('input')?.removeAttribute('required');
            field.style.display = value ? '' : 'none';
        });
}


/**
 * @param { CatalogForm } component 
 * @param { CatalogEndpointType } details
 */
export function handleAddEndpointField(endpointCounter, component, details) {

    /** @type { EndpointGroupType } */
    const self = { fieldList: [], component, endpointCounter, paginateFields: [], details };
    const fieldSet = document.createElement('fieldset');
    const legend = document.createElement('legend');
    legend.innerText = 'Endpoint ' + endpointCounter;
    fieldSet.appendChild(legend);
    fieldSet.className = `endpointSettings${endpointCounter}`;

    const dataSetting = document.createElement('div');
    dataSetting.className = 'endpoint-data-setting';

    const formGroup1 = document.createElement('div'), formGroup2 = document.createElement('div'), formGroup3 = document.createElement('div');
    formGroup1.className = 'form-group use-pagination-field', formGroup2.className = 'form-group use-pagination-field', formGroup3.className = 'form-group use-pagination-check';

    formGroup1.insertAdjacentHTML('afterbegin', `<label>Path</label>`);
    
    const delEntpointBtn = document.createElement('div');
    delEntpointBtn.className = 'del-endpoint-setting-icon';
    delEntpointBtn.innerText = 'Remove';
    fieldSet.appendChild(delEntpointBtn);

    // Creates the field for entering the endpoint
    let fieldName = `apiEndpointPath${endpointCounter}`;
    const endpointField = newStilComponentField(self, 
        { fieldName, required: true, placeholder: 'e.g. /transaction/paginate', className: ' endpoint-input' }
    );
    formGroup1.insertAdjacentHTML('beforeend', endpointField);

    // Creates the field for entering the endpoint data primary key
    fieldName = `apiEndpointPathPK${endpointCounter}`;
    const primaryKeyField = newStilComponentField(self, { required: true, placeholder: 'e.g. transactionId', fieldName });
    formGroup2.insertAdjacentHTML('afterbegin', `<label>Primary key</label>${primaryKeyField}`);
    
    addPaginateOption(self, formGroup3, endpointCounter, component);

    dataSetting.appendChild(formGroup1);
    dataSetting.appendChild(formGroup2);
    dataSetting.appendChild(formGroup3);

    fieldSet.appendChild(dataSetting);
    
    component.dynamicEndpointsDelButtons.push(delEntpointBtn);
    delEntpointBtn.onclick = function(){
        for(const fieldName of self.fieldList)
            FormHelper.delField(component, component.formRef, fieldName);
        fieldSet.remove();
    }
    document.querySelector(`.catalog-form-secret-api .endpoint-group-config`).appendChild(fieldSet);

    if(details){
        if(details.paginationLimitField != ''){
            addPaginateEndpoint(self,endpointCounter);
            const yesRadioButton = document.querySelector(`input[name="endpointField${endpointCounter}"]`);
            yesRadioButton.checked = true, 1000;
        }
    }

}

/** @param { EndpointGroupType } self */
function newStilComponentField(self, { fieldName, placeholder = '', required, className, paginateField }){

    const obj = self.component;

    /** @type { InParams } */
    const settings = { required: true, placeholder }; 
 
    if(self.details){        
        const fieldCategory = fieldName.slice(0, fieldName.length - 1);
        settings.value = self.details[fieldCategory];
    }
    
    if(paginateField)
        self.paginateFields.push(fieldName);

    if(className) {
        if(settings.className) settings.className += ' '+className;
        else settings.className += className;
    }
    if(required) settings.required = required;
    
    self.fieldList.push(fieldName);

    return FormHelper.newField(obj, obj.formRef, fieldName)
        .input(settings)
        .element;

}

/** @param { EndpointGroupType } self */
function addPaginateOption(self, formGroup, endpointCounter, component){

    formGroup.insertAdjacentHTML('afterbegin', `<label>Use pagination</label>`);
    const paginateRadioButtonContainer = document.createElement('div');
    paginateRadioButtonContainer.classList = 'use-pagination-check-group';
    const yesRadio = document.createElement('input');
    const noRadio = document.createElement('input');

    yesRadio.type = 'radio', noRadio.type = 'radio', noRadio.checked = true;
    yesRadio.name = `endpointField${endpointCounter}`, noRadio.name = `endpointField${endpointCounter}`;

    paginateRadioButtonContainer.appendChild(yesRadio);
    paginateRadioButtonContainer.insertAdjacentText('beforeend','Yes');
    paginateRadioButtonContainer.appendChild(noRadio);
    paginateRadioButtonContainer.insertAdjacentText('beforeend','No');
    formGroup.appendChild(paginateRadioButtonContainer);

    yesRadio.onclick = () => addPaginateEndpoint(self, endpointCounter);

    noRadio.onclick = () => delPaginateEndpoint(self, endpointCounter);

}

/** @param { EndpointGroupType } self */
export function addPaginateEndpoint(self, endpointCounter){
    addPaginateFields(self, endpointCounter);
    document.querySelector(`.endpointFieldGrp${endpointCounter}`).style.display = '';
}

/** @param { EndpointGroupType } self */
export function delPaginateEndpoint(self, endpointCounter){

    const component = self.component;
    for(const fieldName of self.paginateFields)
        FormHelper.delField(component, component.formRef, fieldName);
    document.querySelector(`.endpointFieldGrp${endpointCounter}`).remove();
    
}

/** @param { EndpointGroupType } self */
function addPaginateFields(self, endpointCounter){

    self.paginateFields = [];

    const paginateSetting = document.createElement('div');
    paginateSetting.className = `endpoint-paginate-setting endpointFieldGrp${endpointCounter}`

    const formGroup4 = document.createElement('div'), formGroup5 = document.createElement('div'), formGroup6 = document.createElement('div');
    formGroup4.className = 'form-group use-pagination-field',
        formGroup5.className = 'form-group use-pagination-field',
        formGroup6.className = 'form-group use-pagination-field';

    // Creates the field for entering the name of the pagination offset filend name
    let fieldName = `paginationStartField${endpointCounter}`;
    const offsetFieldName = newStilComponentField(self, { placeholder: 'e.g. offset', fieldName, paginateField: true });
    formGroup4.insertAdjacentHTML('afterbegin', `<label>Offset field name</label>${offsetFieldName}`);

    // Creates the field for entering the name of the pagination limit filend name
    fieldName = `paginationLimitField${endpointCounter}`;
    const limitFieldName = newStilComponentField(self, { placeholder: 'e.g. limit', fieldName, paginateField: true });
    formGroup5.insertAdjacentHTML('afterbegin', `<label>Limit field name</label>${limitFieldName}`);

    // Creates the field for entering the name of the pagination record per page
    fieldName = `paginationRecPerPage${endpointCounter}`;
    const recordPerPage = newStilComponentField(self, { placeholder: 'e.g. 1000', fieldName, paginateField: true });
    formGroup6.insertAdjacentHTML('afterbegin', `<label>Records per page</label>${recordPerPage}`);

    paginateSetting.appendChild(formGroup4);
    paginateSetting.appendChild(formGroup5);
    paginateSetting.appendChild(formGroup6);

    document.querySelector(`.endpointSettings${endpointCounter}`).appendChild(paginateSetting);

}


export function showHidePaginateEndpoint(endpointCounter, show = false){

    const paginateFieldsContainer = document.querySelector(`.endpointFieldGrp${endpointCounter}`);
    const paginateFields = document.querySelectorAll(`.endpointField${endpointCounter}`);

    if(show){
        paginateFieldsContainer.style.display = '';
        paginateFields.forEach(field => {
            field.setAttribute('required', true);
            field.style.display = '';
        });
    }else{
        paginateFieldsContainer.style.display = 'none';
        paginateFields.forEach(field => {
            field.removeAttribute('required');
            field.style.display = 'none';
        });
    }
}

