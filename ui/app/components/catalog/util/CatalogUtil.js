import { FormHelper } from "../../../../@still/helper/form.js";
import { CatalogForm } from "../CatalogForm.js";

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
 */
export function handleAddEndpointField(endpointCounter, component) {

    const self = { fieldList: [], component };
    const fieldSet = document.createElement('fieldset');
    const legend = document.createElement('legend');
    legend.innerText = 'Endpoint ' + endpointCounter;
    fieldSet.appendChild(legend);

    const dataSetting = document.createElement('div'), paginateSetting = document.createElement('div');
    dataSetting.className = 'endpoint-data-setting', paginateSetting.className = 'endpoint-paginate-setting';

    const formGroup1 = document.createElement('div'), formGroup2 = document.createElement('div');
    formGroup1.className = 'form-group use-pagination-field', formGroup2.className = 'form-group use-pagination-field';

    formGroup1.insertAdjacentHTML('afterbegin', `<label>Path</label>`);
    
    const delEntpointBtn = document.createElement('div');
    delEntpointBtn.className = 'del-endpoint-setting-icon';
    delEntpointBtn.innerText = 'Remove';
    fieldSet.appendChild(delEntpointBtn);

    // Creates the field for entering the endpoint
    let fieldName = `apiEndpointPath1${endpointCounter}`;
    const endpointField = newStilComponentField(self, 
        { fieldName, required: true, placeholder: 'e.g. /transaction/paginate', className: 'endpoint-input' }
    );
    formGroup1.insertAdjacentHTML('beforeend', endpointField);

    // Creates the field for entering the endpoint data primary key
    fieldName = `apiEndpointPathPK1${endpointCounter}`;
    const primaryKeyField = newStilComponentField(self, { required: true, placeholder: 'e.g. transactionId', fieldName });
    formGroup2.insertAdjacentHTML('afterbegin', `<label>Primary key</label>${primaryKeyField}`);

    const formGroup3 = document.createElement('div'), formGroup4 = document.createElement('div'), formGroup5 = document.createElement('div');
    formGroup3.className = 'form-group use-pagination-field',
        formGroup4.className = 'form-group use-pagination-field',
        formGroup5.className = 'form-group use-pagination-field';

    // Creates the field for entering the name of the pagination offset filend name
    fieldName = `paginateOffsetName${endpointCounter}`;
    const offsetFieldName = newStilComponentField(self, { placeholder: 'e.g. offset', fieldName });
    formGroup3.insertAdjacentHTML('afterbegin', `<label>Offset field name</label>${offsetFieldName}`);

    // Creates the field for entering the name of the pagination limit filend name
    fieldName = `paginateLimitName${endpointCounter}`;
    const limitFieldName = newStilComponentField(self, { placeholder: 'e.g. limit', fieldName });
    formGroup4.insertAdjacentHTML('afterbegin', `<label>Limit field name</label>${limitFieldName}`);

    // Creates the field for entering the name of the pagination record per page
    fieldName = `paginateRecPerPage${endpointCounter}`;
    const recordPerPage = newStilComponentField(self, { placeholder: 'e.g. 1000', fieldName });
    formGroup5.insertAdjacentHTML('afterbegin', `<label>Records per page</label>${recordPerPage}`);

    dataSetting.appendChild(formGroup1);
    dataSetting.appendChild(formGroup2);
    paginateSetting.appendChild(formGroup3);
    paginateSetting.appendChild(formGroup4);
    paginateSetting.appendChild(formGroup5);

    fieldSet.appendChild(dataSetting);
    fieldSet.appendChild(paginateSetting);

    delEntpointBtn.onclick = function(){
        for(const fieldName of self.fieldList)
            FormHelper.delField(component, component.formRef, fieldName);
        fieldSet.remove();
    }
    document.querySelector(`.catalog-form-secret-api .endpoint-group-config`).appendChild(fieldSet);

}


function newStilComponentField(self, { fieldName, placeholder = '', required, className }){

    const obj = self.component;
    const settings = { required: false, placeholder };
    
    if(className) settings.className = className;
    if(required) settings.required = required;
    
    self.fieldList.push(fieldName);

    return FormHelper.newField(obj, obj.formRef, fieldName)
        .input(settings)
        .element;

}