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
 * @param { CatalogForm } self 
 */
export function handleAddEndpointField(endpointCounter, self) {

    const fieldSet = document.createElement('fieldset');
    const legend = document.createElement('legend');
    legend.innerText = 'Endpoint ' + endpointCounter;
    fieldSet.appendChild(legend);

    const dataSetting = document.createElement('div'), paginateSetting = document.createElement('div');
    dataSetting.className = 'endpoint-data-setting', paginateSetting.className = 'endpoint-paginate-setting';

    const formGroup1 = document.createElement('div'), formGroup2 = document.createElement('div');
    formGroup1.className = 'form-group use-pagination-field', formGroup2.className = 'form-group use-pagination-field';

    let fieldName = `endpointPath${endpointCounter}`;
    const endpointField = FormHelper.newField(self, self.formRef, fieldName)
        .input({ required: true, placeholder: 'e.g. /transaction/paginate', className: 'endpoint-input' })
        .element;
    formGroup1.insertAdjacentHTML('afterbegin', `<label>Path</label>${endpointField}`);

    fieldName = `endpointPK${endpointCounter}`;
    const primaryKeyField = FormHelper.newField(self, self.formRef, fieldName)
        .input({ required: true, placeholder: 'e.g. transactionId' })
        .element;
    formGroup2.insertAdjacentHTML('afterbegin', `<label>Primary key</label>${primaryKeyField}`);

    const formGroup3 = document.createElement('div'), formGroup4 = document.createElement('div'), formGroup5 = document.createElement('div');
    formGroup3.className = 'form-group use-pagination-field',
        formGroup4.className = 'form-group use-pagination-field',
        formGroup5.className = 'form-group use-pagination-field';

    fieldName = `paginateOffsetName${endpointCounter}`;
    const offsetFieldName = FormHelper.newField(self, self.formRef, fieldName)
        .input({ required: true, placeholder: 'e.g. offset' })
        .element;
    formGroup3.insertAdjacentHTML('afterbegin', `<label>Offset field name</label>${offsetFieldName}`);

    fieldName = `paginateLimitName${endpointCounter}`;
    const limitFieldName = FormHelper.newField(self, self.formRef, fieldName)
        .input({ required: true, placeholder: 'e.g. limit' })
        .element;
    formGroup4.insertAdjacentHTML('afterbegin', `<label>Limit field name</label>${limitFieldName}`);

    fieldName = `paginateRecPerPage${endpointCounter}`;
    const recordPerPage = FormHelper.newField(self, self.formRef, fieldName)
        .input({ required: true, placeholder: 'e.g. 1000' })
        .element;
    formGroup5.insertAdjacentHTML('afterbegin', `<label>Records per page</label>${recordPerPage}`);

    dataSetting.appendChild(formGroup1);
    dataSetting.appendChild(formGroup2);
    paginateSetting.appendChild(formGroup3);
    paginateSetting.appendChild(formGroup4);
    paginateSetting.appendChild(formGroup5);

    fieldSet.appendChild(dataSetting);
    fieldSet.appendChild(paginateSetting);

    document.querySelector(`.catalog-form-secret-api .endpoint-group-config`).appendChild(fieldSet);

}