import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State } from "../../../@still/component/type/ComponentType.js";
import { FormHelper } from "../../../@still/helper/form.js";

export class CatalogForm extends ViewComponent {

	isPublic = true;

	/** @Prop */ modal;
	/** @Prop */ openModal;
	/** @Prop */ closeModal;
	/** @Prop */ showAddSecrete = false;

	/** @Prop */ dataBaseSettingType = null;
	/** @Prop */ formRef = null;
	/** @Prop */ formRef = null;
	/** @Prop */ dynamicFieldCount = 0;

	stAfterInit(){

		this.modal = document.getElementById('modal');
		//this.openModal = document.getElementById('openModal');
		this.closeModal = document.getElementById('closeModal');
		this.handleModalCall();

	}

	changeType(value){
		this.showAddSecrete = true;
		this.dataBaseSettingType = value;
	}

	showDialog(){		
		if(this.modal.style.display !== 'flex')
			this.modal.style.display = 'flex';
		else
			this.modal.style.display = 'none';
	}

	handleModalCall(){
		const self = this;
		//this.openModal.addEventListener('click', () => self.modal.style.display = 'flex');
		this.closeModal.addEventListener('click', () => self.modal.style.display = 'none');

		window.addEventListener('click', (e) => {
			if (e.target === modal) self.modal.style.display = 'none';
		});
	}

	addSecreteGroup(){
		const type = this.dataBaseSettingType == 1 ? 'db' : 'secret';
		const targetForm = type == 'db' ? 'catalog-form-db-fields' : 'catalog-form-secret-group';

		this.dynamicFieldCount++;
		let fieldName = `key${this.dynamicFieldCount}-${type}`;
		const secretKeyField = FormHelper
			.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'e.g. DB_PASSWORD' })
			.element;

		fieldName = `val${this.dynamicFieldCount}-${type}`;
		const secretValField = FormHelper
			.newField(this, this.formRef, fieldName)
			.input({ required: true, placeholder: 'Enter the secret value', type: 'password' })
			.element;

		this.addSecreteField(secretKeyField, targetForm);
		this.addSecreteField(secretValField, targetForm, 'val', fieldName);

	}

	addSecreteField = (field, targetForm, type, fieldName) => {
		const div = document.createElement('div');
		div.className = 'form-group';
		let fieldContent = field;

		if(type == 'val')
			fieldContent = this.parseEvents(`
				<span class="${fieldName} hidden-secret-value">
					${field} <img src="app/assets/imgs/view-svgrepo-com.svg" onclick="inner.viewSecretValue('${fieldName}')">
				<span>`);

		div.innerHTML = fieldContent;
		document.querySelector(`.${targetForm}`).appendChild(div);
	}

	viewSecretValue(fieldContainer){
		//view-hide-svgrepo-com
		const type = this.dataBaseSettingType == 1 ? 'catalog-form-db-fields' : 'catalog-form-secret-group'
		const clsName = fieldContainer == 'initial' ? '.initial-secret-field' : `.${type} .${fieldContainer}`;
		const currentVal = document.querySelector(`${clsName} input`).type;
		if(currentVal == 'text') {
			document.querySelector(`${clsName} input`).type = 'password';
			document.querySelector(`${clsName} img`).src = 'app/assets/imgs/view-svgrepo-com.svg';
		}else{
			document.querySelector(`${clsName} input`).type = 'text';
			document.querySelector(`${clsName} img`).src = 'app/assets/imgs/view-hide-svgrepo-com.svg';
		}
	}

}