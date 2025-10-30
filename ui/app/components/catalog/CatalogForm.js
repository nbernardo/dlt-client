import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";

export class CatalogForm extends ViewComponent {

	isPublic = true;

	/** @Prop */ modal;
	/** @Prop */ openModal;
	/** @Prop */ closeModal;

	stAfterInit(){

		this.modal = document.getElementById('modal');
		//this.openModal = document.getElementById('openModal');
		this.closeModal = document.getElementById('closeModal');
		this.handleModalCall();

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
}