import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { ListState } from "../../../@still/component/type/ComponentType.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";

export class FileList extends ViewComponent {

	isPublic = true;

	/** @type { ListState<Array<{}>> } */
	filesList = [];

	/** @Prop */
	fileMenu;

	/** @Prop */
	fileExt = 'png';

	/** @Prop */
	selectedFile;

	// The bellow uniqId is bound to the template as well
	// so to allow DOM (getElementById) to work properly
	/** @Prop */
	uniqId = '_'+UUIDUtil.newId();

	togglePopup(element, filename) {
		const rect = element.getBoundingClientRect();
		this.selectedFile = filename;

		if (this.activeFileDropdown === element) {
			this.fileMenu.classList.remove('is-active');
			this.activeFileDropdown = null;
		} else {
			this.fileMenu.classList.remove('is-active');
			this.fileMenu.style.left = `${rect.left - 8}px`;
			this.fileMenu.style.top = `${rect.top}px`;
			this.fileMenu.classList.add('is-active');
			this.activeFileDropdown = element;
		}
	}

	setUpFileMenuEvt(){
		this.fileMenu = document.getElementById(this.uniqId);

		const obj = this; //Becuase inside callbakc this is not available
        document.addEventListener('click', function(event) {
			
            const [isClickInsideMenu, isClickTrigger] = [obj.fileMenu.contains(event.target), event.target.closest('img')];
            if (!isClickInsideMenu && !isClickTrigger) {
                obj.fileMenu.classList.remove('is-active');
                obj.activeFileDropdown = null;
            }
        });
	}

	/** @template */
	openInEditor(){}
}