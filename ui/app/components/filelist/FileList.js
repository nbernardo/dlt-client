import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { ListState } from "../../../@still/component/type/ComponentType.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";
import { Workspace } from "../workspace/Workspace.js";

export class FileList extends ViewComponent {

	isPublic = true;

	/** @type { ListState<Array<{}>> } */
	filesList = [];

	/** @Prop */ fileMenu;
	/** @Prop */ fileExt = 'png';
	/** @Prop */ selectedFile;
	/** @Prop */ fileId;

	noFilesMessage = 'No file found';

	/** @Prop */ isOpenInEditor = true;
	/** @Prop */ isDataFile = true;

	/** @type { Workspace } */ $parent;

	// The bellow uniqId is bound to the template as well
	// so to allow DOM (getElementById) to work properly
	/** @Prop */
	uniqId = '_'+UUIDUtil.newId();

	togglePopup(element, filename, fileId = null) {
		const rect = element.getBoundingClientRect();
		this.selectedFile = filename;
		this.fileId = fileId;

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

		const obj = this; //Becuase inside callback this is not available
        document.addEventListener('click', function(event) {
			
            const [isClickInsideMenu, isClickTrigger] = [obj.fileMenu.contains(event.target), event.target.closest('img')];
            if (!isClickInsideMenu && !isClickTrigger) {
                obj.fileMenu.classList.remove('is-active');
                obj.activeFileDropdown = null;
            }
        });
	}

	closeDropdown = () => this.fileMenu.classList.remove('is-active');

	/** @template */
	openInEditor(){}

	downloadfile(){
		const type = this.isOpenInEditor ? 'pipeline' : 'data';
		this.$parent.service.downloadfile(this.selectedFile, type);
		this.closeDropdown();
	}

	async deletefile(){
		/** @type { WorkSpaceController } */
		const parentController = WorkSpaceController.get();
		parentController.showDialog(`Are you sure you want to remove <br><b>${this.selectedFile}</b>`, 
			{
				title: 'Deleting File', type: 'confirm', 
				onConfirm: async () => {
					const result = await this.$parent.service.deletefile(this.selectedFile);
					if(!result.error){
						AppTemplate.toast.success(result.result, 5000);
						document.querySelector(`.current-file-${this.fileId}`).remove();
					}
				}
		});
		this.closeDropdown();
	}
	/** @param { HTMLLIElement } elm  */
	async showOrHideVersion(elm, fileId){
		const versionsContainer = document.querySelector(`.versions-of-file-${fileId}`);
		if(versionsContainer.style.display === 'none'){
			elm.innerHTML = '-', versionsContainer.style.display = '';
		}else{
			elm.innerHTML = '+', versionsContainer.style.display = 'none';
		}
	}

	filterFileByName(type, fileName){		
		const fileItems = document.querySelectorAll(`.current-file-item-${type}`);
		fileItems.forEach(item => {
			const foundFileName = item.dataset.name;			
			if(foundFileName.search(fileName) >= 0){
				item.style.display = '';
			}else{
				item.style.display = 'none';
			}
		});

	}
	
}