import { $still } from "../../../@still/component/manager/registror.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { StillAppSetup } from "../../../config/app-setup.js";
import { AppTemplate } from "../../../config/app-template.js";
import { UserService } from "../../services/UserService.js";
import { WorkspaceService } from "../../services/WorkspaceService.js";
import { UserUtil } from "../auth/UserUtil.js";

export class FileUpload extends ViewComponent {

	isPublic = true;

	/** @Prop */ fileInfo;
	/** @Prop */ filesList;
	/** @Prop */ fileCount;
	/** @Prop */ selectedFiles = [];
	/** @Prop */ isUploading = false;
	/** @Prop */ uploadErrorIcon = `<i style="font-size:16px;color:white;" class="fas fa-exclamation-triangle"></i>`;
	/** @Prop @type { HTMLElement } */ uploadError = false;

	/**
	 * @Inject  @Path services/
	 * @type { WorkspaceService } */
	wSpaceService;

	/** @Delayed 1s */
	constructor(){
		super();
	}

	stAfterInit(){
	
        const uploadArea = document.querySelector('.upload-area'), fileInput = document.getElementById('fileInput');
        this.fileInfo = document.getElementById('fileInfo');
        this.filesList = document.getElementById('filesList');
        this.fileCount = document.getElementById('fileCount');
		this.uploadError = document.querySelector('.file-upload-error');

        const highlight = () => uploadArea.classList.add('dragover');
        const unhighlight = () => uploadArea.classList.remove('dragover');

        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, preventDefaults, false);
            document.body.addEventListener(eventName, preventDefaults, false);
        });

        ['dragenter', 'dragover'].forEach(eventName => {
            uploadArea.addEventListener(eventName, highlight, false);
        });

        ['dragleave', 'drop'].forEach(eventName => {
            uploadArea.addEventListener(eventName, unhighlight, false);
        });

        uploadArea.addEventListener('drop', handleDrop, false);

        fileInput.addEventListener('change', function(e) {
            if (e.target.files.length > 0)  addFiles(Array.from(e.target.files));
        });

        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }

        function handleDrop(e) {
            const dt = e.dataTransfer;
            addFiles(Array.from(dt.files));
        }

		const obj = this;
        function addFiles(files) {
            files.forEach(file => {
                const duplicate = obj.selectedFiles.find(f => f.name === file.name && f.size === file.size);
                if (!duplicate) obj.selectedFiles.push(file);
            });
            obj.updateFilesDisplay();
        }
	}

	removeFile(index) {
		this.selectedFiles.splice(index, 1);
		this.updateFilesDisplay();
	}

	updateFilesDisplay() {
		if (this.selectedFiles.length === 0) return this.fileInfo.style.display = 'none';
		
		const self = this 
		this.uploadError.style.display = 'none';
		
		function formatFileSize(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024, sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
			const actualFileSize = parseFloat((bytes / Math.pow(k, i)).toFixed(2));
			const fileSize = parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
			const sizeLimit = StillAppSetup.config.get('fileUploadSizeLimit');

			if(self.checkFileSizeLimit(actualFileSize, fileSize, sizeLimit) === false){
				self.uploadError.innerHTML = `${self.uploadErrorIcon} Each file size limit can't be more than ${sizeLimit}`;
				self.uploadError.style.display = '';
			}
			
            return fileSize;
        }

		this.fileCount.textContent = `${this.selectedFiles.length} file${this.selectedFiles.length > 1 ? 's' : ''} selected`;
		this.filesList.innerHTML = '';
		this.selectedFiles.forEach((file, index) => {
			const fileItem = document.createElement('div');
			fileItem.className = 'file-item';
			/** the onclick event points to inner because it's being embeded, otherwise would be self/parent */
			fileItem.innerHTML = this.parseEvents(`
				<div class="file-details">
					<div class="file-name">${file.name}</div>
					<div class="file-size">${formatFileSize(file.size)}</div>
				</div>
				<button type="button" class="remove-btn" onclick="inner.removeFile(${index})">Remove</button>
			`);
			this.filesList.appendChild(fileItem);
		});
		fileInfo.style.display = 'block';
	}

	clearAllFiles() {
		this.selectedFiles = [];
		this.updateFilesDisplay();
	}

	async uploadFiles() {
		this.isUploading = true 
		this.uploadError.innerHTML = '';
		this.uploadError.style.display = 'none';
		if (this.selectedFiles.length > 0) {

			const formData = new FormData();
			for (let file of this.selectedFiles) {
				formData.append('files', file);
			}

			formData.append('user', UserUtil.email);

			try {
				let response = await $still.HTTPClient.post('/upload', formData);
				if (response.ok)  {
					AppTemplate.toast.success('File(s) uploaded successfully');
					this.clearAllFiles();
					// Auto-click the to force fetching the updated list of file
					document.getElementById('dataFilesLeftMenu').click();
					document.getElementById('collapse').checked = true;
				}
				else {
					response = await response.json();
					if(response?.exceed_limit){
						const content = `${this.uploadErrorIcon} File upload limit of ${response?.limit_size} reached`
						this.uploadError.innerHTML = content, 
						this.uploadError.style.display = '';
					}
					AppTemplate.toast.error('Error while uploading the file(s)');
				}
				
			} catch (error) {
				AppTemplate.toast.error(`File(s) upload failed: ${error.message}`);
			}finally{
				this.isUploading = false;
			}
		}
	}
	/** This only implements file limit size in the Frontend, backend is implemented in the Proxy layer (e.g. Nginx) */
	checkFileSizeLimit(actualFileSize, fileSize /** contains the unity (e.g. MB, KB) */, sizeLimit){
		
		const fileSizeSymbol = String(fileSize).slice(-2)[0].toLowerCase(), sizeLimitSymbol = String(sizeLimit).slice(-1).toLowerCase();
		const actualSizeLimit = Number(sizeLimit.slice(0,-1));
		
		if(fileSizeSymbol === 's')
			if(sizeLimitSymbol !== fileSizeSymbol || (actualFileSize > actualSizeLimit)) return false;
		
		if(fileSizeSymbol === 'k')
			if(['m','g'].includes(sizeLimitSymbol) || (actualFileSize > actualSizeLimit)) return false;
		
		if(fileSizeSymbol === 'm')
			if(['g'].includes(sizeLimitSymbol) || (actualFileSize > actualSizeLimit)) return false;
			
        return true;
	}

	listFiles = async () => this.wSpaceService.listFiles();
}
