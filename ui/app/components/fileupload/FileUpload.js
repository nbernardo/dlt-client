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

	/**
	 * @Inject  @Path services/
	 * @type { WorkspaceService } */
	wSpaceService;

	/** @Delayed 1s */
	constructor(){
		super();
	}

	stAfterInit(){
	
        const uploadArea = document.querySelector('.upload-area');
        const fileInput = document.getElementById('fileInput');
        this.fileInfo = document.getElementById('fileInfo');
        this.filesList = document.getElementById('filesList');
        this.fileCount = document.getElementById('fileCount');

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
            if (e.target.files.length > 0) {
                addFiles(Array.from(e.target.files));
            }
        });

        function preventDefaults(e) {
            e.preventDefault();
            e.stopPropagation();
        }

        function highlight() {
            uploadArea.classList.add('dragover');
        }

        function unhighlight() {
            uploadArea.classList.remove('dragover');
        }

        function handleDrop(e) {
            const dt = e.dataTransfer;
            const files = Array.from(dt.files);
            addFiles(files);
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
		if (this.selectedFiles.length === 0) {
			this.fileInfo.style.display = 'none';
			return;
		}

		function formatFileSize(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
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
		this.isUploading = true;
		if (this.selectedFiles.length > 0) {

			const formData = new FormData();
			for (let file of this.selectedFiles) {
				formData.append('files', file);
			}

			formData.append('user', UserUtil.email);

			try {
				const response = await $still.HTTPClient.post('/upload', formData);

				if (response.ok)  {
					AppTemplate.toast.success('File(s) uploaded successfully');
					this.clearAllFiles();
					// Auto-click the to force fetching the updated list of file
					document.getElementById('dataFilesLeftMenu').click();
					document.getElementById('collapse').checked = true;
				}
				else AppTemplate.toast.error('Error while uploading the file(s)');
				
			} catch (error) {
				AppTemplate.toast.error(`File(s) upload failed: ${error.message}`);
			}finally{
				this.isUploading = false;
			}
		}
	}

	async listFiles(){
		return this.wSpaceService.listFiles();
	}

}
