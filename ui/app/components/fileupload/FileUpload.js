import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";

export class FileUpload extends ViewComponent {

	isPublic = true;

	/** @Prop */
	fileInfo;
	/** @Prop */
	filesList;
	/** @Prop */
	fileCount;
	/** @Prop */
	selectedFiles = [];

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
                if (!duplicate) {
                    obj.selectedFiles.push(file);
                }
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

	uploadFiles() {
		if (this.selectedFiles.length === 0) {
			alert('No files selected for upload');
			return;
		}
		
		// Here you would implement the actual upload logic
		alert(`Ready to upload ${this.selectedFiles.length} file(s):\n${this.selectedFiles.map(f => f.name).join('\n')}`);
		
		// After successful upload, you might want to clear the files
		// clearAllFiles();
	}
}

