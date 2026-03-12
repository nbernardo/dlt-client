import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";

export class ModalWindowComponent extends ViewComponent {

    	setOnMouseMoveContainer() {

		const container = document.getElementById('container-'+this.uniqueId);
		const self = this;

		container.onmousemove = e => {
			if (self.util.isDragging) {				
				self.popup.style.left = (e.clientX - self.dragStart.x) + 'px';
				self.popup.style.top = (e.clientY - self.dragStart.y) + 'px';
			}

			if (self.isResizing) {

				const dx = e.clientX - self.resizeStart.x, dy = e.clientY - self.resizeStart.y;
				let newWidth = self.resizeStart.w, newHeight = self.resizeStart.h;
				let newLeft = self.resizeStart.left, newTop = self.resizeStart.top;

				if (self?.isResizing?.includes('e')) newWidth = Math.max(200, self.resizeStart.w + dx);
				if (self?.isResizing?.includes('w'))
					[newWidth, newLeft] = [Math.max(200, self.resizeStart.w - dx), self.resizeStart.left + dx];

				if (self?.isResizing?.includes('s')) newHeight = Math.max(100, self.resizeStart.h + dy);
				if (self?.isResizing?.includes('n'))
					[newHeight, newTop] = [Math.max(100, self.resizeStart.h - dy), self.resizeStart.top + dy];

				[self.popup.style.width, self.popup.style.height] = [newWidth + 'px', newHeight + 'px'];
				[self.popup.style.left, self.popup.style.top] = [newLeft + 'px', newTop + 'px'];
			}
		};

		container.onmouseup = () => {
			self.util.isDragging = false;
			self.isResizing = false;
		};
	}

	setOnPopupResize() {

		// Dragging
		this.popup.querySelector('.popup-mov-window-header-'+this.uniqueId).onmousedown = e => {
			if (this.isMaximized) return;
			this.util.isDragging = true;
			this.dragStart = { x: e.clientX - this.popup.offsetLeft, y: e.clientY - this.popup.offsetTop };
		};

		// Dragging
		this.popup.querySelector('.popup-mov-window-header-'+this.uniqueId).onmouseup = e => {
			this.util.isDragging = false;			
		};

		// Resizing
		this.popup.querySelectorAll('.resize-handle').forEach(handle => {
			handle.onmousedown = e => {
				if (this.isMaximized || this.isMinimized) return;
				e.stopPropagation();
				this.isResizing = handle.className.split(' ')[1];
				this.resizeStart = {
					x: e.clientX,
					y: e.clientY,
					w: this.popup.offsetWidth,
					h: this.popup.offsetHeight,
					left: this.popup.offsetLeft,
					top: this.popup.offsetTop
				};
			};
		});

	}

    closePopup(){
        this.popup.classList.add('hidden');
        this.popup.classList.remove('minimized', 'maximized');
        this.isMinimized = false;
        this.showWindowPopup = false;
    }

    showPopup = () => {
        this.showWindowPopup = true;
        this.popup.classList.remove('hidden');
    }

}