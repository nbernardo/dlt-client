import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";
import { PopupUtil } from "./PopupUtil.js";

export class PopupWindow extends ViewComponent {

	isPublic = true;

	/** @Prop */ uniqueId = UUIDUtil.newId();

	/** @Prop */ popup;

	/** @Prop */ isDragging = false;

	/** @Prop */ isResizing = false;

	/** @Prop */ dragStart = { x: 0, y: 0 };

	/** @Prop */ resizeStart = { x: 0, y: 0, w: 0, h: 0 };

	/** @Prop */ isMinimized = false;

	/** @Prop */ isMaximized = false;

	/** @Prop */ prevState = { w: 400, h: 300, x: 50, y: 50 };

	/** @Prop */ showWindowPopup = false;

	/** @Prop */ util = new PopupUtil();

	stAfterInit(){
		this.popup = document.getElementById(this.uniqueId);
		this.setOnMouseMoveContainer();
		this.setOnPopupResize();
		this.util = new PopupUtil();
	}

	openPopup() {
		this.popup.classList.remove('hidden');
		this.showWindowPopup = true;
	}

	closePopup() {
		this.popup.classList.add('hidden');
		this.popup.classList.remove('minimized', 'maximized');
		this.isMinimized = false;
		this.showWindowPopup = false;
	}

	toggleMinimize() {
		if (this.isMaximized) return;
		this.popup.classList.toggle('minimized');
		this.isMinimized = !this.isMinimized;
	}

	toggleMaximize() {
		if (this.isMaximized) {
			this.popup.classList.remove('maximized');
			this.popup.style.width = this.prevState.w + 'px';
			this.popup.style.height = this.prevState.h + 'px';
			this.popup.style.left = this.prevState.x + '%';
			this.popup.style.top = this.prevState.y + '%';
			this.popup.style.transform = 'translate(-50%, -50%)';
		} else {
			this.prevState = {
				w: this.popup.offsetWidth,
				h: this.popup.offsetHeight,
				x: this.popup.offsetLeft,
				y: this.popup.offsetTop
			};
			this.popup.classList.add('maximized');
			this.popup.classList.remove('minimized');
			this.isMinimized = false;
		}
		this.isMaximized = !this.isMaximized;
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
}