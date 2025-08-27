import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";

export class PopupWindow extends ViewComponent {

	isPublic = true;

	/** @Prop */
	uniqueId = UUIDUtil.newId();

	/** @Prop */
	popup;
	/** @Prop */
	isDragging = false;
	/** @Prop */
	isResizing = false;
	/** @Prop */
	dragStart = { x: 0, y: 0 };
	/** @Prop */
	resizeStart = { x: 0, y: 0, w: 0, h: 0 };
	/** @Prop */
	isMinimized = false;
	/** @Prop */
	isMaximized = false;
	/** @Prop */
	prevState = { w: 400, h: 300, x: 50, y: 50 };
	/** @Prop */
	showWindowPopup = false;

	stAfterInit(){
		this.popup = document.getElementById(this.uniqueId);
		this.setOnMouseMoveContainer();
		this.setOnPopupResize();
	}

	openPopup() {
		this.popup.classList.remove('hidden');
	}

	closePopup() {
		this.popup.classList.add('hidden');
		this.popup.classList.remove('minimized', 'maximized');
		this.isMinimized = false;
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
			this.isDragging = true;
			this.dragStart = { x: e.clientX - this.popup.offsetLeft, y: e.clientY - this.popup.offsetTop };
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
		container.onmousemove = e => {
			if (this.isDragging) {
				this.popup.style.left = (e.clientX - this.dragStart.x) + 'px';
				this.popup.style.top = (e.clientY - this.dragStart.y) + 'px';
			}

			if (this.isResizing) {

				const dx = e.clientX - this.resizeStart.x, dy = e.clientY - this.resizeStart.y;
				let newWidth = this.resizeStart.w, newHeight = this.resizeStart.h;
				let newLeft = this.resizeStart.left, newTop = this.resizeStart.top;

				if (this.isResizing.includes('e')) newWidth = Math.max(200, this.resizeStart.w + dx);
				if (this.isResizing.includes('w'))
					[newWidth, newLeft] = [Math.max(200, this.resizeStart.w - dx), this.resizeStart.left + dx];

				if (this.isResizing.includes('s')) newHeight = Math.max(100, this.resizeStart.h + dy);
				if (this.isResizing.includes('n'))
					[newHeight, newTop] = [Math.max(100, this.resizeStart.h - dy), this.resizeStart.top + dy];

				[this.popup.style.width, this.popup.style.height] = [newWidth + 'px', newHeight + 'px'];
				[this.popup.style.left, this.popup.style.top] = [newLeft + 'px', newTop + 'px'];
			}
		};

		container.onmouseup = () => {
			this.isDragging = false;
			this.isResizing = false;
		};
	}


}