import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";

export class CleanerType extends ViewComponent {

	isPublic = true;
	template = `
	<div>
		<div class="title-box"><i class="fas fa-mouse"></i> Db Click</div>
		<div class="box dbclickbox" ondblclick="controller('WorkSpaceController').showpopup(event)">
			Db Click here
			<div class="modal" style="display:none">
				<div class="modal-content">
					<span class="close" onclick="controller('WorkSpaceController').closemodal(event)">&times;</span>
					Change your variable {name} !
					<input type="text" df-name>
				</div>
			</div>
		</div>
	</div>
	`;
}