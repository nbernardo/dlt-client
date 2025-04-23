import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";

export class ObjectType extends ViewComponent {

	isPublic = true;

	/** @Prop */
	typeName;

	/** @Prop */
	icon;

	/** @Prop */
	label;

	/** @Prop */
	imgIcon;

	/** @Prop */
	source = true;

	/** @Prop */
	dest = true;


	template = `
		<div 
			class="drag-drawflow" 
			draggable="true" 
			ondragstart="controller('WorkSpaceController').drag(event)" 
			data-node="@typeName"
			data-lbl="@label"
			data-icon="@icon"
			data-img="@imgIcon"
			data-src=@source
			data-dst=@dest
			>
			<i (renderIf)="self.icon" class="@icon"></i>
			<span>
				<span (renderIf)="self.imgIcon">
					<img src="@imgIcon" style="width: 20px;" />
				</span> @label
			</span>
		</div>
	`;



}