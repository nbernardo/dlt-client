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


	template = `
		<div 
			class="drag-drawflow" 
			draggable="true" 
			ondragstart="drawdrag(event)" 
			data-node="@typeName">
			<i (renderIf)="self.icon" class="@icon"></i>
			<span><span (renderIf)="self.imgIcon">@imgIcon</span> @label</span>
		</div>
	`;



}