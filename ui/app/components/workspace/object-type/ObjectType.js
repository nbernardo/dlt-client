import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";

export class ObjectType extends ViewComponent {

	isPublic = true;

	/** @Prop */ typeName;

	/** @Prop */ icon;

	/** @Prop */ label;

	/** @Prop */ imgIcon;

	/** @Prop */ source = true;

	/** @Prop */ dest = true;

	/** @Prop */ name = '';

	/** @Prop */ disable = 'no';

	/** @Prop */ tmplt;

	/** @Prop */ isNodeGroup;

	/** @Prop */ groupType;

	/** @Prop */ dropDownIcon = '';

	template = `
		<div 
			class="drag-drawflow @groupType-item" 
			draggable="true" 
			ondragstart="controller('WorkSpaceController').drag(event, '@disable', '@isNodeGroup')" 
			onclick="controller('WorkSpaceController').showItemsGroup(this)"
			data-node="@typeName"
			data-lbl="@label"
			data-icon="@icon"
			data-img="@imgIcon"
			data-src=@source
			data-dst=@dest
			data-template="@tmplt"
			data-name="@tmplt"
			data-group-name="@name"
			>
			<i (renderIf)="self.icon" class="@icon @groupType-icon"></i>
			<span class="node-icon-and-name-wrapper">
				<span (renderIf)="self.imgIcon">
					<img src="@imgIcon" style="width: 20px;" class="@name @groupType-icon"/>
				</span>
				<label>&nbsp;@label</label>
				<label class="drop-down-icon">@dropDownIcon</label>
			</span>
		</div>
	`;

}