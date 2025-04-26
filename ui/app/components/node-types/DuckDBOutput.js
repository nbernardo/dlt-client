import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { WorkSpaceController } from "../../controller/WorkSpaceController.js";

export class DuckDBOutput extends ViewComponent {

	isPublic = true;

	/** @Prop */
	nodeId;

	database;
	tableName;
	label = 'Duckdb Output';

	/** @Prop */
	inConnectors = 1;
	/** @Prop */
	outConnectors = 0;

	stOnRender(nodeId) {
		this.nodeId = nodeId;
	}

	stAfterInit() {

		this.database.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['database'] = newValue;
		});

		this.tableName.onChange((newValue) => {
			const data = WorkSpaceController.getNode(this.nodeId).data;
			data['tableName'] = newValue;
		});
	}

}