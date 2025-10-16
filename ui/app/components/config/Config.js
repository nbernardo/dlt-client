import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { AppTemplate } from "../../../config/app-template.js";
import { Header } from "../parts/Header.js";

export class Config extends ViewComponent {

	isPublic = false;

	/** @Proxy @type { Header } */ headerProxy;

	stAfterInit() {
		AppTemplate.hideLoading();
		//setTimout to avoid the component/page load to 
		// block due to the Chart library
		const { totalPipelines } = this.headerProxy.workspaceService;
		const { schedulePipelinesStore } = this.headerProxy.workspaceService;	
		setTimeout(() => this.renderCharts(totalPipelines, schedulePipelinesStore.value.length));
	}

	renderCharts(totalPipelines, shcedulePipelines) {

		const pipelineChart = document.getElementById('pipelineChart');
		new Chart(pipelineChart, {
			type: 'pie',
			data: {
				labels: ['Scheduled', 'Created'],
				datasets: [{
					label: 'Pipelines',
					data: [Number(shcedulePipelines), Number(totalPipelines)],
					borderWidth: 1
				}]
			},
			options: {
				scales: {
					y: { beginAtZero: true }
				}
			}
		});

		// const recordsIngestion = document.getElementById('recordsIngestion');
		// new Chart(recordsIngestion, {
		// 	type: 'bar',
		// 	data: {
		// 		labels: ['Sept-01', 'Sept-2', 'Sept-3', 'Sept-4', 'Sept-5', 'Sept-6', 'Sept-7'],
		// 		datasets: [{
		// 			label: 'Day',
		// 			data: [100020, 2300, 100, 400, 12009, 0, 80090],
		// 			borderWidth: 1
		// 		}]
		// 	},
		// 	options: {
		// 		scales: {
		// 			y: {
		// 				beginAtZero: true
		// 			}
		// 		}
		// 	}
		// });

	}
}