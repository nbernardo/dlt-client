import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Assets } from "../../../@still/util/componentUtil.js";
import { AppTemplate } from "../../../config/app-template.js";

export class Config extends ViewComponent {

	isPublic = false;

	stOnRender(){
		AppTemplate.showLoading();
	}

	stAfterInit() {
		AppTemplate.hideLoading();
		//setTimout to avoid the component/page load to 
		// block due to the Chart library
		setTimeout(() => this.renderCharts());
	}

	renderCharts() {

		const pipelineChart = document.getElementById('pipelineChart');
		new Chart(pipelineChart, {
			type: 'pie',
			data: {
				labels: ['Scheduled', 'Created'],
				datasets: [{
					label: 'Stattus',
					data: [12, 19],
					borderWidth: 1
				}]
			},
			options: {
				scales: {
					y: { beginAtZero: true }
				}
			}
		});

		const recordsIngestion = document.getElementById('recordsIngestion');
		new Chart(recordsIngestion, {
			type: 'bar',
			data: {
				labels: ['Sept-01', 'Sept-2', 'Sept-3', 'Sept-4', 'Sept-5', 'Sept-6', 'Sept-7'],
				datasets: [{
					label: 'Day',
					data: [100020, 2300, 100, 400, 12009, 0, 80090],
					borderWidth: 1
				}]
			},
			options: {
				scales: {
					y: {
						beginAtZero: true
					}
				}
			}
		});

	}
}