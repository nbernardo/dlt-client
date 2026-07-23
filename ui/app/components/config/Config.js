import { $still } from "../../../@still/component/manager/registror.js";
import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { HTTPHeaders } from "../../../@still/helper/http.js";
import { AppTemplate } from "../../../config/app-template.js";
import { Header } from "../parts/Header.js";
import { Workspace } from "../workspace/Workspace.js";

export class Config extends ViewComponent {

	isPublic = false;

	landingZonePath = null;

	landingZoneFiles = [];

	userToken = null;

	/** @Proxy @type { Header } */ headerProxy;

	/** @type { Workspace } */ $parent;

	async stAfterInit() {
		AppTemplate.hideLoading();
		await this.getUserToken();
		//setTimout to avoid the component/page load to 
		// block due to the Chart library
		const { totalPipelines } = this.headerProxy.workspaceService;
		const { schedulePipelinesStore } = this.headerProxy.workspaceService;	
		setTimeout(() => this.renderCharts(totalPipelines, schedulePipelinesStore.value.length));
		await this.getLandingZonePath();
	}

	renderCharts(totalPipelines, shcedulePipelines) {
		try {			
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
				options: { scales: { y: { beginAtZero: true } } }
			});
		} catch (error) {}

		// const recordsIngestion = document.getElementById('recordsIngestion');
		// new Chart(recordsIngestion, {
		// 	type: 'bar',
		// 	data: {
		// 		labels: ['Sept-01', 'Sept-2', 'Sept-3', 'Sept-4', 'Sept-5', 'Sept-6', 'Sept-7'],
		// 		datasets: [{ label: 'Day', data: [100020, 2300, 100, 400, 12009, 0, 80090], borderWidth: 1 }]
		// 	},
		// 	options: { scales: { y: { beginAtZero: true } } }
		// });

	}

	async getLandingZonePath(){
		let result = await $still.HTTPClient.get('/workspace/landing-zone');
		result = await result.json();
		if(!result.error){
			this.landingZonePath = result.result.path[0];
			this.landingZoneFiles = result.result.files;
		}
	}

	async checkPath(){
		let result = await $still.HTTPClient.post(
			'/workspace/landing-zone/check', 
			JSON.stringify({ path: this.landingZonePath.value }),
			HTTPHeaders.JSON
		)
		/workspace/user/token
		result = await result.json();
		if(result.error) return AppTemplate.toast.error(result.result);
		this.landingZoneFiles = result.result;
	}

	async getUserToken(){
		let result = await $still.HTTPClient.get('/workspace/user/token')
		result = await result.json();
		
		if(result.error) return AppTemplate.toast.error(result.result);
		this.userToken = result.result.token;
	}

	copyTokenToClipboard = async () => await navigator.clipboard.writeText(this.userToken.value);
}