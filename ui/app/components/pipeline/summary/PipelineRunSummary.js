import { sleepForSec } from "../../../../@still/component/manager/timer.js";
import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { State } from "../../../../@still/component/type/ComponentType.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { PipelineService } from "../../../services/PipelineService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { Workspace } from "../../workspace/Workspace.js";

export class PipelineRunSummary extends ViewComponent {

	isPublic = true;

	/** @Prop @type { HTMLElement } */
	container;

	/** @type { State<Array> } */
	runList;

	/** @type { Workspace } */
	$parent;

	/**
	 * @Inject
	 * @Path services/
	 * @type { PipelineService } */
	pplService;

	/** @Prop */ loading = false;

	async stBeforeInit(){
		await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
	}

	async stAfterInit(){
		this.container = document.querySelector(`.${this.cmpInternalId}`);
		this.pplService.pipelineRun.onChange(val => {

			const failedRuns = Object.keys(val.fails);
			failedRuns.forEach(execId => {
				document.querySelector(`#rerun-status-${execId}`).innerHTML = 'Manual run failed';
				document.querySelector(`#rerun-icon-${execId}`).style.display = '';
				document.querySelector(`#rerun-loading-icon-${execId}`).style.display = 'none';
				try { delete this.pplService.pipelineRun.value.fails[execId]; } catch (error) { }
			});
		});
	}

	async immediateRun(pipelineName, execId){
		document.querySelector(`#rerun-status-${execId}`).innerHTML = 'Running';
		document.querySelector(`#rerun-icon-${execId}`).style.display = 'none';
		document.querySelector(`#rerun-loading-icon-${execId}`).style.display = '';
		await PipelineService.immediatePipelineRun(pipelineName, execId);
	}

	async getLogs(execId){

		const collapsableLogs = document.querySelector(`.collapsable-logs-${execId}`);
		if(collapsableLogs){
			if(collapsableLogs.style.display == 'none')
				return collapsableLogs.style.display = '';
			return collapsableLogs.style.display = 'none';
		}

		const logs = await WorkspaceService.getLogs({ execution_id: execId });
		let content = '<table>'+this.logsToHTML(logs.all_logs)+'</table>';
		content = `<tr class="collapsable-logs-${execId}">
					<td colspan="20" class="pipeline-run-log-container">
						<div class="pipeline-run-log-content">${content}</div>
					</td>
				</tr>`;

		document.querySelector(`#view-error-trace-${execId}`).parentElement.insertAdjacentHTML('afterend', content);
	}

	logsToHTML(logs = []){
		return logs.map(item => {
			item = PipelineRunSummary.parseLogRow(item);
			return `
				<tr each="item" class="log-row log-type-${item.log_level}">
					<td>
						<div class="txt-bold">${item.timestamp}</div><div class="txt-mute">#${item.id}</div>
					</td>
					<td>
						<div class="txt-bold has-pipeline-complete has-pipeline-complete-${item.is_complete}"></div>
					</td>
					<td><span class="log-tag log-type-${item.log_level}">${item.log_level}</span></td>
					<td><div class="txt-mute">${item.execution_id}</div></td>
					<td><div class="txt-msg">${item.message}</div></td>
					<td>
						<div class="log-pill-wrap"><span class="log-pill">${item.extra_data}</span></div>
					</td>
				</tr>
			`
		}).join('');
	}

	static parseLogRow(itm){
		return {
			timestamp: itm[0].replace(/(\.\d{3})\d+/, '$1'), id: itm[1], log_level: itm[2], module: itm[3], execution_id: itm[4],
			line_number: itm[5], message: itm[6], namespace: itm[7], extra_data: itm[8], is_complete: itm[9]
		}
	}

}