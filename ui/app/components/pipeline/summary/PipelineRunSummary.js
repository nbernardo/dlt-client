import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";
import { State } from "../../../../@still/component/type/ComponentType.js";
import { Assets } from "../../../../@still/util/componentUtil.js";
import { AppTemplate } from "../../../../config/app-template.js";
import { PipelineService } from "../../../services/PipelineService.js";
import { WorkspaceService } from "../../../services/WorkspaceService.js";
import { StringUtil } from "../../../util/StringUtil.js";
import { switchActiveTab } from "../../../util/tabs.js";
import { Workspace } from "../../workspace/Workspace.js";

export const runStatus = { MANUAL_FAIL: 'Manual run failed', MANUAL_SUCCESS: 'Manual run success', PROGRESS: 'Running', RCHV: 'Archiving' }

export class PipelineRunSummary extends ViewComponent {

	isPublic = true;

	/** @Prop @type { HTMLElement } */
	container;

	/** @type { State<Array> } */
	runList;

	/** @type { State<Array> } */
	successRuns;

	/** @type { State<Array> } */
	archivedRuns;

	/** @type { Workspace } */
	$parent;

	/**
	 * @Inject @Path services/
	 * @type { PipelineService } */
	pplService;

	/** @Prop */ loading = false;

	/** @Prop */ showHistory = 1;

	/** @returns { HTMLElement } */ $ = (ref) => this.container.querySelector(ref);
    /** @returns { HTMLElement } */ $$ = (ref) => this.container.querySelectorAll(ref);

	async stBeforeInit(){
		await Assets.import({ path: '/app/components/pipeline/styles/shared.css', type: 'css' });
	}

	async stAfterInit(){
		this.container = document.querySelector(`.${this.cmpInternalId}`);
	}

	async immediateRun(pipelineName, execId){
		const showPlayIcon = false;
		this.handleUIElements(execId, runStatus.PROGRESS, showPlayIcon, !showPlayIcon);
		await PipelineService.immediatePipelineRun(pipelineName, execId);
	}

	async archiveFailedRun(pipelineName, execId){
		const showPlayIcon = false;
		this.handleUIElements(execId, runStatus.RCHV, showPlayIcon, !showPlayIcon);
		const result = await PipelineService.handlePipelineCheckpoint(pipelineName, execId, true);
		if(result?.error === false){
			AppTemplate.toast.success('Pipeline execution archived successfully');
			this.$parent.headerProxy.pipelinesErrorCount = parseInt(this.$parent.headerProxy.pipelinesErrorCount.value) - 1;
			return document.querySelector(`#rerun-icon-${execId}`).parentElement.parentElement.remove();
		}
		AppTemplate.toast.error(result.result,15000);
	}

	handleUIElements(execId, statDescription, showPlayIcon, showRunLoading){
		
		const statuLabel = document.querySelector(`#rerun-status-label-${execId}`);
		if(!statuLabel) return;

		statuLabel.classList.remove('rerun-loading-badge');
		statuLabel.classList.remove('rerun-loading-failed');
		statuLabel.classList.remove('rerun-loading-success');

		if(statDescription === runStatus.PROGRESS || statDescription === runStatus.RCHV)
			statuLabel.classList.add('rerun-loading-badge');
		
		if(String(statDescription) === runStatus.MANUAL_FAIL)
			statuLabel.classList.add('rerun-loading-failed');

		if(String(statDescription).includes('success'))
			statuLabel.classList.add('rerun-loading-success');
		

		statuLabel.innerHTML = statDescription;
		if(String(statDescription) === runStatus.MANUAL_SUCCESS)
			document.querySelector(`#rerun-icon-${execId}`).style.display = 'none';
		else if(String(statDescription).includes('success') === false)
			document.querySelector(`#rerun-icon-${execId}`).style.display = showPlayIcon ? '' : 'none';

		document.querySelector(`#rerun-loading-icon-${execId}`).style.display = showRunLoading ? '': 'none';
	}

	async getLogs(execId){

		const collapsableLogs = document.querySelector(`.collapsable-logs-${execId}`);
		if(collapsableLogs){
			if(collapsableLogs.style.display == 'none')
				return collapsableLogs.style.display = '';
			else
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

	async getSuccessPipelineHistory(status, el, grid){
		this.showHistory = grid;
		switchActiveTab(this, null, el)
		const result = await PipelineService.getPipelinesRunHistory(status);
		if(grid == 2)
			this.successRuns = result.map(PipelineRunSummary.parseRunListResult);
		if(grid == 3)
			this.archivedRuns = result.map(PipelineRunSummary.parseRunListResult);
			
	}
	
	static parseRunListResult(row){

		let { start_time: startTime, update_time: upTime } = row;
		
		row.start_time = new Date(startTime * 1000).toISOString().split('.')[0].replace('T',' '), 
		row.update_time = new Date(upTime * 1000).toISOString().split('.')[0].replace('T',' ');
		row.pipelineUniqueName = row.pipeline;
		row.pipeline = StringUtil.snakeToCamel(row.pipeline);

		return row;

	}

}