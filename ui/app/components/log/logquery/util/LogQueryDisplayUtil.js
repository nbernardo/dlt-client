
export function loadDonutChart(data){

	// Data from your JSON
	const summaryData = {
		labels: ['Success', 'Failed', 'Warning'],
		datasets: [{
			data: [
				data.summary.success_count, 
				data.summary.failed_count, 
				data.summary.warning_count
			],
			backgroundColor: [
				'#4caf50', // Success Green
				'#f44336', // Failed Red
				'#ff9800'  // Warning Orange
			],
			borderWidth: 0,
			hoverOffset: 4,
			cutout: '75%' // Creates the thin donut look
		}]
	};
	
	const centerTextPlugin = {
		id: 'centerText',
		afterDraw: (chart) => {
			const { width, height, ctx } = chart;
			ctx.save();
			
			const centerX = width / 2;
			const centerY = height / 2;
			const totalRuns = data.summary.total_runs;

			// 1. Big Number - Pure White
			ctx.textAlign = 'center';
			ctx.textBaseline = 'middle';
			ctx.fillStyle = '#FFFFFF'; // Explicit White
			ctx.font = `bold ${Math.floor(height / 6)}px sans-serif`;
			ctx.fillText(totalRuns, centerX, centerY - 8);

			// 2. Label - Light Grey
			ctx.fillStyle = '#94a3b8'; // Muted slate color
			ctx.font = `${Math.floor(height / 16)}px sans-serif`;
			ctx.fillText("TOTAL RUNS", centerX, centerY + (height / 8));
			
			ctx.restore();
		}
	};

	const config = {
		type: 'doughnut',
		data: summaryData,
		options: {
			responsive: true,
			maintainAspectRatio: true, 
			aspectRatio: 1, 
			cutout: '86%', 
			plugins: {
				legend: { display: false },
				tooltip: { enabled: true }
			}
		},
		plugins: [centerTextPlugin] 
	};
	
	const ctx = document.getElementById('thisIsTheDonutGraph').getContext('2d');
	new Chart(ctx, config);

	updateDashboardHeader(data);
	updateLegend(data.summary);
	renderPipelineList(data);
	
} 



function updateLegend(summary) {

    const target = document.getElementById('thisIsTheDonutGraphChartLegend');
    const { success_count, failed_count, warning_count, total_runs } = summary;

    const rows = [
        { label: 'Success', count: success_count, color: '#4caf50' },
        { label: 'Failed',  count: failed_count,  color: '#f44336' },
        { label: 'Warning', count: warning_count, color: '#ff9800' }
    ];

    target.innerHTML = rows.map(row => {
        const pct = total_runs > 0 ? ((row.count / total_runs) * 100).toFixed(1) : 0;
        return `
            <div class="item">
                <div class="left-group">
                    <span class="dot" style="background-color: ${row.color}"></span>
                    <span class="label">${row.label}</span>
                </div>
                <div class="right-group">
                    <span class="count" style="color: ${row.color}">${row.count}</span>
                    <span class="percent">${pct}%</span>
                </div>
            </div>
        `;
    }).join('');

}


function renderPipelineList(jsonData) {
    const target = document.querySelector('#thisIsThePipelineListContainer .pipelineListTarget');
    
    target.innerHTML = '';

    const rows = jsonData.pipelines.map(pipe => {
		let dotColor = '#4caf50'; // Success Green
		if (pipe.status_indicator === 'Failed') dotColor = '#f44336'; // Failed Red
		if (pipe.status_indicator === 'Warning') dotColor = '#ff9800'; // Warning Orange

		return `
			<div class="pipeline-row">
				<div class="pipeline-name" title="${pipe.pipeline_id}">${pipe.pipeline_id}</div>
				<div class="pipeline-stats" style="display: flex; align-items: center;">
					<span class="run-count">${pipe.total_runs} runs</span>
					<span class="status-dot" style="background-color: ${dotColor}"></span>
				</div>
			</div>
		`;
	}).join('');

    target.innerHTML = rows;
}


function updateDashboardHeader(data) {
    const totalRunsElement = document.querySelector('#thisIsTheRunStatusSummaryHeader .dynamicTotalRuns');
    if (totalRunsElement && data.summary) {
        totalRunsElement.innerText = `${data.summary.total_runs} Total Runs`;
    }
}
