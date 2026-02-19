
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
	updateDashboardTable(data);
	
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


function sanitizeId(pipelineId) {
    return pipelineId.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function updateDashboardTable(data) {
    const tableBody = document.getElementById('leaderboardBody');
    if (!tableBody) return;

	
	
    const rowsHtml = data.pipelines.map(pipe => {
		const isFailing = pipe.failed_count > 0;
        const statusClass = isFailing ? 'failing' : 'ok';
        const statusText = isFailing ? '● FAILING' : '● OK';
        const safeId = sanitizeId(pipe.pipeline_id);
		const formattedRecords = pipe.total_records > 999 
			? (pipe.total_records / 1000).toFixed(1) + 'k' 
			: pipe.total_records;

        return `
            <tr class="pipeline-row">
                <td class="col-name" title="${pipe.pipeline_id}">${pipe.pipeline_id}</td>
                <td class="col-runs">${pipe.total_runs}</td>
                <td class="col-success">${pipe.success_count}</td>
                <td class="col-failed">${pipe.failed_count}</td>
                <td class="align-center">${pipe.avg_duration || '0'}s</td>
                <td class="align-center">${formattedRecords}</td>
                <td class="align-center"><span class="status-pill ${statusClass}">${statusText}</span></td>
                <td class="col-trend">
                    <canvas id="canvas-${safeId}" width="120" height="30"></canvas>
                </td>
            </tr>
        `;
    }).join('');

    tableBody.innerHTML = rowsHtml;

    requestAnimationFrame(() => {
        data.pipelines.forEach(pipe => {
            const safeId = sanitizeId(pipe.pipeline_id); // ← same sanitize here
            const canvas = document.getElementById(`canvas-${safeId}`);

			const existingChart = Chart.getChart(`canvas-${safeId}`);
			if (existingChart) existingChart.destroy();

            if (canvas) {
                drawSparkline(canvas, pipe);
            } else {
                console.error(`Canvas not found: canvas-${safeId}`);
            }
        });
    });
}

function drawSparkline(canvas, pipe) {
    const isFailing = pipe.failed_count > 0;
    const existing = Chart.getChart(canvas);
    if (existing) existing.destroy();

    canvas.style.display = 'block';
    canvas.style.visibility = 'visible';
    canvas.style.opacity = '1';
    canvas.width = 120;
    canvas.height = 30;

    const ctx = canvas.getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: [1, 2, 3, 4, 5, 6, 7],
            datasets: [{
                data: pipe.trend_data,
                borderColor: isFailing ? '#ef4444' : '#22c55e',
                backgroundColor: isFailing ? 'rgba(239,68,68,0.1)' : 'rgba(34,197,94,0.1)',
                fill: true,
                borderWidth: 1.5,
                pointRadius: 0,
				pointHoverRadius: 4,
				pointHitRadius: 20,
                tension: 0.4
            }]
        },
		options: {
			responsive: false,
			maintainAspectRatio: false,
			interaction: {
				mode: 'index',
				intersect: false
			},
			plugins: { 
				legend: { display: false },
				tooltip: {
					enabled: true,
					callbacks: {
						title: () => '',
						label: (ctx) => `${ctx.parsed.y} runs`
					}
				}
			},
			scales: { x: { display: false }, y: { display: false } },
		}
    });
}

