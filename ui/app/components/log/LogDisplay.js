import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { UUIDUtil } from "../../../@still/util/UUIDUtil.js";

export class LogDisplay extends ViewComponent {

	isPublic = false;

	/** @Prop */ showLogs = false;
	/** @Prop */ logOutput;
	/** @Prop @type { HTMLTableElement } */ logTableBody;
	/** @Prop */ scrollControlBtn;

    /** @Prop */ logLevels = {info: 'INFO', warn: 'WARN', error: 'ERROR'};
    /** @Prop */ maxBarWidth = 100; // Max width of the bar in pixels
    /** @Prop */ maxDelay = 2000; // Max possible delay in ms
    /** @Prop */ lastLogTime = null;
    /** @Prop */ isAutoScrollEnabled = true;
    /** @Prop */ logTableId = '_'+UUIDUtil.newId();
    /** @Prop */ autoScrollBtnId = '_autoScroll'+UUIDUtil.newId();
    /** @Prop */ logOutputContainerId = '_outLog'+UUIDUtil.newId();

	stAfterInit() {
		this.logOutput = document.getElementById(this.logOutputContainerId);
		this.logTableBody = document.querySelector(`#${this.logTableId} tbody`);
		this.scrollControlBtn = document.getElementById(this.autoScrollBtnId);
	}

	appendLogEntry(type, entry, time) {

		if(this.showLogs === false) this.showLogs = true;

		if(type === 'error' && entry.trim() == "")
			return;

		const now = new Date(time).getTime();
		if(this.lastLogTime === null) this.lastLogTime = now;
		const timeSinceLastLog = now - this.lastLogTime;
		const logStartTime = this.lastLogTime;
		const logEndTime = now;
		this.lastLogTime = now;

		const logEntry = this.createLogEntry(type, entry, timeSinceLastLog, logStartTime, logEndTime);
		this.logTableBody.appendChild(logEntry);

		if (this.isAutoScrollEnabled) {
			requestAnimationFrame(() => {
				this.logOutput.scrollTop = this.logOutput.scrollHeight;
			});
		}
	}

	getRandomElement(arr) {
		return arr[Math.floor(Math.random() * arr.length)];
	}

	createLogEntry(type, entry, timeGap, startTime, endTime) {

		const date = new Date();
		const timestamp = date.toISOString().slice(11, 23); // Format: HH:MM:SS.sss
		const logLevel = this.logLevels[type];
		const logMessage = entry;

		const row = document.createElement('tr');
		row.className = 'log-entry';

		const timestampCell = document.createElement('td');
		timestampCell.className = 'timestamp';
		timestampCell.textContent = timestamp;
		row.appendChild(timestampCell);

		const levelCell = document.createElement('td');
		levelCell.className = `log-level ${logLevel.toLowerCase()}`;
		levelCell.textContent = logLevel;
		row.appendChild(levelCell);

		const messageCell = document.createElement('td');
		messageCell.className = 'log-message';
		messageCell.textContent = logMessage;
		row.appendChild(messageCell);

		const timeGapCell = document.createElement('td');
		timeGapCell.className = 'time-gap-cell';
		const timeGapLabel = document.createElement('span');
		timeGapLabel.className = 'time-gap-ms';
		timeGapLabel.textContent = `${timeGap}ms`;
		timeGapCell.appendChild(timeGapLabel);
		row.appendChild(timeGapCell);

		const barCell = document.createElement('td');
		barCell.className = 'bar-cell';
		const barContainer = document.createElement('div');
		barContainer.className = 'bar-container';

		const barWidth = (timeGap / this.maxDelay) * this.maxBarWidth;

		const formattedStartTime = new Date(startTime).toISOString().slice(11, 23);
		const formattedEndTime = new Date(endTime).toISOString().slice(11, 23);
		const tooltipText = `Start: ${formattedStartTime}, End: ${formattedEndTime}`;

		const bar = document.createElement('div');
		bar.className = 'log-chart-bar';
		bar.style.width = `${Math.min(barWidth, this.maxBarWidth)}px`;
		bar.setAttribute('data-tooltip', tooltipText);

		barContainer.appendChild(bar);
		barCell.appendChild(barContainer);
		row.appendChild(barCell);

		return row;
	}

	clearLogs = () => this.logTableBody.innerHTML = '';
	showHideLogsDisplay = () => this.showLogs = !this.showLogs;
}