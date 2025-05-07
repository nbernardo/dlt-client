import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";

export class Terminal extends ViewComponent {

	isPublic = true;

	/** 
	 * @Prop 
	 * @type { HTMLElement }
	 * */
	terminal;

	/** @Prop */
	terminalClassName = '.generater_class'+ this.dynCmpGeneratedId

	template = `
		<div>
			<div class="run-button" onclick="component.onRun()">Run</div>
			<div 
				id="@dynCmpGeneratedId" class="terminal-output"
				>
				Terminal ready...\n
			</div>
		</div>
		<style>
			.terminal-output {
				overflow-y: auto;
				background: #1e1e1e;
				color: #00ff00;
				padding: 10px;
				font-size: 14px;
				white-space: pre-wrap;
				margin-top:-20px;
			}

			.run-button{
				padding: 3px; position: absolute; color: white;
				cursor: pointer;
			}
		</style>
	`;

	stAfterInit(){
		this.terminal = document.getElementById(this.dynCmpGeneratedId);
	}

	resizeHeight(value){ this.terminal.style.height = value + 'px'; }

	writeTerminal(code){
		try {
			const output = eval(code);
			this.terminal.innerHTML = `\n> ${output}`;
		} catch (error) {
			this.terminal.innerHTML = `Error on running the code`;
		}
	}


	onRun(){}
}