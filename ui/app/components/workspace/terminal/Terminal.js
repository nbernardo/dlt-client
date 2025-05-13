import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";

export class Terminal extends ViewComponent {

	isPublic = true;

	/** 
	 * @Prop 
	 * @type { HTMLElement }
	 * */
	terminal;

	/** @Prop */
	terminalClassName = '.generater_class'+ this.dynCmpGeneratedId;

	/** @Prop */
	runStatus = false;

	/** @Prop */
	selectedLang = 'Python'

	template = `
		<div class="terminal-container">
			<div class="run-button" onclick="component.onRun()">Run</div>
			<div id="@dynCmpGeneratedId" class="terminal-output">
				
			</div>
		</div>
		<style>
			.terminal-output {
				overflow-y: auto;
				background: #1e1e1e;
				color: #00ff00;
				padding: 10px;
				font-size: 12px;
				white-space: pre-wrap;
				margin-top:-20px;
				padding-left: 80px;
			}

			.run-button{
				padding: 3px; position: absolute; color: white;
				cursor: pointer;
			}
		</style>
	`;

	stAfterInit(){
		this.terminal = document.getElementById(this.dynCmpGeneratedId);
		this.terminal.innerHTML = `\n> $`;
	}

	resizeHeight(value){ this.terminal.style.height = value + 'px'; }

	writeTerminal(output){
		try {
			if(this.selectedLang == 'javascript') output = eval(output);
			if(this.runStatus){
				this.terminal.insertAdjacentHTML('beforeend', `<br>${output}`)
			}else{
				this.terminal.innerHTML = `\n${output}`;
				this.runStatus = true;
			}
		} catch (error) {
			this.terminal.innerHTML = `\nError on running the code`;
			this.runStatus = false;
		}
	}

	/** This is a signature  */
	onRun(){}
}