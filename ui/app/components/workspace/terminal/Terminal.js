import { ViewComponent } from "../../../../@still/component/super/ViewComponent.js";

class RunResponseType {
	error; result; status;
}

export class Terminal extends ViewComponent {

	isPublic = true;

	/** 
	 * @Prop 
	 * @type { HTMLElement }
	 * */
	terminal;

	/** @Prop */
	didFirstRun = false;

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
				padding-left: 50px;
				display: flex;
    			flex-direction: column;
			}

			.run-button{
				padding: 3px; position: absolute; color: white;
				cursor: pointer;
			}

			@keyframes blinking_cursor{
				0%{opacity: 0;}
				50%{opacity: .5;}
				100%{opacity: 1;}
			}

			.the-cursor-obj{
				border: 0.5px solid;
    			width: 7px;
				animation: blinking_cursor .2s linear infinite;
			}
		</style>
	`;

	stAfterInit(){
		this.terminal = document.getElementById(this.dynCmpGeneratedId);
		this.terminal.innerHTML = `${this.cursor({ topPos: 16 })}`;
	}

	resizeHeight(value){ this.terminal.style.height = value + 'px'; }

	/** @param { RunResponseType | string } output  */
	writeTerminal(output){
		try {
			//if(this.selectedLang == 'javascript') output = eval(output);
			this.removeOldCursor();
			if(this.didFirstRun){
				this.terminal.insertAdjacentHTML('beforeend', `${output}${this.cursor()}`)
			}else{
				// setting padding to display the first cursor (>) sign a
				// and last printed result fully
				this.terminal.style.paddingTop = '26px';
				this.terminal.style.paddingBottom = '60px';

				this.terminal.innerHTML = `><br>${output}${this.cursor({ topPos: -8 })}`;
				this.didFirstRun = true;
			}
			this.scrollToEnd();
		} catch (error) {
			this.terminal.innerHTML = `\nError on code submission`;
			this.didFirstRun = false;
		}
	}

	/** This is a signature  */
	onRun(){}

	cursor({ topPos = -13 } = {}){
		return `
			<div style="margin-top: ${topPos}px">> <div class="the-cursor-obj"></div></div>
		`;
	}

	removeOldCursor(){
		const obj = document.querySelector('.the-cursor-obj');
		obj.parentElement.removeChild(obj);
	}

	scrollToEnd(){
		this.terminal.scrollTo(0, this.terminal.scrollHeight);
	}
}