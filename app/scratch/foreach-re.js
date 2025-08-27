/*
const extremRe = /[\n \r \<\> \$ \( \) \. \;\: \-\_ \s A-Za-z0-9 \= \"]{0,}/.source;
const matchForEach = /<[a-zA-Z0-9\s\n\t\r\"\=\-\_\(\)\.\$ \;\:]{0,}(\(forEach\))\=\"(\w*){0,}\"/.source;

const re = new RegExp(matchForEach + extremRe, 'gi');

const dd = `
<div class="dynamic-_cmp6878985875976165SqlDBComponent">
   \n\t
   <div class="title-box">
      \n\t\t
      <div>
         \n\t\t\t<img src="app/assets/imgs/sql-server-2.png" width="20">\n\t\t\t
         <state class="state-change-dynamic-_cmp6878985875976165SqlDBComponent-label">SQL DB Source</state>
         \n\t\t
      </div>
      \n\t\t
      <div class="statusicon"></div>
      \n\t
   </div>
   \n\t
   <div class="box">
      \n\t\t
      <form id="fId_formRef" onsubmit="return false;">
         \n\t\t\t<select (required)="true" \n\t\t\t\t\t(forEach)="databaseEngines" \n\t\t\t\t\t \n\t\t\t\t\tplaceholder="Database Engine"data-formRef="formRef" data-field="selectedDbEngine" data-cls="SqlDBComponent" class="still-validation-class still-validation-class-dynamic-_cmp6878985875976165SqlDBComponent-formRef-selectedDbEngine  -combobox dynamic-_cmp6878985875976165SqlDBComponent-selectedDbEngine onChange_41392115177487865 ">\n\t\t\t\t
         <option each="item" value="{item.dialect}">{item.name}</option>
         \n\t\t\t</select>\n\t\t\t
         <p>Database:</p>
         \n\t\t\t<input type="text" (required)="true"  value=""  class="still-validation-class still-validation-class-dynamic-_cmp6878985875976165SqlDBComponent-formRef-database listenChangeOn-dynamic-_cmp6878985875976165SqlDBComponent-database  dynamic-_cmp6878985875976165SqlDBComponent-database"  onkeyup="$still.c.ref('dynamic-_cmp6878985875976165SqlDBComponent').onValueInput(event,'database',this, 'formRef')">\n\t\t\t
         <p>Tables list:</p>
         \n\t\t\t<input type="text" \n\t\t\t\t\t(required)="true" \n\t\t\t\t\t value=""  class=" listenChangeOn-dynamic-_cmp6878985875976165SqlDBComponent-tableName  dynamic-_cmp6878985875976165SqlDBComponent-tableName"  onkeyup="$still.c.ref('dynamic-_cmp6878985875976165SqlDBComponent').onValueInput(event,'tableName',this, 'formRef')" placeholder="Enter table 1 name">\n\t\t
      </form>
      \n\t\t<button onclick="component.addField()" class="add-table-buttons">Add Table field</button>\n\t\t\n\t
   </div>
   \n
</div>
\n
<style>\n\t.add-table-buttons {\n\t\tmargin-top: 5px;;\n\t}\n</style>
`;

dd.replace(re, (mt) => {

    console.log(mt);
    

})
*/

const matchShowIfRE = /\(showIf\)\="(\!){0,}[A-Za-z0-9 \. \( \)]{0,}\"/g;

const dd = `
<div>
	<div class="still-cmp-loader" (showIf)="self.showLoading"></div>
	<div id="drawflowContainer" (showIf)="!self.loadCompleted">

		<header>
			<h2>Workspace</h2>
		</header>
		<div class="wrapper">
			<div class="col" id="still-adj-left-panel">
				<div class="tabset">
					<input type="radio" name="tabset" id="tab1" aria-controls="data-panel">
					<label for="tab1" onclick="component.showHideDatabase()">Data</label>
					
					<input type="radio" name="tabset" id="tab2" aria-controls="draw-diagram-tab" checked>
					<label for="tab2">Diagram</label>			
					<div class="tab-panels">
						<section id="data-panel" class="tab-panel">
							<st-element 
								showBullets="false"
								component="@treeview/StillTreeView"
								tooltipXPos="10"
								(onRefreshClick)="refreshTree()"
								showRefresh="false"
								tooltipText="Update tree data"
								proxy="dbTreeviewProxy">
							</st-element>
						</section>
						<section id="draw-diagram-tab" class="tab-panel">
							<span (forEach)="objectTypes">
								<st-element component="ObjectType" each="item"></st-element>
							</span>
						</section>
					</div>
				</div>
			</div>

			<st-divider type="horizontal"/>

			<div class="col-right" id="still-adj-right-panel">
				<div class="menu">
					<ul>
						<li onclick="controller.changeModule('Home'); controller.changeModule(event);" class="selected">
							<div onkeypress="component.onPplineNameKeyPress(event)" contenteditable="true">
								<state class="state-change-_cmp628495839644402-activeGrid">pipeline_name</state>
							</div>
						</li>
						<li onclick="controller.changeModule('Other'); controller.changeModule(event);">Space 2
						</li>
					</ul>
				</div>

				<div id="drawflow" ondrop="controller.drop(event)" ondragover="controller.allowDrop(event)">

					<div class="btn-export" onclick="component.savePipeline()">
						<i class="fa fa-save"></i> Save
					</div>
					<div class="btn-clear" onclick="component.editor.clearModuleSelected()">Clear</div>
					<div class="btn-lock">
						<i id="lock" class="fas fa-lock"
							onclick="component.editor.editor_mode='fixed'; controller.changeMode('lock');"></i>
						<i id="unlock" class="fas fa-lock-open"
							onclick="component.editor.editor_mode='edit'; controller.changeMode('unlock');"
							style="display:none;"></i>
					</div>
					<div class="bar-zoom">
						<i class="fas fa-search-minus" onclick="component.editor.zoom_out()"></i>
						<i class="fas fa-search" onclick="component.editor.zoom_reset()"></i>
						<i class="fas fa-search-plus" onclick="component.editor.zoom_in()"></i>
					</div>
				</div>

				<st-divider 
					type="vertical" 
					label="&nbsp;&lt;&#47;&gt; Code eitor" 
					(onResize)="onSplitterMove(params)"
					minHeight="65"
					maxHeight="900"
					proxy="codeEditorSplitter"
					(onLblClick)="showHideEditor()"
				/>
				<div style="display: contents;">
					<div class="terminal-mode-list">
						Mode:&nbsp;&nbsp; 
						<div 
							onclick="component.selecteLang('python-lang')"
							class="editor-lang editor-lang-mode-selected python-lang"> 
								Python <img src="app/assets/imgs/python_lang.png"> 
						</div>
						<div class="separator"> / </div>
						<div 
							onclick="component.selecteLang('sql-lang')"
							class="editor-lang editor-lang-mode sql-lang"> SQL 
						</div>
					</div>
					<st-element 
						proxy="cmProxy" 
						editorHeight="160"
						startHeight="7"
						component="@codemirror/CodeMiror">
					</st-element>
				</div>
				<st-element 
					proxy="terminalProxy"
					(onRun)="runCode()"
					component="Terminal"></st-element>
			</div>
		</div>
	</div>
</div>

<style>
	.wrapper .divider {
		height: 25px;
		padding-top: 2px;
		padding-bottom: 2px;
		font-size: 14px;
		color: rgb(86, 84, 84);
	}

	.wrapper .divider .label {
		cursor: pointer;
	}

	.terminal-mode-list{
		position: relative; 
		padding: 3px 0px 3px 5px;
		font-size: 13px;
		background-color: white;
		display: flex;
	}

	.terminal-mode-list .separator{
		padding-left: 7px;
		padding-right: 10px;
		background: no-repeat;
		display: flex;
		padding-top: 5px;
	}

	.editor-lang-mode, .editor-lang-mode-selected {
		display: flex;
		padding: 3px;
		border-radius: 6px;
		border: 1px solid #c9c4c4;
		color: #939090;
		cursor: pointer;
		font-weight: normal;
	}

	.editor-lang-mode-selected {
		border: 1px solid black !important;
		color: black !important;
		font-weight: bold !important;
	}

	.editor-lang-mode img, .editor-lang-mode-selected img {
		width: 15px;
		margin-left: 5px;
		opacity: .4;
	}

	.editor-lang-mode-selected img{
		opacity: 1 !important;
	}

	.table-in-treeview, .ppline-treeview{
		display: flex;
		justify-content:space-between;
	}

	.tbl-to-terminal {
		width: 13px; opacity: .8; cursor: pointer;
	}

	.tables-icn-container {
		width: 15px; left: -6px; cursor: pointer; display: flex;
	}

	.tables-icn-container span:nth-child(1){ margin-left: -28px;}
	.tables-icn-container span:nth-child(2){ margin-left: 5px;}

	.ppline-treeview .ppline-treeview-label{ margin-left: -25px; display: flex; }
	.ppline-treeview { margin-left: 0px; display: flex; }
	.ppline-treeview img{ width: 30px; position: relative; z-index: 1000; }

</style>
`;


dd.replace(matchShowIfRE, (mt, mt2) => {

   console.log(`MATCH IS: `, mt);

});
