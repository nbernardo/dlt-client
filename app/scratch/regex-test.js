// const regex = /\<st-element\b[^>]*>([\s\S]*?)<\/<st-element>/i;
// const data = `
// <st-element component="AdjustableContainer" >
//     <span>
//         This is the content
//     </span>
//     After and outside
// </st-element >
// `;

// const openAdjustable = /<st-element[\s]{0,}component="AdjustableContainer"[\s]{0,}>/;
// const closeAdjustable = /<\/st-element[\s]{0,}>/;
// const ll = openAdjustable.source + /([\s\S]*)/.source + closeAdjustable.source

// data.replace(new RegExp(ll), (_, mt2) => {

//     //console.log(`Content is: `, mt2);
//     console.log(`<mydiv>${mt2}</mydiv>`);

// });

// const dd = `
// <div class="@cmpInternalId">
// 	<div class="title-box">
// 		<div>
// 			<img src="app/assets/imgs/sql-server-2.png" width="20">
// 			@label
// 		</div>
// 		<div class="statusicon"></div>
// 	</div>
// 	<div class="box">
// 		<form (formRef)="formRef" onsubmit="return false;">
// 			<p>Database:</p>
// 			<input type="text" (required)="true" (value)="database" placeholder="Enter the DB name">
// 			<p>Table name 1:</p>
// 			<input type="text" (value)="tableName" placeholder="Enter table name">
// 		</form>
// 		<button onclick="component.addField()">Add field</button>
// 		<button onclick="component.validate()">Validate</button>
// 		<!--
// 			<form (formRef)="anotherForm" onsubmit="return false;">
// 				<p>Another Database:</p>
// 				<input type="text" (required)="true" (value)="database1" placeholder="Enter the DB name">
// 				<p>Another Table name 1:</p>
// 				<input type="text" (value)="tableName1" placeholder="Enter table name">
// 			</form>
// 			<button onclick="component.validate()">Validate1</button>
// 			<button onclick="component.validate1()">Validat2</button>
// 		-->
// 	</div>
// </div>
// `;

//console.log(dd.replace(/<!--[\s\S]*?-->/, ''));

const dd = `\n\t\t\t\t\t<div class="ppline-treeview">\n\t\t\t\t\t\t<span class="ppline-treeview-label"> \n    <img \n        src="app/assets/imgs/pipeline.png" \n        class="tbl-to-terminal">\n noutro_teste.duckdb </span>\n\t\t\t\t\t\t<span onclick="self.viewPipelineDiagram($event,'noutro_teste.duckdb')">\n    <i class='fas fa-eye' \n        style='margin-left: -23px; \n               position:absolute; \n               color: grey;\n               z-index: 1000'>\n    </i>\n<span>\n\t\t\t\t\t</div>`;

console.log(dd.replace(/self./,'vamosSim'));

