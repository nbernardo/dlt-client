const extremRe = /[\n \r \t \< \$ \( \) \- \s A-Za-z0-9 \@ \= \" \.]{0,}/.source;
const matchRenderIfRE = /\(renderIf\)\="[A-Za-z0-9 \. \( \)]{0,}\"/;
const matchShowIfRE = /\(showIf\)\="[A-Za-z0-9 \. \( \)]{0,}\"/;
const reSIf = new RegExp(extremRe + matchShowIfRE.source + extremRe, 'gi');
const reRIf = new RegExp(extremRe + matchRenderIfRE.source + extremRe, 'gi');

const dd = `
<ul class="still-tree-view" id="st_7156413390651353">
					
					<span 
						class="tree-refresh-container" 
						tooltip-x="10"
						(showIf)="self.showRefresh"
						tooltip="Refresh the tree">
						<svg
							(click)="onRefreshClick()" 
							width="15"
							id="Layer_1" data-name="Layer 1" 
							xmlns="http://www.w3.org/2000/svg" 
							viewBox="0 0 122.61 122.88">
							<title>update</title>
							<path d="M111.9,61.57a5.36,5.36,0,0,1,10.71,0A61.3,61.3,0,0,1,17.54,104.48v12.35a5.36,5.36,0,0,1-10.72,0V89.31A5.36,5.36,0,0,1,12.18,84H40a5.36,5.36,0,1,1,0,10.71H23a50.6,50.6,0,0,0,88.87-33.1ZM106.6,5.36a5.36,5.36,0,1,1,10.71,0V33.14A5.36,5.36,0,0,1,112,38.49H84.44a5.36,5.36,0,1,1,0-10.71H99A50.6,50.6,0,0,0,10.71,61.57,5.36,5.36,0,1,1,0,61.57,61.31,61.31,0,0,1,91.07,8,61.83,61.83,0,0,1,106.6,20.27V5.36Z"/>
						</svg>
					</span>
					<div class="tree-view-blank-space"></div>
				</ul>
				<style>
					.tree-refresh-container{
						margin-left: 15px;
						position: absolute;
						margin-top: -20px;
					}

					.tree-view-blank-space{
						height: 20px;
					}
				</style>

`;

dd.replace(reSIf, (mt) => {

    console.log(mt);
    

})