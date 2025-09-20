import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Router } from "../../../@still/routing/router.js";
import { AppTemplate } from "../../../config/app-template.js";
import { UserUtil } from "../auth/UserUtil.js";

export class Header extends ViewComponent {

	isPublic = true;

	/** 
	 * @Inject 
	 * @Path services/ 
	 * @type { UserService }*/
	userService;

	loggedUser = null;

	stAfterInit(){
		
		this.userService.on('load', async () => {

			const user = (await this.userService.getLoggedUser());
			if(UserUtil.name === null){
				UserUtil.name = user.name;
				UserUtil.email = user.email;
				Object.freeze(UserUtil);
			}
			this.loggedUser = user.name;
			this.userEmail = user.email;

		});
		
	}

	gotoConfig(){
		//AppTemplate.showLoading();
		Router.goto('Config');
	}

}