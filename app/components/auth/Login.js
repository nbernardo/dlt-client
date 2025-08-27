import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { Router } from "../../../@still/routing/router.js";
import { StillAppSetup } from "../../../config/app-setup.js";
import { UserService } from "../../services/UserService.js";

export class Login extends ViewComponent {

	isPublic = true;

	/** @Prop */
	auth0Client;

	loginSuccess = null;

	/** @Prop */
	loggedUser = null;

	/**
	 * @Inject
	 * @Path services/
	 * @type { UserService }
	 */
	userService;

	async stAfterInit(){
		this.userService.on('load', () => this.userService.auth0Connect());
	}

	async login(provider){

		const { user, success } = await this.userService.login(provider);
		
		if(success === false) this.loginSuccess = false;

		if(user){
	 		this.loginSuccess = true;
	 		StillAppSetup.get().setAuthN(true);
	 		Router.goto('Workspace');
		}
		
	}

	logOut(){
		this.userService.logOut();
		Router.goto('exit');
	}

}