import { ViewComponent } from "../../../@still/component/super/ViewComponent.js";
import { State } from "../../../@still/component/type/ComponentType.js";
import { Router } from "../../../@still/routing/router.js";
import { StillAppSetup } from "../../../config/app-setup.js";
import { UserService } from "../../services/UserService.js";

const env = (_var) => StillAppSetup.config.get(_var)

export class Login extends ViewComponent {

	isPublic = true;

	/** @Prop */
	auth0Client;

	loginSuccess = null;

	/** @Prop */
	loggedUser = null;

	/** @Prop */
	isAnonumousLogin = env('anonymousLogin');

	/** @Prop */
	devAuthN = env('devauthn.active');

	/** @Prop */
	activeTab = 'managed';

	/**
	 * @Inject @Path services/
	 * @type { UserService }
	 */
	userService;

	/** @type { State<String> } */
	username;

	/** @type { State<String> } */
	password;

	async stAfterInit(){
		this.userService.on('load', () => this.userService.auth0Connect());
	}

	async login(provider){

		if(this.isAnonumousLogin){
			this.userService.anonymousLogin();
			return this.handleSuccessLogin();
		}
		
		if(this.devAuthN){ this.username = env('devauthn.user'), this.password = env('devauthn.pwd'); }

		let { username, password } = this;
		const { user, success } = await this.userService.login(provider, {username: username.value, password: password.value });
		
		if(success === false) this.loginSuccess = false;
		if(user)  this.handleSuccessLogin();
		
	}

	handleSuccessLogin(){
	 	this.loginSuccess = true;
	 	StillAppSetup.get().setAuthN(true);
	 	Router.goto('Workspace');
	}

	logOut(){
		this.userService.logOut();
		Router.goto('exit');
	}

	switchLogin(tab){ this.activeTab = tab }

}