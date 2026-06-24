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

	/** @Prop */
	activeIdiom = 'pt';

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

	changeIdiom = (idiom) => this.activeIdiom = idiom;

	handleSuccessLogin(){
	 	this.loginSuccess = true;
	 	StillAppSetup.get().setAuthN(true);
	 	Router.goto('Workspace', { urlParams: `lang=${this.activeIdiom}` });
	}

	logOut(){
		this.userService.logOut(this.activeTab);
		Router.goto('exit');
	}

	switchLogin(tab, btn){ 
		this.activeTab = tab;
		document
			.querySelectorAll('.auth-tabs .tab-btn')
			.forEach(b => b.classList.remove('active'));

		btn.classList.add('active'); 
	}

}