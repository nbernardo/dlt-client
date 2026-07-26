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

	/** @Prop */ loggedUser = null;

	/** @Prop */ isAnonumousLogin = env('anonymousLogin');

	/** @Prop */ devAuthN = env('devauthn.active');

	/** @Prop */ activeTab = 'managed';

	/** @Prop */ activeIdiom = 'pt';

	/** @Prop */ showTabs = true;

	/** @Prop */ pwdChangeAlrtShow = false;

	/** @Prop */ loading = false;

	/**
	 * @Inject @Path services/
	 * @type { UserService } */
	userService;

	/** @type { State<String> } */
	username;

	/** @type { State<String> } */
	password;
	
	/** @type { State<String> } */
	oldPassword;
	
	/** @type { State<String> } */
	passwordConfirm;

	/** @type { State<String> } */
	pwdChangeAlrtMsg;

	stBeforeInit(){
		this.pwdChangeAlrtMsg = '';
		
		if(Router.routeUrlParams?.pwd === 'change'){
			this.showTabs = false;
			this.username = Router.data(this).user;
			this.activeTab = 'passwordChange';
		}
	}

	async stAfterInit(){
		this.pwdChangeAlrtShow = false;
		this.userService.on('load', () => this.userService.auth0Connect());
	}

	async login(provider){
		this.loading = true;
		if(this.isAnonumousLogin){
			this.userService.anonymousLogin();
			this.loading = false;
			return this.handleSuccessLogin();
		}
		
		if(this.devAuthN){ this.username = env('devauthn.user'), this.password = env('devauthn.pwd'); }

		let { username, password } = this;
		const { user, success, passwordRst } = await this.userService.login(provider, {username: username.value, password: password.value });
		if(passwordRst){
			this.password = '', this.passwordConfirm = '', this.oldPassword = '';
			this.showTabs = false, this.loading = false;
			return this.activeTab = 'passwordChange';
		}
		
		if(success === false) this.loginSuccess = false;
		this.loading = false;
		if(user)  this.handleSuccessLogin();
		
	}

	async changePassword(){
		this.loading = true, this.pwdChangeAlrtShow = false;
		let { password, passwordConfirm, oldPassword, username } = this;
		const payload = { username: username.value, password: password.value, passwordConfirm: passwordConfirm.value, oldPassword: oldPassword.value }
		const { user, success, error } = await this.userService.passwordChange(payload);
		if([false,null,undefined].includes(success)){
			this.pwdChangeAlrtShow = true, this.pwdChangeAlrtMsg = error;
		}else
			if(user)  this.handleSuccessLogin();
		this.loading = false;
		
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
		document.querySelectorAll('.auth-tabs .tab-btn').forEach(b => b.classList.remove('active'));

		btn.classList.add('active'); 
	}

}