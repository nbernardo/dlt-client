import { $still } from "../../@still/component/manager/registror.js";
import { BaseService } from "../../@still/component/super/service/BaseService.js";
import { HTTPHeaders } from "../../@still/helper/http.js";
import { Router } from "../../@still/routing/router.js";
import { StillAppSetup } from "../../config/app-setup.js";

class UserModel { username; password; }

export class UserService extends BaseService {

    static auth0Client = null;
    userDetailes = null;
    static namespace = null;
    managedUser = false;

    async auth0Connect(){ UserService.auth0Client = await authConnect(); }

	async login(provider, { username, password } = UserModel){

		try{

            if(provider === 'managed'){
                this.managedUser = true;
                const creds = {username, password};
                let jwt = await $still.HTTPClient.post('/user/login', JSON.stringify(creds), HTTPHeaders.JSON);
                jwt = await jwt.json()
                if('tenant' in (jwt || {})){  
                    const user = { name: jwt.username, email: jwt.tenant, tkn: null, permissions: jwt.permissions, userEmail: username };
                    localStorage.setItem('loggedIn', JSON.stringify(user))
                    this.userDetailes = { user, success: true, exception: false };
                    UserService.namespace = jwt.tenant;
                    return this.userDetailes;
                }else
                    return { success: false, exception: true, user: null };
            }

			await UserService.auth0Client.loginWithPopup({ connection: provider });
			const isAuthenticated = await UserService.isAuthenticated();
			
			if (isAuthenticated) {
                this.userDetailes = { user: await UserService.auth0Client.getUser(), success: true, exception: false };
                return this.userDetailes;
            };
			return { success: false, exception: true, user: null };

		} catch (err) {
			return { success: false, exception: true, user: null };
		}
	  
	}

	async anonymousLogin(){
        this.userDetailes = { user: {name: 'Anonymous', 'email': 'anonymous-dlt@none.dlt'}, success: true, exception: false };
        return this.userDetailes;	  
	}

	async logOut(loginType){
        localStorage.removeItem('loggedIn');
        if(loginType !== 'managed')
		    await UserService.auth0Client.logout({ localOnly: true });
        Router.goto('exit');
	}

    static async isAuthenticated(){
        await auth0GetConnection();
        return await UserService.auth0Client.isAuthenticated() || localStorage.getItem('loggedIn')
    }

    async getLoggedUser(){
        if(localStorage.getItem('loggedIn'))
            return { user: JSON.parse(localStorage.getItem('loggedIn')) };

        if(this.managedUser) return this.userDetailes;

        const anonymousLogin = StillAppSetup.config.get('anonymousLogin');
        if(anonymousLogin){
            return new Promise(async resolve => {
                const login = await this.anonymousLogin();
                resolve(login);
            });
        }
        await auth0GetConnection();
        return UserService.auth0Client.getUser();
    }

    static async getNamespace(){
        if(localStorage.getItem('loggedIn')){
            return UserService.namespace = JSON.parse(localStorage.getItem('loggedIn')).email;
        }
        if([null,undefined].includes(UserService.namespace)){
            const auth = (await new UserService().getLoggedUser());
            if(auth?.user?.name == 'Anonymous') return auth.user.email;
            UserService.namespace = (await new UserService().getLoggedUser())?.sub.replace('|','_')
            || (await new UserService().getLoggedUser())?.email
        }
        return UserService.namespace;
    }

    async getUsersList(){
        let users = await $still.HTTPClient.get('/user', HTTPHeaders.BearerTkn(this.getTkn()));
        return await users.json();
    }

    async createIdentity({ name, email, password }){
        const userDetails = { username: name, email, password };
        let result = await $still.HTTPClient.post('/user', JSON.stringify(userDetails), HTTPHeaders.JSONAndBearerTkn(this.getTkn()));
        return await result.json();
    }

    async updateIdentity({ permissions, email }){
        permissions = { permissions: permissions.join(','), email };
        let result = await $still.HTTPClient.post('/user/permission', JSON.stringify(permissions), HTTPHeaders.JSONAndBearerTkn(this.getTkn()));
        return await result.json();
    }

    async getAllPemissions(){
        let result = await $still.HTTPClient.get('/user/rbac/catalog', HTTPHeaders.JSONAndBearerTkn(this.getTkn()));
        return await result.json();
    }

    getTkn = () => this.userDetailes?.user?.tkn;

    async saveTableAccessLevel(roleName, tablesConstraint){
        const url = '/user/rbac/table';
        const perms = { roleName, tablesConstraint };
        let result = await $still.HTTPClient.post(url, JSON.stringify(perms), HTTPHeaders.JSONAndBearerTkn(this.getTkn()));
        return await result.json();
    }

    async getAccessLevelByRole(roleName, pipeline){
        const namespace = await UserService.getNamespace();
        const url = `/role/${roleName}/${namespace}/${pipeline}`;
        let result = await $still.HTTPClient.get(url, HTTPHeaders.JSONAndBearerTkn(this.getTkn()));
        return await result.json();
    }

}

async function auth0GetConnection(){
    if(UserService.auth0Client === null)
        UserService.auth0Client = await authConnect();
}

async function authConnect(){
    return await auth0.createAuth0Client({
        domain: StillAppSetup.config.get('auth0.domain'),
        clientId: StillAppSetup.config.get('auth0.clientId'),
    });
}