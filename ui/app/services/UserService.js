import { BaseService } from "../../@still/component/super/service/BaseService.js";
import { Router } from "../../@still/routing/router.js";
import { StillAppSetup } from "../../config/app-setup.js";

export class UserService extends BaseService {

    static auth0Client = null;
    userDetailes = null;
    static namespace = null;

    async auth0Connect(){
        UserService.auth0Client = await authConnect();
    }

	async login(provider){

		try{
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

	async logOut(){
		await UserService.auth0Client.logout({ localOnly: true });
        Router.goto('exit');
	}

    static async isAuthenticated(){
        await auth0GetConnection();
        return await UserService.auth0Client.isAuthenticated()
    }

    async getLoggedUser(){
        await auth0GetConnection();
        return UserService.auth0Client.getUser();
    }

    static async getNamespace(){
        if(UserService.namespace === null)
            UserService.namespace = (await new UserService().getLoggedUser()).email
        return UserService.namespace
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