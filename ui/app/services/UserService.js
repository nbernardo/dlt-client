import { BaseService } from "../../@still/component/super/service/BaseService.js";

export class UserService extends BaseService {

    static auth0Client = null;
    userDetailes = null;

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

	logOut(){
		UserService.auth0Client.logout({ localOnly: true });
	}

    static async isAuthenticated(){
        if(UserService.auth0Client === null)
            UserService.auth0Client = await authConnect();
        
        return await UserService.auth0Client.isAuthenticated()
    }

}

async function authConnect(){
    return await auth0.createAuth0Client({
        domain: "dev-rmnn416pju788hwl.us.auth0.com",
        clientId: "lwfP9CGSSXC3Fvo1vVhHv21Pmm4oLdBG",
    });
}