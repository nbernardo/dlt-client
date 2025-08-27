import { ViewComponent } from "../../@still/component/super/ViewComponent.js";

export class HomeComponent extends ViewComponent {

    isPublic = true;
    template = `
        <div>
            <div>This is the top part</div>
            <st-divider type="vertical"/>
            <div>This is the bottom part</div>
        </div>
    `;

    constructor() {
        super();
    }

}