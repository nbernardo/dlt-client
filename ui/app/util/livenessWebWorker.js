import { StillHTTPClient } from "../../@still/helper/http.js";
import { Header } from "../components/parts/Header.js";

/** @param { Header } workspaceHeader */
export function startPingWorker(workspaceHeader){

    const workerCode = `
        // Listen for configuration messages if needed
        self.onmessage = function(e) {
            const pingInterval = setInterval(async () => {
                try {
                    const pingResult = await fetch(e.data.endpoint+'/workspace/live', { method: 'GET' });
                    if((await pingResult.text()) === 'Ok')
                        self.postMessage({ live: true })
                    else
                        self.postMessage({ live: false })
                } catch (err) { self.postMessage({ live: false }) }
            }, 5000);
        };
    `;

    console.log(`Starting ping worker`);
    const blob = new Blob([workerCode], { type: 'application/javascript' });
    const workerUrl = URL.createObjectURL(blob);
    const pingWorker = new Worker(workerUrl);

    URL.revokeObjectURL(workerUrl);
    pingWorker.postMessage({ endpoint: StillHTTPClient.getBaseUrl() })
    const placeHolder = document.querySelector('.application-status-semaphore');

    pingWorker.onmessage = (evt) => {
        placeHolder.style.background = evt.data.live ? 'green' : 'red';
        placeHolder.parentElement.style.color = evt.data.live ? 'green' : 'red';
    }

}