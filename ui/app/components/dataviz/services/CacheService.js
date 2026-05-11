export class CacheService {
    static db = null;

    static async createCache() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open("LargeDataCache", 1);

            request.onupgradeneeded = (event) => {
                const database = event.target.result;
                if (!database.objectStoreNames.contains("cachedObjects"))
                    database.createObjectStore("cachedObjects");
            };

            request.onsuccess = (event) => {
                CacheService.db = event.target.result;
                resolve(CacheService.db);
            };

            request.onerror = (event) => {
                console.error("Database error:", event.target.errorCode);
                reject(event.target.error);
            };
        });
    }

    static async add(customStringKey, objectForCache = {}) {
        if (!CacheService.db) await CacheService.createCache();

        const transaction = CacheService.db.transaction(["cachedObjects"], "readwrite");
        const store = transaction.objectStore("cachedObjects");
        
        const payload = { data: objectForCache, timestamp: Date.now() };
        const saveRequest = store.put(payload, customStringKey);

        saveRequest.onsuccess = () => console.log(`Successfully cached data under key: "${customStringKey}"`);
        saveRequest.onerror = (event) => console.error("Failed to store data:", event.target.error);
    }


    static async get(customStringKey) {
        return new Promise(async (resolve, reject) => {
            if (!CacheService.db)  await CacheService.createCache();

            const transaction = CacheService.db.transaction(["cachedObjects"], "readonly");
            const store = transaction.objectStore("cachedObjects");
            const getRequest = store.get(customStringKey);

            getRequest.onsuccess = () => {
                if (getRequest.result) {
                    console.log(`Got from cache`);
                    resolve(getRequest.result.data);
                }
                else resolve(null);
            };

            getRequest.onerror = (event) => reject(event.target.error);
        });
    }

    static async hasKey(customStringKey) {
        return new Promise(async (resolve, reject) => {
            if (!CacheService.db) await CacheService.createCache();

            const transaction = CacheService.db.transaction(["cachedObjects"], "readonly");
            const store = transaction.objectStore("cachedObjects");
            
            const checkRequest = store.getKey(customStringKey);
            checkRequest.onsuccess = () => {
                resolve(checkRequest.result !== undefined);
            }
            checkRequest.onerror = (event) => reject(event.target.error);
        });
    }

    static async clearAll() {
        indexedDB.deleteDatabase("LargeDataCache");
    }

}

window.addEventListener('pagehide', () => CacheService.clearAll());
window.addEventListener('load', () => CacheService.clearAll());
