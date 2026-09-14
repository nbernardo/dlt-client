ENV ?= dev

set-env:
	@echo "######### Setting UI environment to '$(ENV)' and IP to '$(IP_ADDR)'..."
	@sed -i.bak -E 's#("baseUrl": "https://)[^/]+(:9443/api")#\1$(IP_ADDR)\2#g' ./ui/config/settings/stage.json && rm -f ./ui/config/settings/stage.json.bak
	@sed -i.bak -E 's#("websocketAddr": "wss://)[^/]+(:9443/pipeline")#\1$(IP_ADDR)\2#g' ./ui/config/settings/stage.json && rm -f ./ui/config/settings/stage.json.bak
	@sed -i.bak -E 's#("baseUrl": "https://)[^/]+(:443/api")#\1$(IP_ADDR)\2#g' ./ui/config/settings/prod.json && rm -f ./ui/config/settings/prod.json.bak
	@sed -i.bak -E 's#("websocketAddr": "wss://)[^/]+(:443/pipeline")#\1$(IP_ADDR)\2#g' ./ui/config/settings/prod.json && rm -f ./ui/config/settings/prod.json.bak


set-dev: certs
	@echo "######### Compiling Development Configuration (Port: 8443, Local Certs)..."
	@sed -i.bak -E "s#^[^|]*\|\|#    loadEnv = () => this.setConfigFile('dev') \|\|#" ./ui/config/app-setup.js && rm -f ./ui/config/app-setup.js.bak