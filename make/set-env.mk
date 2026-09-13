ENV ?= dev

set-env:
	@echo "🔧 Setting UI environment to '$(ENV)' in app-setup.js..."
	@sed -i.bak -E "s#^[^|]*\|\|#    loadEnv = () => this.setConfigFile('$(ENV)') \|\|#" ./ui/config/app-setup.js && rm -f ./ui/config/app-setup.js.bak
	@sed -i.bak -E 's|("baseUrl": "https://)[^:]*(:8443/api")|\1$(IP_ADDR)\2|g' ./ui/config/settings/stage.json && rm -f ./ui/config/settings/stage.json.bak
	@sed -i.bak -E 's|("websocketAddr": "wss://)[^:]*(:8443/pipeline")|\1$(IP_ADDR)\2|g' ./ui/config/settings/stage.json && rm -f ./ui/config/settings/stage.json.bak
	@sed -i.bak -E 's|("baseUrl": "https://)[^:]*(:443/api")|\1$(IP_ADDR)\2|g' ./ui/config/settings/default.json && rm -f ./ui/config/settings/default.json.bak
	@sed -i.bak -E 's|("websocketAddr": "wss://)[^:]*(:443/pipeline")|\1$(IP_ADDR)\2|g' ./ui/config/settings/default.json && rm -f ./ui/config/settings/default.json.bak


set-dev: certs
	@echo "🛠️ Compiling Development Configuration (Port: 8443, Local Certs)..."
	@sed -i.bak -E "s#^[^|]*\|\|#    loadEnv = () => this.setConfigFile('dev') \|\|#" ./ui/config/app-setup.js && rm -f ./ui/config/app-setup.js.bak