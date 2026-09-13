dev: certs
	@echo "######### Compiling Development Configuration (Port: 8443, Local Certs)..."
	@sed -i.bak -E "s#^[^|]*\|\|#    loadEnv = () => this.setConfigFile('dev-https') \|\|#" ./ui/config/app-setup.js && rm -f ./ui/config/app-setup.js.bak
	@sed -e 's|{{PORT}}|8443|g' \
	     -e 's|{{CERT}}|./dev-certs/devcert.crt|g' \
	     -e 's|{{KEY}}|./dev-certs/devcert.key|g' \
		 -e 's|{{HOST_IP}}|127.0.0.1|g' \
		 -e 's|{{PORT1}}|8111|g' \
		 -e 's|{{PORT2}}|8112|g' \
	     nginx.conf.template > nginx.d.conf
	@echo ">>>>>>>>> Starting Symmetrical Local Development Cluster..."
	@nginx -c $$(pwd)/nginx.d.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping cluster..."; kill 0 2>/dev/null; nginx -c $$(pwd)/nginx.d.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT; \
	PYTHONUNBUFFERED=1 PORT=8111 python backend/src/app.py 2>&1 | awk '{print " [34m[DEV-NODE-8001] [0m " $$0}' & \
	if false; then \
		PYTHONUNBUFFERED=1 PORT=8112 python backend/src/app.py --no-reload 2>&1 | awk '{print " [32m[DEV-NODE-8002] [0m " $$0}' & \
	fi; \
	wait