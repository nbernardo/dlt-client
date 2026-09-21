PYTHON ?= $(shell command -v python3 2>/dev/null || command -v python 2>/dev/null)

stage: certs
	@$(MAKE) set-env ENV=stage
	@echo "######### Compiling Stage Configuration (Port: 9443, IP: $(IP_ADDR))..."
	@sed -i.bak -E "s#^[^|]*\|\|#    loadEnv = () => this.setConfigFile('stage') \|\|#" ./ui/config/app-setup.js && rm -f ./ui/config/app-setup.js.bak
	@sed -i.bak 's|{{machine_ip}}|https://$(IP_ADDR):9443|g' ./backend/src/.env && rm -f ./backend/src/.env.bak
	@sed -e 's|{{PORT}}|9443|g' \
	     -e 's|{{CERT}}|./dev-certs/devcert.crt|g' \
	     -e 's|{{KEY}}|./dev-certs/devcert.key|g' \
		 -e 's|{{HOST_IP}}|$(IP_ADDR)|g' \
		 -e 's|{{PORT1}}|8221|g' \
		 -e 's|{{PORT2}}|8222|g' \
	     nginx.conf.template > nginx.conf
	@echo ">>>>>>>>> Starting Symmetrical Local Development Cluster..."
	@nginx -c $$(pwd)/nginx.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping cluster..."; kill 0 2>/dev/null; nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT HUP; \
	PYTHONUNBUFFERED=1 PORT=8221 $(PYTHON) backend/src/app.py 2>&1 | awk '{print " [34m[DEV-NODE-8001] [0m " $$0}' & \
	if false; then \
		PYTHONUNBUFFERED=1 PORT=8222 $(PYTHON) backend/src/app.py --no-reload 2>&1 | awk '{print " [32m[DEV-NODE-8002] [0m " $$0}' & \
	fi; \
	wait