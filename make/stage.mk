stage: certs
	@$(MAKE) set-env ENV=stage
	@echo "🛠️ Compiling Stage Configuration (Port: 8443, IP: $(IP_ADDR))..."
	@sed -e 's|{{PORT}}|8443|g' \
	     -e 's|{{CERT}}|./dev-certs/devcert.crt|g' \
	     -e 's|{{KEY}}|./dev-certs/devcert.key|g' \
		 -e 's|{{HOST_IP}}|$(IP_ADDR)|g' \
	     nginx.conf.template > nginx.conf
	@echo "🚀 Starting Symmetrical Local Development Cluster..."
	@nginx -c $$(pwd)/nginx.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping cluster..."; kill 0 2>/dev/null; nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT; \
	PYTHONUNBUFFERED=1 PORT=8001 python backend/src/app.py 2>&1 | awk '{print " [34m[DEV-NODE-8001] [0m " $$0}' & \
	if false; then \
		PYTHONUNBUFFERED=1 PORT=8002 python backend/src/app.py --no-reload 2>&1 | awk '{print " [32m[DEV-NODE-8002] [0m " $$0}' & \
	fi; \
	wait