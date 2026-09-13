# Default Production Certificate Settings (can be overridden from CLI or environment)
PROD_DOMAIN    ?= example.com
PROD_CERT_PATH ?= /etc/letsencrypt/live/$(PROD_DOMAIN)/fullchain.pem
PROD_KEY_PATH  ?= /etc/letsencrypt/live/$(PROD_DOMAIN)/privkey.pem

prod: certs
	@$(MAKE) set-env ENV=prod
	@echo "🏗️ Compiling Production Configuration (Port: 443, Cert: $(PROD_CERT_PATH))..."
	@sed -e 's|{{PORT}}|443|g' \
	     -e 's|{{CERT}}|$(PROD_CERT_PATH)|g' \
	     -e 's|{{KEY}}|$(PROD_KEY_PATH)|g' \
	     -e 's|{{HOST_IP}}|$(IP_ADDR)|g' \
	     nginx.conf.template > nginx.conf
	@echo "🚀 Starting Symmetrical Production Server Ingestion Engine..."
	@sudo nginx -c $$(pwd)/nginx.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping Production Server..."; kill 0 2>/dev/null; sudo nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT; \
	PYTHONUNBUFFERED=1 PORT=8001 python backend/src/app.py 2>&1 | awk '{print " \033[34m[PROD-NODE-8001]\033[0m " $$0}' & \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8002 python backend/src/app.py --no-reload 2>&1 | awk '{print " \033[32m[PROD-NODE-8002]\033[0m " $$0}' & \
	fi; \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8003 python backend/src/app.py --no-reload 2>&1 | awk '{print " \033[30m[PROD-NODE-8003]\033[0m " $$0}' & \
	fi; \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8004 python backend/src/app.py --no-reload 2>&1 | awk '{print " \033[35m[PROD-NODE-8004]\033[0m " $$0}' & \
	fi; \
	wait