# Default Production Certificate Settings (can be overridden from CLI or environment)
PROD_DOMAIN    ?= example.com
PROD_CERT_PATH ?= /etc/letsencrypt/live/$(PROD_DOMAIN)/fullchain.pem
PROD_KEY_PATH  ?= /etc/letsencrypt/live/$(PROD_DOMAIN)/privkey.pem

# .PHONY avoids a name clash with the prod-certs/ dir this target creates —
# without it, make would treat that dir as the target's output and skip
# the recipe on later runs.
.PHONY: prod-certs
prod-certs:
	@mkdir -p prod-certs logs
	@if [ ! -f "$(PROD_CERT_PATH)" ] && [ ! -f prod-certs/prod.crt ]; then \
		echo "🔑 No certificate found at $(PROD_CERT_PATH) — generating self-signed cert for $(IP_ADDR)..."; \
		openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
			-keyout prod-certs/prod.key \
			-out prod-certs/prod.crt \
			-subj "/CN=$(IP_ADDR)" \
			-addext "subjectAltName=IP:$(IP_ADDR)"; \
	fi
	$(eval PROD_CERT_PATH := $(shell [ -f "$(PROD_CERT_PATH)" ] && echo "$(PROD_CERT_PATH)" || echo "prod-certs/prod.crt"))
	$(eval PROD_KEY_PATH  := $(shell [ -f "$(PROD_KEY_PATH)" ] && echo "$(PROD_KEY_PATH)" || echo "prod-certs/prod.key"))

prod: prod-certs
	@$(MAKE) set-env ENV=prod
	@echo "######### Compiling Production Configuration (Port: 443, Cert: $(PROD_CERT_PATH))..."
	@sed -i.bak -E 's|{{prd_machine_ip}}|https://$(IP_ADDR):9443|g' ./backend/src/.env && rm -f ./backend/src/.env.bak
	@sed -e 's|{{PORT}}|443|g' \
	     -e 's|{{CERT}}|$(PROD_CERT_PATH)|g' \
	     -e 's|{{KEY}}|$(PROD_KEY_PATH)|g' \
	     -e 's|{{HOST_IP}}|$(IP_ADDR)|g' \
		 -e 's|{{PORT1}}|8001|g' \
		 -e 's|{{PORT2}}|8002|g' \
		 -e 's|{{PORT2}}|8003|g' \
		 -e 's|{{PORT2}}|8004|g' \
	     nginx.conf.template.prod > nginx.conf
	@echo ">>>>>>>>> Starting Symmetrical Production Server Ingestion Engine..."
	@sudo nginx -c $$(pwd)/nginx.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping Production Server..."; kill 0 2>/dev/null; sudo nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT; \
	PYTHONUNBUFFERED=1 PORT=8001 python backend/src/app.py 2>&1 | awk '{print " \033[34m[PROD-NODE-8001]\033[0m " $$0}' & \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8002 python backend/src/app.py --no-reload 2>&1 | awk '{print " \033[32m[PROD-NODE-8002]\033[0m " $$0}' & \
	fi; \
	if false; then \
		PYTHONUNBUFFERED=1 PORT=8003 python backend/src/app.py --no-reload 2>&1 | awk '{print " \033[30m[PROD-NODE-8003]\033[0m " $$0}' & \
	fi; \
	if false; then \
		PYTHONUNBUFFERED=1 PORT=8004 python backend/src/app.py --no-reload 2>&1 | awk '{print " \033[35m[PROD-NODE-8004]\033[0m " $$0}' & \
	fi; \
	wait