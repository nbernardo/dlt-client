UV := $(shell command -v uv 2> /dev/null)
OS := $(shell uname -s 2>/dev/null || echo Windows)

# ====================================================================
# PHASE 1: INSTALLATION TARGETS
# ====================================================================

install: extensions

install-deps:
ifeq ($(UV),)
	pip install -r backend/requirements.txt
else
	uv pip install -r backend/requirements.txt
endif

extensions: install-deps
	python backend/setup_extensions.py

# ====================================================================
# PHASE 2: CERTIFICATE GENERATION
# ====================================================================

certs:
	@mkdir -p dev-certs logs
	@if [ ! -f dev-certs/devcert.crt ]; then \
		echo "🔐 Generating custom self-signed development keys..."; \
		openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
			-keyout dev-certs/devcert.key \
			-out dev-certs/devcert.crt \
			-subj "/CN=localhost"; \
	fi

# ====================================================================
# PHASE 3: RUNTIME ENGINES (TEMPLATE DRIVEN)
# ====================================================================

dev: certs
	@echo "🛠️ Compiling Development Configuration (Port: 8443, Local Certs)..."
	@sed -e 's|{{PORT}}|8443|g' \
	     -e 's|{{CERT}}|./dev-certs/devcert.crt|g' \
	     -e 's|{{KEY}}|./dev-certs/devcert.key|g' \
	     nginx.conf.template > nginx.conf
	@echo "🚀 Starting Symmetrical Local Development Cluster..."
	@nginx -c $$(pwd)/nginx.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping cluster..."; kill 0 2>/dev/null; nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT; \
	PYTHONUNBUFFERED=1 PORT=8001 python backend/src/app.py 2>&1 | awk '{print " [34m[DEV-NODE-8001] [0m " $$0}' & \
	if false; then \
		PYTHONUNBUFFERED=1 PORT=8002 python backend/src/app.py --no-reload 2>&1 | awk '{print " [32m[DEV-NODE-8002] [0m " $$0}' & \
	fi; \
	wait

prod: certs
	@echo "🏗️ Compiling Production Configuration (Port: 443, System Certificates)..."
	@sed -e 's|{{PORT}}|443|g' \
	     -e 's|{{CERT}}|/etc/letsencrypt/live/://example.com|g' \
	     -e 's|{{KEY}}|/etc/letsencrypt/live/://example.com|g' \
	     nginx.conf.template > nginx.conf
	@echo "🚀 Starting Symmetrical Production Server Ingestion Engine..."
	@sudo nginx -c $$(pwd)/nginx.conf -p $$(pwd); \
	trap 'echo "\n🛑 Stopping Production Server..."; kill 0 2>/dev/null; sudo nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1; exit 0' INT TERM EXIT; \
	PYTHONUNBUFFERED=1 PORT=8001 python backend/src/app.py 2>&1 | awk '{print " [34m[PROD-NODE-8001] [0m " $$0}' & \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8002 python backend/src/app.py --no-reload 2>&1 | awk '{print " [32m[PROD-NODE-8002] [0m " $$0}' & \
	fi; \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8003 python backend/src/app.py --no-reload 2>&1 | awk '{print " [30m[PROD-NODE-8002] [0m " $$0}' & \
	fi; \
	if true; then \
		PYTHONUNBUFFERED=1 PORT=8004 python backend/src/app.py --no-reload 2>&1 | awk '{print " [28m[PROD-NODE-8002] [0m " $$0}' & \
	fi; \
	wait

# ====================================================================
# PHASE 4: CLEANUP ROUTINES
# ====================================================================

clean:
	@echo "🧹 Cleaning up lingering Python processes and NGINX instances..."
	-@pkill -f "backend/src/app.py" || true
	-@nginx -c $$(pwd)/nginx.conf -p $$(pwd) -s stop >/dev/null 2>&1 || true
	-@pkill -f "nginx" || true
	-@sudo pkill -f "nginx" >/dev/null 2>&1 || true
	-@rm -f nginx.conf || true

.PHONY: install install-deps extensions dev prod clean certs
