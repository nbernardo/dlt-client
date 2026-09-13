UV := $(shell command -v uv 2> /dev/null)
OS := $(shell uname -s 2>/dev/null || echo Windows)
IP_ADDR := $(shell \
    if [ "$(OS)" = "Darwin" ]; then \
        ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null || echo "127.0.0.1"; \
    else \
        hostname -I 2>/dev/null | awk '{print $$1}' || ip route get 1.1.1.1 2>/dev/null | awk '{print $$7}' || echo "127.0.0.1"; \
    fi \
)

include make/set-env.mk

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
include make/dev.mk
include make/stage.mk
include make/prod.mk


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

# ====================================================================
# DIAGNOSTIC TARGETS
# ====================================================================

check-ip:
	@echo "OS Detected : $(OS)"
	@echo "Resolved IP : $(IP_ADDR)"

.PHONY: install install-deps extensions dev stage prod clean certs