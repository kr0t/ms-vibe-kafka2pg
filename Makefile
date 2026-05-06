BROKERS ?= 127.0.0.1:9092
TOPICS ?= topic-1,topic-2,topic-3,topic-4
COUNT ?= 20
DELAY_MS ?= 100
KEY_PREFIX ?= tech-key
SOURCE_NAME ?= make-generator
QUERY_TYPE ?= LOAD
STATUS ?= NEW

.PHONY: help generate-message generate-messages generate-messages-fast

help:
	@echo "Available targets:"
	@echo "  make generate-message       - send one test message through kcat"
	@echo "  make generate-messages      - send COUNT messages with DELAY_MS pause"
	@echo "  make generate-messages-fast - send COUNT messages without pause"
	@echo ""
	@echo "Variables:"
	@echo "  BROKERS=$(BROKERS)"
	@echo "  TOPICS=$(TOPICS)"
	@echo "  COUNT=$(COUNT)"
	@echo "  DELAY_MS=$(DELAY_MS)"
	@echo "  KEY_PREFIX=$(KEY_PREFIX)"
	@echo "  SOURCE_NAME=$(SOURCE_NAME)"
	@echo "  QUERY_TYPE=$(QUERY_TYPE)"
	@echo "  STATUS=$(STATUS)"

generate-message:
	@BROKERS="$(BROKERS)" \
	TOPICS="$(TOPICS)" \
	COUNT=1 \
	DELAY_MS=0 \
	KEY_PREFIX="$(KEY_PREFIX)" \
	SOURCE_NAME="$(SOURCE_NAME)" \
	QUERY_TYPE="$(QUERY_TYPE)" \
	STATUS="$(STATUS)" \
	./scripts/generate_messages.sh

generate-messages:
	@BROKERS="$(BROKERS)" \
	TOPICS="$(TOPICS)" \
	COUNT="$(COUNT)" \
	DELAY_MS="$(DELAY_MS)" \
	KEY_PREFIX="$(KEY_PREFIX)" \
	SOURCE_NAME="$(SOURCE_NAME)" \
	QUERY_TYPE="$(QUERY_TYPE)" \
	STATUS="$(STATUS)" \
	./scripts/generate_messages.sh

generate-messages-fast:
	@BROKERS="$(BROKERS)" \
	TOPICS="$(TOPICS)" \
	COUNT="$(COUNT)" \
	DELAY_MS=0 \
	KEY_PREFIX="$(KEY_PREFIX)" \
	SOURCE_NAME="$(SOURCE_NAME)" \
	QUERY_TYPE="$(QUERY_TYPE)" \
	STATUS="$(STATUS)" \
	./scripts/generate_messages.sh
