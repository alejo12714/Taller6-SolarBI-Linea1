# SolarBI · automatización del despliegue (README §2, INFORME_TECNICO.md §6.5)
# Uso:  make help
# En Windows se ejecuta desde Git Bash o WSL (el Makefile usa sh, kubectl, docker y k3d).

CLUSTER      ?= solarbi
NS           ?= solarbi
IMG_ETL      ?= solarbi-etl:1.0.0
IMG_SIM      ?= solarbi-simulator:1.0.0
GRAFANA_PORT ?= 3000
EVIDENCIA    ?= docs/evidencia

# Umbrales editables con `make umbral` (literales del ConfigMap solarbi-config)
UMBRALES := ALERTA_SOC_MIN ALERTA_FRESCURA_S ALERTA_PCT_VALIDAS ANOMALIA_PROB FORZAR_ANOMALIA DT_MAX_S

.DEFAULT_GOAL := help
.PHONY: help up cluster imagenes secret deploy esperar etl-rerun estado logs-etl \
        demo-pg demo-grafana umbral evidencia down

help: ## Muestra esta ayuda
	@echo "SolarBI · objetivos disponibles:"
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-14s %s\n", $$1, $$2}'
	@echo ""
	@echo "Ejemplo de umbral:  make umbral VAR=ALERTA_SOC_MIN VAL=30"

up: verificar-entorno cluster imagenes secret deploy esperar ## Clúster + imágenes + Secret + manifiestos + espera del ETL
	@echo ""
	@echo "SolarBI listo → Grafana http://localhost:$(GRAFANA_PORT) · PostgreSQL localhost:$${PG_PORT:-5432} (base solarbi)"

.PHONY: verificar-entorno
verificar-entorno:
	@for c in docker k3d kubectl; do command -v $$c >/dev/null 2>&1 || { echo "ERROR: falta '$$c' en el PATH"; exit 1; }; done
	@[ -f .env ] || { echo "ERROR: falta .env  →  cp .env.example .env  y cambia las contraseñas"; exit 1; }
	@grep -q 'cambiar-' .env && echo "AVISO: .env todavía tiene contraseñas de ejemplo ('cambiar-...')." || true

cluster: ## Crea el clúster k3d (idempotente)
	CLUSTER=$(CLUSTER) GRAFANA_PORT=$(GRAFANA_PORT) ./scripts/crear_cluster_k3d.sh

imagenes: ## Construye las imágenes del ETL y del simulador y las importa a k3d
	docker build -f etl/Dockerfile -t $(IMG_ETL) .
	docker build -f simulator/Dockerfile -t $(IMG_SIM) .
	k3d image import $(IMG_ETL) $(IMG_SIM) -c $(CLUSTER)

secret: ## Crea/actualiza el Secret solarbi-secret a partir de .env (nunca se versiona)
	@[ -f .env ] || { echo "ERROR: falta .env"; exit 1; }
	kubectl apply -f k8s/base/00-namespace.yaml
	kubectl -n $(NS) create secret generic solarbi-secret --from-env-file=.env \
	  --dry-run=client -o yaml | kubectl apply -f -

deploy: ## Aplica todos los manifiestos con Kustomize
	kubectl apply -k k8s/base

esperar: ## Espera a PostgreSQL, a Grafana y a que termine el Job ETL
	kubectl -n $(NS) rollout status statefulset/postgres --timeout=300s
	kubectl -n $(NS) rollout status deployment/grafana --timeout=300s
	@echo "Esperando al Job solarbi-etl (carga histórica; puede tardar varios minutos con el dataset completo)..."
	kubectl -n $(NS) wait --for=condition=complete job/solarbi-etl --timeout=7200s
	@kubectl -n $(NS) logs job/solarbi-etl --tail=5

etl-rerun: ## Vuelve a ejecutar el Job ETL (un Job terminado no se puede editar: se recrea)
	kubectl -n $(NS) delete job solarbi-etl --ignore-not-found
	kubectl apply -k k8s/base
	kubectl -n $(NS) wait --for=condition=complete job/solarbi-etl --timeout=7200s
	@kubectl -n $(NS) logs job/solarbi-etl | grep -E "RUN |Idempotencia" || true

estado: ## Resumen de pods, servicios y volúmenes
	kubectl -n $(NS) get pods,svc,pvc,job,cronjob

logs-etl: ## Log del Job ETL
	kubectl -n $(NS) logs job/solarbi-etl

demo-pg: ## Persistencia: cuenta filas → borra postgres-0 → espera → cuenta de nuevo (debe coincidir)
	@SQL="SELECT count(*) FROM gold.fact_telemetria_minuto"; \
	antes=$$(kubectl -n $(NS) exec postgres-0 -- psql -U postgres -d solarbi -tAc "$$SQL"); \
	echo "Filas en gold.fact_telemetria_minuto ANTES : $$antes"; \
	kubectl -n $(NS) delete pod postgres-0 --wait=true; \
	until kubectl -n $(NS) get pod postgres-0 >/dev/null 2>&1; do sleep 1; done; \
	kubectl -n $(NS) wait --for=condition=Ready pod/postgres-0 --timeout=300s; \
	despues=$$(kubectl -n $(NS) exec postgres-0 -- psql -U postgres -d solarbi -tAc "$$SQL"); \
	echo "Filas en gold.fact_telemetria_minuto DESPUÉS: $$despues"; \
	[ "$$antes" = "$$despues" ] && echo "OK: los datos sobrevivieron (PVC)." || { echo "ERROR: los conteos no coinciden"; exit 1; }

demo-grafana: ## Resiliencia: borra el pod de Grafana; vuelve con tablero, fuente de datos y alertas
	kubectl -n $(NS) delete pod -l app=grafana --wait=true
	kubectl -n $(NS) rollout status deployment/grafana --timeout=300s
	@set -a; . ./.env; set +a; \
	for i in 1 2 3 4 5 6 7 8 9 10; do \
	  r=$$(curl -s -u "admin:$$GF_SECURITY_ADMIN_PASSWORD" "http://localhost:$(GRAFANA_PORT)/api/search?query=SolarBI" 2>/dev/null); \
	  [ -n "$$r" ] && [ "$$r" != "[]" ] && { echo "Tablero aprovisionado: $$r"; exit 0; }; sleep 3; \
	done; echo "AVISO: no se pudo confirmar el tablero por la API; revisa http://localhost:$(GRAFANA_PORT)"

umbral: ## Cambia un umbral sin reconstruir imágenes: make umbral VAR=ALERTA_SOC_MIN VAL=30
	@[ -n "$(VAR)" ] && [ -n "$(VAL)" ] || { echo "Uso: make umbral VAR=<nombre> VAL=<valor>   (VAR ∈ $(UMBRALES))"; exit 1; }
	@echo " $(UMBRALES) " | grep -q " $(VAR) " || { echo "VAR inválida. Permitidas: $(UMBRALES)"; exit 1; }
	sed -i.bak -E 's/^([[:space:]]*- $(VAR)=)[^[:space:]#]*/\1$(VAL)/' k8s/base/kustomization.yaml
	@rm -f k8s/base/kustomization.yaml.bak
	@grep -n -- "- $(VAR)=" k8s/base/kustomization.yaml
	kubectl apply -k k8s/base
	kubectl -n $(NS) rollout status deployment/grafana --timeout=300s

evidencia: ## Guarda kubectl get/describe/logs en docs/evidencia/ (para el informe)
	@mkdir -p $(EVIDENCIA)
	kubectl get nodes -o wide                                   > $(EVIDENCIA)/00-nodos.txt
	kubectl -n $(NS) get all,pvc,configmap,secret -o wide       > $(EVIDENCIA)/01-get-all.txt
	kubectl -n $(NS) describe statefulset/postgres              > $(EVIDENCIA)/02-describe-postgres.txt
	kubectl -n $(NS) describe deployment/grafana                > $(EVIDENCIA)/03-describe-grafana.txt
	kubectl -n $(NS) describe job/solarbi-etl                   > $(EVIDENCIA)/04-describe-etl.txt
	kubectl -n $(NS) describe cronjob/solarbi-simulador         > $(EVIDENCIA)/05-describe-simulador.txt
	kubectl -n $(NS) logs job/solarbi-etl                       > $(EVIDENCIA)/06-log-etl.txt
	kubectl -n $(NS) get events --sort-by=.lastTimestamp        > $(EVIDENCIA)/07-eventos.txt
	-docker images "solarbi-*"                                  > $(EVIDENCIA)/08-imagenes.txt
	@echo "Evidencia guardada en $(EVIDENCIA)/ (los Secret se listan por nombre; no se vuelcan sus valores)."

down: ## Elimina el clúster k3d (los datos del PVC se pierden; el Bronze en ./data no se toca)
	k3d cluster delete $(CLUSTER)
