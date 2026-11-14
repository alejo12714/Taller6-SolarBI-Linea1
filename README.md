# SolarBI · BI contenerizado y orquestado para la microrred híbrida OpenCEM

> *From inverter telemetry to actionable insights: a containerized, Kubernetes-orchestrated BI pipeline for a hybrid microgrid.*

Taller 6 · Inteligencia de Negocios · I.U. Pascual Bravo · 2026-II · **Línea 1: auditoría y calidad de la telemetría**

El diseño completo (KPIs, modelo, ETL, consumo y despliegue) está en **[INFORME_TECNICO.md](INFORME_TECNICO.md)**.

## Arquitectura

```
Zenodo .sqlite.zst ──sha256 + zstd──▶ BRONZE (.sqlite inmutable, hostPath /data, solo lectura)
                                         │  Job solarbi-etl (Python · reglas R01–R10)
                                         ▼
                        SILVER (1 min) ──▶ GOLD (estrella) ──▶ Power BI (anfitrión, NodePort 5432)
                             ▲                    │
     CronJob solarbi-simulador (cada minuto,      └──────────▶ Grafana (Deployment + provisioning, NodePort 3000)
     replay de lecturas crudas con las mismas reglas)
Namespace solarbi · StatefulSet postgres + PVC · Secret · ConfigMaps (Kustomize) · probes · requests/limits
```

## Requisitos del equipo anfitrión

Docker 24+, [k3d](https://k3d.io) 5.6+, kubectl 1.29+ (trae Kustomize), `make` (en Windows: Git Bash o WSL), `sha256sum`, `zstd`. 8 GB de RAM libres y unos 10 GB de disco. Power BI Desktop y DBeaver en el anfitrión.

## 1. Descargar y verificar los datos (Bronze)

Del registro [10.5281/zenodo.21436223](https://doi.org/10.5281/zenodo.21436223) se descargan solo `opencem-v1.0.0.sqlite.zst`, `opencem-v1.0.0-support.zip`, `opencem-v1.0.0-SHA256SUMS.txt`, `README.md` y `LICENSE-DATA.txt` (≈ 310 MB). Las imágenes de cielo no se descargan.

```bash
mkdir -p data && cd data
sha256sum -c --ignore-missing opencem-v1.0.0-SHA256SUMS.txt      # evidencia para el informe
zstd -d opencem-v1.0.0.sqlite.zst -o opencem-v1.0.0.sqlite
sha256sum opencem-v1.0.0.sqlite   # esperado: 5fe1f213a4a6804cdfc75f6173f437772c918f45dc0f127eb7e8936c2c2d9fbb
cd ..
```

`data/` está en `.gitignore`: el Bronze nunca se versiona ni se modifica (el ETL lo abre con `mode=ro&immutable=1`).

## 2. Levantar todo con un comando

```bash
cp .env.example .env       # cambiar las contraseñas
make up                    # k3d + imágenes + Secret + kubectl apply -k + espera al ETL
```

| Servicio | Acceso desde el anfitrión | Usuario |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `GF_SECURITY_ADMIN_PASSWORD` |
| PostgreSQL (DBeaver, Power BI) | `localhost:5432`, base `solarbi` | `solarbi_reader` (lectura) · `solarbi_etl` (dueño) |

```bash
make demo-pg        # borra postgres-0: los datos siguen ahí (PVC)
make demo-grafana   # borra el pod de Grafana: vuelve con tablero y alertas (provisioning)
make umbral VAR=ALERTA_SOC_MIN VAL=30    # cambio de umbral sin reconstruir la imagen
make evidencia      # kubectl get / describe / logs -> docs/evidencia/
make down           # elimina el clúster
```

Desarrollo local sin clúster: `docker compose up -d postgres grafana && docker compose run --rm etl && docker compose --profile stream up -d simulador`.


## Power BI sin terminal (Docker Desktop)

`powerbi-db/initdb/` trae un volcado de Gold (15 MB) que se restaura solo. En Docker Desktop → Images → `postgres:16` → **Run** → Optional settings:
nombre `solarbi-pg`, puerto **5433** (el 5432 lo usa otro PostgreSQL del equipo), volumen `…\powerbi-db\initdb` → `/docker-entrypoint-initdb.d`, variables `POSTGRES_PASSWORD` y `POSTGRES_DB=solarbi`.
Luego abre `powerbi/SolarBI.pbip` → Actualizar (usuario `solarbi_reader`). Los parámetros M `ServidorPG`/`BaseDatosPG` permiten apuntar al clúster (`localhost:5432`) sin tocar las consultas.

## 3. Probar sin descargar los 4,4 GB

```bash
python -I scripts/crear_sqlite_prueba.py <ruta>/documentation/schema.sql data/prueba.sqlite
SQLITE_PATH=data/prueba.sqlite PGHOST=localhost PGDATABASE=solarbi PGUSER=solarbi_etl PGPASSWORD=... \
  REPLAY_DESDE=2026-06-13T00:00:00+08:00 REPLAY_DIAS=2 python etl/etl.py     # 2 veces: misma huella Gold
```

La base de prueba usa el **esquema oficial** con datos **sintéticos**. Sirve para verificar código e idempotencia, no para sacar cifras.

## Estructura

```
sql/            01 esquemas y roles · 02 Silver/Audit · 03 Gold DDL · 04 semillas · 05 vistas
                06 consultas de negocio (ventanas + JSONB) · 07 carga Silver→Gold
common/         solarbi_core: reglas de calidad y agregación (compartido por ETL y simulador)
etl/            etl.py + Dockerfile          simulator/   simulador.py + Dockerfile
k8s/base/       Namespace, StatefulSet, Deployment, Job, CronJob, kustomization, grafana/ (provisioning)
powerbi/        medidas.dax (el proyecto .pbip se versiona aquí)
scripts/        generador del tablero de Grafana, generador de la SQLite de prueba
```

**Power BI en Git:** se usa el formato *Power BI Project* (`.pbip`: modelo en TMDL y reporte en JSON de texto, comparables en un diff). El `.pbix` binario está en `.gitignore`.

## Datos, licencias y cita

Datos bajo **CC BY 4.0**; el código de este repositorio es para uso académico.

Bartels, T. S., Wu, R., Lu, X., Chen, Y., Lu, Y., Xia, F., Yang, H., Chen, Y., & Li, T. (2026). *OpenCEM: Multimodal photovoltaic and battery microgrid dataset with context records and sky imagery* (Version 1.0.0) [Dataset]. Zenodo. https://doi.org/10.5281/zenodo.21436223
