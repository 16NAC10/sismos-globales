# Sismos globales

Sistema para obtener, procesar, almacenar y analizar eventos sísmicos de todo el mundo a partir de APIs públicas (USGS, EMSC, GFZ e INGV). Construye un histórico de los últimos 30 años y procesa los eventos nuevos a medida que se publican.

## Arquitectura

```
USGS · EMSC · GFZ · INGV  (FDSN Event / GeoJSON)
        │
        ▼
Productor Python ──► Kafka (topic sismos.raw)          ingesta incremental y backfill
        │
        ▼
Spark Structured Streaming ──► Bronze (Parquet, JSON original)
        │
        ▼
Spark batch ──► Silver (Parquet particionado por año)  validación, deduplicación,
        │                                              cruce entre fuentes, clasificación
        ▼
Spark batch ──► Gold (Parquet)                         agregados para el análisis
        │
        ▼
MongoDB (colecciones eventos y gold_*) ──► Panel Streamlit + Plotly + pydeck
```

## Requisitos

- Docker Desktop (Kafka, MongoDB y el pipeline en contenedores).
- Para desarrollo local: Python 3.10 o superior y Java 17 o 21.

## Puesta en marcha con Docker

```powershell
docker compose up -d kafka mongo
docker compose build

# 1. Carga histórica de los últimos 30 años (reanudable si se interrumpe)
docker compose run --rm pipeline python -m sismos backfill

# 2. Procesamiento completo: bronze, silver, gold y carga en MongoDB
docker compose run --rm pipeline python -m sismos pipeline

# 3. Servicios continuos: ingesta periódica y panel
docker compose up -d pipeline dashboard
```

El panel queda disponible en <http://localhost:8501>.

El servicio `pipeline` ejecuta `python -m sismos ciclo`: cada 5 minutos consulta las fuentes desde el último checkpoint, publica los eventos nuevos en Kafka y actualiza las capas de Spark y MongoDB.

## Ejecución local (sin Docker)

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e ".[pruebas]"
.venv\Scripts\python -m pytest
```

Las pruebas de parseo y transformación funcionan en Windows. Para ejecutar el pipeline completo en Windows, Spark necesita `winutils.exe` y la variable `HADOOP_HOME` para escribir Parquet; sin ellos falla la escritura. La forma recomendada es usar el contenedor `pipeline`.

## Comandos

| Comando | Función |
|---|---|
| `python -m sismos backfill [--anios N]` | Carga histórica por meses desde las cuatro fuentes. Se reanuda desde su checkpoint. |
| `python -m sismos ingesta` | Publica en Kafka los eventos nuevos desde el último checkpoint. |
| `python -m sismos bronze` | Kafka → Parquet bronze, solo mensajes nuevos. |
| `python -m sismos silver` | Bronze → silver con Spark. Recalcula la capa completa. |
| `python -m sismos gold` | Silver → agregados de análisis. |
| `python -m sismos mongo` | Silver y gold → MongoDB. |
| `python -m sismos pipeline` | Ejecuta bronze, silver, gold y carga en MongoDB. |
| `python -m sismos ciclo [--intervalo N]` | Ingesta y pipeline de forma periódica. |
| `python -m sismos dashboard` | Abre el panel en local. |

## Configuración

- `config/configuracion.yaml`: magnitud mínima (2.5), años de histórico (30), intervalo de sondeo, umbrales de cruce entre fuentes, umbral de evento relevante (M ≥ 6), rutas.
- `config/fuentes.yaml`: URLs y prioridad de cada fuente. Menor número = preferente cuando un mismo sismo aparece en varias.
- `config/regiones.yaml`: regiones sísmicas como cajas geográficas, evaluadas en orden.
- Variables de entorno: `SISMOS_KAFKA_BOOTSTRAP`, `SISMOS_MONGO_URI`, `SISMOS_SPARK_MEMORIA`, `SISMOS_SPARK_MASTER`.

## Modelo de datos

Cada mensaje de Kafka tiene la clave `fuente:id_fuente` y contiene: `fuente`, `id_fuente`, `tiempo_utc`, `latitud`, `longitud`, `profundidad_km`, `magnitud`, `tipo_magnitud`, `autor_magnitud`, `ubicacion`, `tipo_evento`, `nivel_alerta` (solo USGS), `tsunami`, `significancia`, `estado`, `actualizado_utc` y `url`.

La capa silver añade:

- `evento_id`, `fuentes` (fuentes que describen el sismo) y `n_fuentes`.
- `anio` y `mes` (partición y periodo).
- `categoria_magnitud`: Menor (<3), Leve (3–4.9), Moderado (5–5.9), Fuerte (6–6.9), Mayor (≥7).
- `clase_profundidad`: Superficial (≤70 km), Intermedia (70–300 km), Profunda (>300 km), Desconocida.
- `region` y `zona`. La zona es el texto tras la última coma de `ubicacion`.
- `supera_umbral` (M ≥ 6).

### Reglas de calidad

- **Rechazados** (`data/lake/rechazados`, con motivo): sin identificador, fecha no válida, latitud o longitud fuera de rango, magnitud ausente o fuera de rango, evento no sísmico (voladuras, explosiones, etc.).
- **Profundidad** fuera de [-5, 800] km se trata como faltante, sin imputación.
- **Deduplicación dentro de una fuente**: se conserva la versión más reciente de cada evento (campo `updated` de USGS, o última ingesta en el resto).
- **Cruce entre fuentes**: dos registros son el mismo sismo si distan menos de 60 s, menos de 100 km y difieren menos de 1.0 en magnitud. Se conserva el de mayor prioridad. Se usa propagación de etiquetas en 5 rondas.
- **Tipos de magnitud**: los distintos `Mw` (mww, mwp, mwc…) se unifican como `Mw`.

## Capa de análisis (gold)

| Tabla | Contenido |
|---|---|
| `eventos_por_periodo` | Eventos y magnitud media por mes. |
| `distribucion_magnitud` | Eventos por categoría de magnitud. |
| `distribucion_profundidad` | Eventos y magnitud media por clase de profundidad. |
| `actividad_regional` | Eventos, magnitud media y máxima por región. |
| `malla_magnitud_profundidad` | Frecuencias por bins de profundidad (10 km) y magnitud (0.5). |
| `correlacion_magnitud_profundidad` | Coeficiente de Pearson entre magnitud y profundidad. |
| `resumen_fuentes` | Contribución de cada fuente y eventos canónicos. |
| `alertas_recientes` | Eventos de las últimas 24 h con M ≥ 6 o nivel de alerta de USGS. |

## Panel de análisis

Pestañas: Resumen, Mapas (epicentros y mapa de calor), Evolución temporal, Regiones y magnitud, Profundidad (dispersión magnitud-profundidad), Alertas y fuentes.

Filtros: fecha, magnitud, profundidad, región y fuente de datos. Un evento fusionado aparece al seleccionar cualquiera de sus fuentes.

## Cambios respecto al enunciado

| Enunciado | Decisión | Motivo |
|---|---|---|
| HDFS como almacenamiento masivo | Parquet en un data lake local (`data/lake`) | El volumen de catálogo sísmico (millones de filas, no petabytes) no justifica un clúster HDFS. El formato Parquet y la organización bronze/silver/gold se mantienen, y la ruta puede moverse a HDFS o S3 sin cambiar el código de transformación. |
| Power BI | Panel en Streamlit con Plotly y pydeck | Power BI no se conecta de forma nativa a MongoDB, y su versión Pro limita los modelos a 1 GB. Streamlit consulta MongoDB directamente, cubre todos los gráficos pedidos (mapa, mapa de calor, líneas, barras, dispersión) y se despliega en Docker. |
| Kafka | Se mantiene | Desacopla la ingesta del procesamiento. El topic conserva los mensajes (`KAFKA_LOG_RETENTION_MS=-1`) para poder reprocesar bronze. |
| Spark / PySpark | Se mantiene | Structured Streaming con `availableNow` para procesar solo lo nuevo; batch para silver y gold. |
| MongoDB | Se mantiene | Almacena eventos procesados y agregados para consultas rápidas. |
| Fuentes | USGS, EMSC, GFZ e INGV | Todas exponen el servicio FDSN Event, así que se usa un único cliente. USGS usa GeoJSON para obtener nivel de alerta, tsunami y significancia. |
| Solo ventana de 30 años | Configurable (`anios_historico`) | Se puede ampliar sin cambios de código. |

Cambios adicionales que no estaban en el enunciado:

- **Cruce entre fuentes**: el mismo sismo aparece en varias agencias con identificadores distintos. Sin este paso, los conteos y la distribución quedarían inflados.
- **Rechazados auditables**: los registros descartados se guardan con el motivo.
- **Checkpoints**: la ingesta incremental solo avanza el checkpoint después de confirmar el envío a Kafka, para no perder eventos.
- **Carga en MongoDB sin estados parciales**: se escribe en una colección temporal y se renombra al final.
- **Solapamiento de 48 h** en la ingesta incremental para captar revisiones de magnitud o localización.

## Limitaciones conocidas

- **Regiones aproximadas**: se asignan con cajas geográficas. Para un análisis oficial conviene usar Flinn-Engdahl o polígonos de países.
- **Completitud del catálogo**: la detección varía con la región y el tiempo. Las comparaciones históricas de frecuencia deben limitarse a una magnitud mínima de completitud (p. ej. M ≥ 4.5 para comparaciones globales).
- **Magnitudes**: solo se unifican los tipos `Mw`. Otros tipos (`ml`, `mb`, `Ms`) no se convierten entre sí.
- **Propagación del cruce**: es una aproximación. Cadenas largas de registros parecidos podrían no unirse completamente.
- **Silver se recalcula completo** en cada ejecución. Es correcto y simple para este volumen; con más datos convendría una actualización incremental por particiones.
- **Tiempo real**: el análisis de "tiempo real" consiste en ejecuciones periódicas (5 minutos por defecto) sobre los eventos nuevos, no en un flujo de latencia de segundos.

## Estructura

```
config/              configuración (YAML)
src/sismos/
  ingesta/           clientes FDSN/GeoJSON y productor de Kafka
  procesamiento/     bronze (Kafka→Parquet), silver (limpieza y cruce), gold (agregados), sesión Spark
  carga/             carga en MongoDB
  dashboard/         panel Streamlit
  cli.py             orquestación por comandos
tests/               pruebas de parseo, validación, cruce entre fuentes y agregados
docker-compose.yml   Kafka, MongoDB, pipeline y panel
Dockerfile           imagen del pipeline y del panel (Python 3.12 + Java 17)
```

## Pruebas

```powershell
.venv\Scripts\python -m pytest
```

Cubren: parseo de los cuatro formatos de fuente, cálculo de ventanas mensuales, validación de registros, deduplicación, cruce entre fuentes, clasificación por región y profundidad, correlación y agregados gold, y conversión de documentos para MongoDB.
