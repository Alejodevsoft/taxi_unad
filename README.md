# Tarea 3 · Big Data · UNAD

Aplicación académica local para analizar taxis amarillos de Nueva York en enero de 2025. Python y Spark procesan el Parquet completo; Kafka reproduce una muestra histórica como **simulación** y Spark Structured Streaming calcula ventanas por zona. El panel presenta ambos componentes.

**Para preparar la entrega de Alejandro:** abrir [la guía personal en HTML](../entrega_individual/GUIA_DE_ALEJANDRO.html) directamente en el navegador o leer [el paso a paso](../entrega_individual/01_PASO_A_PASO.md). Incluye guiones completos de los dos videos, aporte para el informe y borradores de foro y presentación. La guía es un archivo local y no requiere servidor.

**Documentación técnica:** [metodología](../docs/METODOLOGIA.md), [resultados de la ejecución](../docs/RESULTADOS.md), [guion batch](../entrega_individual/02_VIDEO_BATCH.md) y [guion streaming](../entrega_individual/03_VIDEO_STREAMING.md).

## Entorno reproducible

Usar Docker Desktop con motor Linux y Docker Compose. Se recomienda asignar al motor al menos 6 GB de RAM y disponer de unos 5 GB libres en disco para imágenes, datos y resultados. La primera construcción requiere Internet para imágenes, PyPI y Maven; las siguientes ejecuciones reutilizan lo descargado. No requiere instalar Python o Java en Windows.

| Componente | Versión fijada |
|---|---|
| Python | 3.11.11, imagen Debian Bookworm |
| Java | Eclipse Temurin JRE 17.0.12+7 |
| PySpark / Spark | 3.5.6 |
| Scala del conector | 2.12 |
| Broker Kafka | 3.9.1, KRaft |
| Conector Spark–Kafka | spark-sql-kafka-0-10_2.12:3.5.6 |
| Cliente Java del conector | kafka-clients:3.4.1 |
| Productor Python | kafka-python:2.2.15 |

El cliente Kafka del conector no necesita coincidir con la versión del broker. Las dependencias Python directas están en `requirements.txt`; el bloqueo completo instalado se conserva en `requirements.lock.txt`. Los JAR se descargan por coordenadas fijadas en `fetch_jars.py`, con manifiesto SHA-256. El módulo de Spark utiliza dos hilos locales, cuatro particiones de shuffle y 2 GB para el driver.

Abrir PowerShell **en la carpeta `taxi_unad`**. Primero iniciar Docker Desktop y esperar que el motor Linux esté listo.

```powershell
docker compose build
docker compose up -d app
docker compose run --rm --no-deps app python src/topic.py
```

El panel estará en http://localhost:8000. Los puertos del host se enlazan a loopback. Kafka escucha en localhost:9092 para clientes del host y en kafka:9092 para el contenedor app. Es una configuración didáctica local sin autenticación, un broker, tres particiones y replicación uno.

## Batch completo

```powershell
docker compose run --rm --no-deps app python src/batch.py
```

La descarga inicial conserva `data/raw/yellow_tripdata_2025-01.parquet`, el catálogo y el manifiesto. El diccionario se consulta en su URL oficial, documentada en la metodología; su descarga automatizada respondió HTTP 403 y no es una dependencia de ejecución. Si los datos ya existen, verifica sus huellas y los reutiliza. El procesamiento sobrescribe las salidas derivadas; no modifica originales. Los resultados aparecen en la pestaña **Análisis batch** del panel.

Para guardar también la salida de consola de una nueva ejecución:

```powershell
docker compose run --rm --no-deps app python src/batch.py 2>&1 | Tee-Object -FilePath evidence/batch_console.log
```

## Streaming para demostrar en video

El batch debe haberse completado para disponer de `data/replay.jsonl`. Abrir el panel y seleccionar **Streaming · simulación**. En una primera terminal:

```powershell
docker compose run --rm --no-deps --name unad-taxi-stream app python src/streaming.py
```

En otra terminal:

```powershell
docker compose run --rm --no-deps app python src/producer.py --count 300 --rate 3
```

`--rate` indica eventos por segundo y `--count` limita la emisión. Cada ejecución crea nuevos IDs de evento. Esperar entre cinco y quince segundos para ver los primeros lotes. El gráfico y la tabla por zona cambian al llegar eventos. Para detener anticipadamente **ese productor**, usar Ctrl+C en su terminal; esperar a que Spark procese lo pendiente. El panel mostrará el tiempo sin nuevos mensajes y los conteos estables. Repetir el comando permite observar el reinicio de la emisión.

Un mensaje inválido controlado:

```powershell
docker compose run --rm --no-deps app python src/producer.py --count 5 --rate 2 --invalid
```

`--invalid` añade un mensaje incompleto con duración negativa. Aumenta el contador de inválidos y queda guardado con motivos; no entra en las ventanas. `--late` envía el último evento con cinco minutos de retraso para explorar la marca de agua; es una prueba, no una observación del mundo real.

Para detener el consumidor desde otra terminal y permitirle guardar su estado:

```powershell
docker stop --time 60 unad-taxi-stream
```

Volver a ejecutar el mismo comando de streaming reutiliza el checkpoint. No iniciar dos consumidores de este proyecto que compartan los mismos checkpoints y SQLite. `startingOffsets=earliest` se aplica al primer arranque; con checkpoint se reanuda desde sus offsets guardados.

## Validación automática

Detener primero el consumidor manual y cualquier productor. Con Kafka y el panel activos:

```powershell
docker compose run --rm --no-deps app python -m unittest discover -s tests -v
docker compose run --rm --no-deps app python src/validate_stream.py
```

La prueba integral tarda varios minutos: envía 150 eventos válidos y uno inválido, verifica el panel, pausa, detiene Spark limpiamente, lo reinicia con su checkpoint y envía 40 más. Los datos de prueba son emisiones simuladas de viajes históricos. No borra estado ni mensajes existentes; registra una línea base y verifica los incrementos. No ejecutar productores simultáneos durante esta validación. Consultar `evidence/stream_validation.json`, snapshots, ciclos de consulta y logs. Las evidencias entregadas corresponden a la ejecución efectivamente realizada, según [los resultados documentados](../docs/RESULTADOS.md).

## Archivos y salidas

| Archivo o directorio | Papel |
|---|---|
| `Dockerfile`, `compose.yaml` | Entorno fijado; broker, panel y volúmenes persistentes |
| `requirements*.txt`, `src/fetch_jars.py` | Dependencias Python y conector Java |
| `src/common.py` | Rutas, sesión Spark y escritura atómica de JSON |
| `src/download.py` | Descarga, reutilización y trazabilidad de originales |
| `src/batch.py` | Diagnóstico, reglas, deduplicación, análisis y cuatro gráficos |
| `src/topic.py` | Creación y comprobación del topic |
| `src/producer.py` | Reproducción histórica JSON a velocidad configurable |
| `src/streaming.py` | Validación, dos consultas Kafka, ventanas, marca de agua y checkpoints |
| `src/store.py` | SQLite, transacciones e idempotencia de destino |
| `src/dashboard.py`, `src/dashboard.html` | Panel local, API agregada y actualización cada dos segundos |
| `src/validate_stream.py`, `tests/test_rules.py` | Prueba integral real y casos de calidad |
| `data/raw/` | Originales intactos y manifiesto |
| `data/clean/` | Cohorte aceptada en Parquet |
| `data/rejected/` | Originales representativos, multiplicidad y motivos |
| `data/replay.jsonl` | Hasta 10000 viajes válidos para el productor |
| `data/checkpoints/`, `data/stream.sqlite` | Estado persistente de consultas y visualización |
| `outputs/` | Fuentes, esquema, calidad, rangos y tablas CSV/JSON |
| `evidence/` | Evidencias de las pruebas ejecutadas |
| `../docs/` | Metodología y resultados verificados |
| `../entrega_individual/` | Guía local, guiones completos y textos para la entrega de Alejandro |

Los CSV/JSON agregados son pequeños y consultables sin Spark. Los originales y resultados Parquet se leen con Spark; los rechazos se pueden inspeccionar con `spark.read.parquet('data/rejected').show(truncate=False)` dentro del entorno.

Los PNG y las capturas existentes se retiraron de las carpetas durante la limpieza y se conservaron en [el respaldo ZIP](../Respaldo_limpieza_2026-10-25.zip). El dashboard dibuja sus gráficos desde las tablas JSON y funciona sin esos archivos de imagen. Una nueva ejecución del batch vuelve a exportar los cuatro PNG.

Estado de la entrega: el batch completo y la prueba integral de streaming pasaron. Kafka y el panel quedaron activos; el consumidor utilizado por la prueba se detuvo limpiamente y conserva sus checkpoints. Para una nueva demostración, iniciar el consumidor manual y luego el productor con los comandos anteriores.

## Cierre y conservación

```powershell
docker compose down
```

Esto detiene servicios y conserva el volumen Kafka, datos locales, SQLite y checkpoints. No eliminar por separado estos estados si se quiere reanudar la demostración. Un reinicio desde cero debe realizarse en **otra copia de la carpeta** con otro nombre de proyecto Compose y datos nuevos, conservando esta entrega y sus evidencias. No usar `down -v` para una pausa normal.

No se ha publicado el repositorio. El Anexo 3 queda pendiente de cotejo; no se atribuye a esta implementación el cumplimiento de un instructivo que no estaba disponible.
