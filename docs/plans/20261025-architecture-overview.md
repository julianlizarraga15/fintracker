# Plan para explicar la arquitectura de Fintracker

Estado: primera versión creada en [Arquitectura de Fintracker](../architecture-overview.md). El objetivo es que puedas ubicar cada componente y seguir una consulta y una valuación nueva. La explicación describe la aplicación actual y usa nombres del código solo para señalar los puntos importantes.

## Entrega de la versión 1

Preparar una página en español, de unas 500 palabras, con este orden:

1. Un párrafo que ubique la web y los cálculos en EC2, y los lectores de brokers en Lambda.
2. Un diagrama con ocho bloques principales y un recuadro que delimite la EC2 compartida.
3. Una tabla breve de responsabilidades, lugar de ejecución, entradas y salidas.
4. Dos recorridos concretos, uno para consultar datos guardados y otro para generar una valuación.

El diagrama tendrá estos bloques: navegador, Nginx compartido, frontend, backend con proceso de valuación, datos locales, watering-system, Lambdas lectoras y proveedores externos. Mostrar el timer como disparador del backend. Agregar SSM y S3 como servicios laterales, para no convertir cada archivo o función en una caja.

## Contenido de la tabla

Usar las siguientes filas como base. En la página final, mantener cada celda en una frase corta.

| Componente | Dónde se ejecuta | Responsabilidad | Entrada → salida | Referencia que vale la pena nombrar |
|---|---|---|---|---|
| Frontend | Archivos estáticos en un contenedor de EC2. JavaScript en el navegador. | Mostrar cartera, historial y acciones. | Acciones del usuario y JSON → solicitudes HTTP y pantalla. | `frontend/index.html`: `loadSnapshot()`. `frontend/jobs.html`: `triggerValuationJob()`. |
| Nginx compartido | Contenedor de la EC2. | Dirigir HTTPS según el dominio y la ruta. | Solicitud web → frontend, API o watering. | `nginx.conf`. |
| API de Fintracker | Contenedor backend de EC2, con Python y FastAPI. | Autenticar, leer datos, guardar posiciones manuales e iniciar trabajos. | Solicitud autenticada → JSON o identificador del trabajo. | `backend/app/main.py` y `job_trigger.start_valuation_job()`. |
| Proceso de valuación | Proceso en EC2, iniciado manualmente desde Jobs o por el timer. | Reunir fuentes, incorporar posiciones manuales y calcular resultados. | Disparador, posiciones y precios → snapshots y estado del trabajo. | `backend/core/daily_snapshot.py`: `main()`. |
| Lectores de brokers | Tres funciones AWS Lambda. | Autenticarse y consultar datos de cada broker. | Invocación fija → posiciones y precios para IOL/PPI, balances para Binance. | `backend/core/broker_lambda.py` y los `handler()` de `backend/workers/iol.py` y `ppi.py`. |
| Persistencia | Directorio de EC2 montado en `/app/data`. Snapshots adicionales en S3. | Conservar posiciones manuales, historial y resultados. | Archivos nuevos o actualizados → datos para próximas consultas. | `docker-compose.isolated.yml` y `backend/core/storage.py`. |
| Configuración y credenciales | AWS Systems Manager Parameter Store y configuración de los lectores. | Entregar a cada componente solo su configuración autorizada. | Identidad y nombres de parámetros → configuración permitida. | `backend/core/settings.py`: `load_settings()`. |
| Watering-system | Sus propios contenedores en la misma EC2. | Atender su aplicación mediante el proxy compartido. | Tráfico de su dominio → respuestas de watering. | Ruta de watering en `nginx.conf`. |

## Dos recorridos para explicar

1. **Ver la cartera o tocar Refresh.** El navegador pide el último snapshot mediante `GET /api/valuations/latest`. Nginx dirige la solicitud a la API. La API lee el snapshot persistido y devuelve JSON. `loadSnapshot()` dibuja la pantalla. Esta acción recarga los datos guardados.
2. **Generar una valuación nueva.** El botón de ejecución de Jobs llama `POST /api/jobs/valuations/run`. `start_valuation_job()` inicia el proceso. El timer también dispara el circuito de valuación. `daily_snapshot.main()` consulta lectores, fuentes públicas y posiciones manuales, calcula y guarda resultados. La pantalla Jobs consulta el estado y el historial. Un bloqueo compartido evita trabajos simultáneos.

La primera versión debe dejar explícito que **Refresh y ejecutar una valuación son acciones distintas**. Esa diferencia está confirmada en los dos archivos HTML y en los routers de la API.

## Aclaraciones que deben quedar visibles

- La EC2 lee configuración web aprobada bajo `/fintracker/web` e invoca los tres lectores. Los permisos aplicados impiden recuperar credenciales de workers o modificar sus funciones.
- IOL y PPI leen sus propios parámetros bajo `/fintracker/workers/...`. Binance conserva su función independiente. Evitar atribuirle los parámetros de IOL/PPI.
- Las posiciones manuales y el historial de trabajos persisten localmente. La subida de snapshots a S3 no implica que todos esos archivos estén respaldados allí.
- Fintracker y watering comparten servidor, recursos y Nginx. El despliegue selecciona contenedores de Fintracker, pero valida y recarga el proxy compartido. Ese límite debe verse en el dibujo.

## Criterio de cierre y expansión

La versión 1 está lista cuando permite responder dónde corre cada bloque, quién inicia una valuación, qué datos intercambia y dónde queda el resultado. Comprobar cada flecha contra los archivos citados y [el estado verificado del proyecto](../../PROJECT_STATUS.md).

Dejar para una ampliación posterior los detalles de clases, esquemas, permisos IAM, errores, reintentos y pasos internos de despliegue. La limpieza de imágenes y del Compose anterior queda fuera de esta explicación. Después de leer la versión 1, elegir qué bloque necesita más detalle antes de ampliar el documento.

Este plan se basa en el código y en la última verificación registrada. Crear la explicación no requiere cambios de aplicación, pruebas de ejecución ni acceso nuevo a AWS.
