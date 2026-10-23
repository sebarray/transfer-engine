# Fase 6: API autenticada y observabilidad

El servicio expone transferencias internas **simuladas**. No conecta bancos ni
procesadores de pagos. El contrato HTTP está en [openapi.json](openapi.json).

## Activación

La API conserva por defecto sólo `/livez` y `/readyz`. Para habilitar `/v1`,
configurar estas variables del proceso (o `.env` para Docker Compose):

```powershell
$env:FINANCIAL_API_ENABLED = 'true'
$env:JWT_ISSUER = 'https://your-test-issuer.example'
$env:JWT_AUDIENCE = 'transfer-engine'
$env:JWT_PUBLIC_KEY = '<base64 standard of the 32 raw Ed25519 public-key bytes>'
# Exclusivamente para development/test:
$env:TEST_FUNDS_ENABLED = 'true'
go run ./cmd/api
```

La clave es pública, no PEM, semilla ni clave privada. El servicio sólo verifica
JWT: no emite tokens ni genera claves de firma. El emisor de prueba debe firmar
con la privada correspondiente, usando `alg=EdDSA`, `iss`, `aud`, `sub` y `exp`.
También se verifican `nbf` e `iat` cuando están presentes, sin tolerancia temporal
adicional. No se descargan claves desde URLs incluidas en el token. Una única
clave configurada se aplica al proceso; rotarla requiere reiniciar instancias.
JWKS dinámico y un servidor de identidad están fuera de este alcance.

Las rutas financieras requieren el historial de migraciones 1, 2 y 3. Para una
base gestionada por el runner, ejecutar `go run ./cmd/api -migrate`. Para un
volumen Compose con los tres scripts ya aplicados pero sin historial, verificar
primero el esquema y ejecutar `go run ./cmd/api -migrate -baseline=3`.
No adoptar un baseline sin verificarlo. Un esquema sin historial, incompleto o
más nuevo impide el arranque financiero. Readiness vuelve a comprobar versión y
tablas requeridas; liveness no depende del broker ni de la base.

## Autoridad, entrada y respuestas

| Operación | Autorización y comportamiento |
|---|---|
| `POST /v1/accounts` | Propietario derivado del `sub`; cuerpo sólo `currency`; 201 |
| `GET /v1/accounts/{id}` | Sólo propietario; 200 o 404 |
| `GET /v1/accounts/{id}/balance` | Sólo propietario; moneda e importe en unidades menores |
| `GET /v1/accounts/{id}/movements` | Sólo propietario; `limit` 1–100 (50 por defecto), cursor opaco |
| `POST /v1/transfers` | Propietario del origen; clave de idempotencia obligatoria; 201 o replay 200 |
| `GET /v1/transfers/{id}` | Propietario de cualquiera de las cuentas participantes; otros reciben 404 |
| `POST /v1/test-funds` | Sólo development/test con flag; propietario del destino; 201 o replay 200 |

Los movimientos están ordenados por ID de posting descendente, con una consulta
adicional de un elemento para calcular `next_cursor`. Un cursor no concede
permisos. La paginación no mantiene un snapshot entre peticiones: ante commits
concurrentes puede ser necesario comenzar de nuevo para ver nuevos registros.
La respuesta de emisión ficticia contiene el ID del journal, no de una
transferencia; no existe una transferencia bancaria detrás de ese resultado.

Ejemplo, con un JWT del emisor y UUIDs de cuentas creadas por la API:

```http
POST /v1/transfers
Authorization: Bearer <JWT>
Idempotency-Key: test-order-001
Content-Type: application/json

{"source_account_id":"<source UUID>","destination_account_id":"<destination UUID>","currency":"ARS","amount":2500}
```

Se admite un único objeto JSON UTF-8, con nombres exactos de campos. Se rechazan
duplicados, campos desconocidos (incluido `actor_id`), null, valores posteriores,
importes fraccionarios o fuera de int64 y contenido comprimido. El límite de
cuerpo se aplica antes de leerlo. Los importes nunca pasan por float.

Los errores tienen `{ "error": { "code", "message", "request_id" } }` y nunca
contienen errores internos, SQL, tokens o payloads. Mapeo principal:

- 400 `invalid_request`; 401 `unauthenticated` con `WWW-Authenticate: Bearer`.
- 404 `not_found`, también para recursos ajenos y fondos deshabilitados.
- 409 `idempotency_conflict` o `concurrency_conflict`.
- 413 `body_too_large`; 415 `unsupported_media_type`.
- 422 `insufficient_funds`, `inactive_account` o `balance_overflow`.
- 503 `service_unavailable`, `operation_in_flight` o `commit_unknown`, con
  `Retry-After: 1`. Para transferencias y fondos, conservar **la misma clave y
  payload** al reintentar. Un 503 o perder la respuesta no prueba rollback.

Crear cuentas no es idempotente. No aplicar automáticamente a ese endpoint la
política de reintentos de transferencias. `TEST_FUNDS_ENABLED=true` se rechaza
en configuración de producción; además, el adaptador no registra la ruta allí.

## OpenTelemetry y logs

Se instrumentan HTTP, publicación y consumo. El contexto W3C `traceparent` pasa
del HTTP al contexto del servicio, al outbox dentro de la transacción financiera,
al envelope y al span consumidor. No se propaga baggage. Los spans de publicación
y consumo comparten la traza del evento original, incluso si hay reintentos.
Los duplicados pueden crear varios spans, con un solo efecto persistido.

Los logs HTTP incluyen `request_id` generado por el servidor, `trace_id`, `span_id`,
ruta registrada, método normalizado y status. Los de consumo incluyen la traza y
un resultado acotado. Se omiten JWT, IDs financieros, cuerpos, query strings,
errores internos y URLs sin normalizar. Las métricas son:

- `http.server.requests`: contador por método, plantilla de ruta y status.
- `http.server.duration`: histograma en segundos con las mismas dimensiones.
- `messaging.deliveries`: contador por resultado `handled`/`unconfirmed`.

`handled` significa que se hizo ACK tras persistir el efecto o confirmar un
handoff a retry/DLQ; no significa necesariamente que se creó una notificación.
Los IDs de cuenta, transferencia, actor, petición y traza no son labels métricos.

Para exportar a un colector OTLP/HTTP accesible:

```powershell
$env:TELEMETRY_OTLP_ENDPOINT = 'http://localhost:4318'
go run ./cmd/api
```

En Compose la dirección debe ser alcanzable desde el contenedor; su `localhost`
es el propio contenedor. Sin endpoint se generan contextos para correlación pero
no se exportan señales. No se despliega un colector ni dashboard automáticamente.
El endpoint es configuración confiable del operador, nunca un dato del request.

La exportación de trazas usa una cola de 512 spans y lotes de hasta 128, sin
bloquear peticiones; ante saturación puede descartar spans. Las métricas se
exportan cada 10 s. Cada export tiene timeout de 1 s y sin reintentos internos.
El apagado dispone de 3 s después de finalizar los usuarios de las dependencias.
Los fallos emiten `telemetry_export_failed` o `telemetry_shutdown_failed` sin
revelar el error del exportador. No se promete entrega duradera de telemetría ni
un contador específico de spans descartados; no debe usarse como ledger de
auditoría. Se respeta el bit sampled remoto; las trazas raíz se muestrean al 100 %.

## Evidencia

Los unitarios prueban firma/algoritmo, issuer/audience, exp, nbf, iat y subject;
entrada estricta, ausencia de fondos en producción, errores, labels acotados y
redacción. Un receptor HTTP local comprueba los POST OTLP reales de ambas señales;
otro colector bloqueado comprueba el límite de shutdown.

`TestAuthenticatedFinancialHTTP` usa PostgreSQL real: creación, emisión,
transferencia, replay/conflicto, propiedad, paginación, esquema incompatible,
traceparent en outbox y cierre TCP tras commit antes de enviar respuesta. El
reintento devuelve 200 y los saldos prueban que no hubo doble movimiento.
`TestReliableMessaging` agrega una transferencia HTTP autenticada y confirma
la misma traza en el consumidor a través de RabbitMQ real.

Referencias: [validación JWT](https://golang-jwt.github.io/jwt/usage/parse/) y
[exportadores OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/exporters/).
