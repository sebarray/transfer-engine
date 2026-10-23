# Transfer Engine

Servicio Go de **transferencias internas simuladas**: transferencias atómicas, ledger de doble entrada, idempotencia, conciliación con fencing y eventos con outbox/inbox, reintentos y DLQ. La API financiera autenticada se activa con `FINANCIAL_API_ENABLED=true`; por defecto HTTP expone salud y documentación. Ver [configuración JWT y OTLP](docs/phase6.md) y [OpenAPI](docs/openapi.json). No integra bancos, no procesa dinero real ni afirma estar listo para una institución financiera.

## Swagger UI

Con el servicio iniciado, abre **http://localhost:8080/swagger/** (ajusta el
puerto si lo cambiaste). `/swagger` redirige a esa ruta y `/openapi.json` entrega
el contrato. La documentación es pública y está disponible también en Docker;
los archivos están incluidos en el ejecutable y no requieren un CDN.

Para probar operaciones financieras, activa `FINANCIAL_API_ENABLED=true` y
configura migraciones y JWT según [fase 6](docs/phase6.md). En **Authorize** pega
un JWT válido, sin el prefijo `Bearer`, y usa **Try it out**. El token no se
conserva al recargar. Para transferencias y fondos ficticios completa también
`Idempotency-Key`. La documentación muestra el contrato completo: los endpoints
deshabilitados por configuración devuelven 404; los fondos ficticios siguen
limitados a development/test con `TEST_FUNDS_ENABLED=true`.

## Probar el recorrido completo

Con Go y Docker para contenedores Linux, desde la raíz:

```sh
go test -tags=integration ./tests/integration/... -run TestProcessRecoveryDemo -count=1 -timeout=7m -v
```

Compila y ejecuta el binario real con claves temporales y servicios descartables. Crea cuentas por HTTP, detiene RabbitMQ, confirma una transferencia con outbox pendiente, recupera el broker y ejecuta el replay por CLI. Verifica saldo, ledger, notificación e inbox. No requiere configurar tokens manualmente ni modifica los datos de Compose. Ver [demostraciones y límites de la evidencia](docs/DEMO.md); también disponible como `make demo`.

## Ejecutar con Docker

Requiere Docker con contenedores Linux y Docker Compose v2. Desde la raíz:

```sh
docker compose -f deployments/compose.yaml up --build -d --wait
curl http://localhost:8080/livez
curl http://localhost:8080/readyz
```

`/livez` devuelve `200 {"status":"ok"}` mientras el servidor atiende. `/readyz` verifica PostgreSQL con un plazo acotado: devuelve `200 {"status":"ready"}` o un error JSON `503`. La caída de PostgreSQL afecta readiness, no liveness. Al habilitar la API financiera, readiness exige además el historial de migraciones 1–3 y las tablas requeridas.

Compose incluye PostgreSQL, Redis y RabbitMQ. Redis coordina el scheduler opcional de conciliación; RabbitMQ publica el outbox y procesa notificaciones internas cuando se activa `MESSAGING_ENABLED=true`; ver [fase 4](docs/phase4.md). Los puertos se publican sólo en localhost. Las credenciales incluidas son exclusivamente para el entorno local. Se pueden cambiar los puertos y parámetros mediante variables de entorno o `.env`; ver [.env.example](.env.example).

```sh
docker compose -f deployments/compose.yaml down
```

Este comando conserva los volúmenes. La migración inicial se aplica automáticamente **sólo al crear un volumen PostgreSQL vacío**. Modificar el SQL y reiniciar un volumen existente no vuelve a aplicarla.

## Ejecutar Go en Windows

El módulo local se llama `transfer-engine`; no se presupone un propietario ni un remoto Git. Usa Go **1.27.3**, fijado también en CI y Docker. Un Go anterior con `GOTOOLCHAIN=auto` puede descargar el toolchain indicado en `go.mod`.

```powershell
docker compose -f deployments/compose.yaml up -d --wait postgres
$env:DATABASE_URL = 'postgres://transfer:transfer_dev@localhost:5432/transfer_engine?sslmode=disable'
go run ./cmd/api
```

La API lee variables del proceso: no carga `.env` automáticamente. `DATABASE_URL` es obligatoria. `TEST_FUNDS_ENABLED` vale `false` por defecto y la configuración rechaza `true` en `APP_ENV=production`; el endpoint de fondos ficticios sólo se registra con API financiera habilitada y entorno development/test. Ctrl+C inicia el drenaje de HTTP antes de cerrar el pool PostgreSQL. Al vencer `SHUTDOWN_TIMEOUT` se cancelan las solicitudes restantes y el proceso termina con error.

## Estructura y decisiones

- `cmd/api`: composición, configuración del servidor y ciclo de vida.
- `internal/presentation/http`: Echo, autenticación, DTOs estrictos, rutas, errores y telemetría HTTP.
- `internal/business`: transferencias, consultas, eventos, conciliación, identidad y salud; sólo biblioteca estándar.
- `internal/infrastructure`: PostgreSQL, Redis, RabbitMQ, JWT, workers, configuración y SDK OpenTelemetry.
- `migrations`: ledger, outbox/inbox, fencing, notificaciones y auditoría de replay.
- `tests/integration`: PostgreSQL/Redis/RabbitMQ, HTTP, concurrencia, fallos y proceso real.
- `deployments` y `.github/workflows`: imagen sin usuario root, Compose y CI.

Son exactamente tres capas de aplicación; infraestructura implementa el contrato consumido en negocio. Los logs usan `slog` JSON y un ID generado por el servidor para cada solicitud. Omiten tokens, cuerpo, query strings y errores internos de PostgreSQL. La [fase 6](docs/phase6.md) agrega JWT EdDSA, trace/span IDs y exportación opcional OTLP de métricas y trazas.

```mermaid
flowchart TD
    Client[Cliente HTTP con JWT] --> P[Presentation: HTTP]
    P --> B[Business: casos de uso y puertos]
    I[Infrastructure: adaptadores] -. implementa puertos .-> B
    I --> PG[(PostgreSQL: dinero, ledger, outbox e inbox)]
    I --> MQ[RabbitMQ: eventos, retry y DLQ]
    I --> Redis[Redis: coordinación auxiliar]
    Root[cmd/api: composición] --> P
    Root --> B
    Root --> I
```

| Decisión | Motivo y costo |
|---|---|
| Una transacción PostgreSQL por movimiento | Dinero, ledger, idempotencia y outbox se confirman juntos; requiere locks ordenados y plazos |
| Publicar antes de marcar el outbox | Evita perder eventos; puede duplicarlos tras un fallo y exige inbox |
| Conexión AMQP por publicación confirmada | Asociación simple entre return/confirm; mayor costo de conexión, sin promesas de throughput |
| Notificación interna persistida | Efecto e inbox atómicos; no representa entrega a correo o proveedor externo |
| Redis auxiliar con breaker y fencing PostgreSQL | Su caída no gobierna el dinero; cada proceso mantiene su propio breaker |
| JWT con una clave pública configurada | Trust boundary explícito; rotación por reinicio, sin JWKS dinámico |
| Telemetría asíncrona con cola acotada | No bloquea por caída del colector; puede perder señales y no sustituye al ledger |

El catálogo inicial admite ARS, USD y EUR. Los importes se almacenan como `bigint` en unidades menores. Cada journal tiene un débito y un crédito y debe cuadrar al commit. El historial financiero es inmutable. La tesorería de prueba es la contrapartida explícita de la emisión ficticia; sólo esa clase de cuenta puede tener saldo negativo. Ver [decisiones del esquema](docs/schema.md).

Las migraciones `.up.sql` y `.down.sql` contienen su propia transacción para uso con `psql`. El runner registra versión y checksum atómicamente con cada migración. Para una base vacía fuera de Compose:

```powershell
go run ./cmd/api -migrate
```

El rollback borra datos y se prueba únicamente en bases desechables. Para adoptar una base existente del bootstrap anterior y aplicar 000002, revisá los pasos de `-baseline=1` en [fase 3](docs/phase3.md). El baseline afirma qué migraciones ya están aplicadas: requiere verificar previamente ese estado. Compose aplica las tres migraciones sólo en volúmenes nuevos. La migración 000003 y el baseline de bases existentes se describen en [fase 4](docs/phase4.md).

## Verificación

```sh
go test ./... -count=1
go vet ./...
go build ./cmd/api
go test -race ./... -count=1
go test -tags=integration ./tests/integration/... -count=1 -timeout=10m
docker build -f deployments/Dockerfile -t transfer-engine:local .
```

La integración exige Docker y falla si no está disponible. Valida migración up/down/up, restricciones, journals al commit, inmutabilidad, conciliación, rollback, unicidad de idempotencia e inbox. También ejecuta los casos de uso reales: emisión ficticia, consultas por propietario, 100 gastos simultáneos, 100 transferencias opuestas y fallo del outbox después de modificar saldos. Comprueba que los registros se revierten juntos y los saldos concilian con el ledger.

`internal/business/transfers.Service` consume puertos transaccionales implementados por `postgres.NewTransferStore(pool)`. `CreateAccount`, `Account`, `Fund` y `Transfer` reciben un contexto y un actor de confianza. El constructor rechaza fondos habilitados en producción. Las transferencias y emisiones requieren una clave de idempotencia; el replay idéntico devuelve el mismo resultado y un payload diferente produce conflicto. Ante un commit incierto, consulta el resultado persistido durante un máximo de tres segundos, sin repetir el movimiento. Si no puede confirmarlo, conserva `ErrCommitUnknown`; cualquier reintento debe mantener la clave y el payload originales.

La fase 3 prueba 100 solicitudes idénticas con dos pools independientes, pérdida de respuesta, recuperación con contexto cancelado, conflictos optimistas y leases expirados. La conciliación se activa con `RECONCILIATION_ENABLED=true`, `REDIS_ADDR` y la configuración versionada almacenada en PostgreSQL. Está deshabilitada por defecto y jamás modifica saldos. Ver [diseño, límites y activación](docs/phase3.md).

El detector de carreras necesita CGO y un compilador C compatible. En Windows sin compilador, puede ejecutarse en Linux/WSL; CI lo ejecuta en Ubuntu. El `Makefile` ofrece los mismos comandos; `fmt-check` requiere un shell POSIX.

## Alcance y límites

[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) registra las siete fases y sus verificaciones. CI ejecuta unitarios, race detector, vet, compilación, integración completa (incluido el demo de proceso), validación de Compose e imagen Docker. Las verificaciones registradas son locales; no se afirma una ejecución alojada de GitHub Actions.

- Compose usa un nodo por dependencia; no demuestra alta disponibilidad, recuperación de disco ni failover de clúster.
- Las entregas son al menos una vez. Los efectos están protegidos por inbox y unicidad; no se promete entrega exactamente una vez ni orden global.
- La paginación usa ID de posting y no mantiene un snapshot entre solicitudes concurrentes.
- El CLI de replay depende de acceso al host y credenciales administrativas; el nombre del operador es una declaración auditada.
- No se incluyen issuer de JWT, colector, dashboards, benchmarks, pruebas de carga ni integraciones bancarias.
- Fondos ficticios y API financiera requieren habilitación explícita. El esquema bootstrap de Compose debe adoptarse con un baseline verificado antes de habilitar `/v1`.

Detalles: [mensajería y replay](docs/phase4.md), [resiliencia y apagado](docs/phase5.md), [seguridad y observabilidad](docs/phase6.md), [evidencia reproducible](docs/DEMO.md).

Requisitos: [producto](docs/PRODUCT.md), [arquitectura](docs/ARCHITECTURE.md), [contratos](docs/CONTRACTS.md), [confiabilidad](docs/RELIABILITY.md), [pruebas](docs/TESTING.md).
