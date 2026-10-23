# Demostraciones reproducibles

Ejecutar desde la raíz con Go 1.27.3 (o descarga automática del toolchain) y Docker
con contenedores Linux. No hace falta configurar un issuer, copiar JWT, levantar
Compose ni reservar los puertos habituales. Testcontainers crea servicios con
puertos aleatorios y datos descartables. La prueba genera claves y tokens en
memoria; no los imprime. No detiene contenedores ajenos.

## API real, caída del broker y replay administrativo

```powershell
go test -tags=integration ./tests/integration/... -run TestProcessRecoveryDemo -count=1 -timeout=7m -v
```

En Linux/macOS/WSL también puede usarse `make demo`. El test compila `cmd/api` en
una carpeta temporal y ejecuta el binario como proceso separado. El Go usado para
compilar debe estar en PATH. La prueba falla si Docker o una dependencia no está
disponible; no se omite silenciosamente.

El recorrido y sus condiciones de aprobación son:

1. Inicia PostgreSQL y RabbitMQ, aplica migraciones y habilita la API con JWT.
   Crea cuentas de Alice y Bob por HTTP y emite 100 unidades menores ficticias.
2. Detiene la aplicación RabbitMQ con `rabbitmqctl stop_app`. Comprueba que AMQP
   queda inaccesible. Una transferencia de 25 devuelve **201**, deja saldos
   **75/25**, ledger conciliado y outbox pendiente.
3. Ejecuta `rabbitmqctl start_app` y espera evidencia persistida: el evento queda
   publicado y aparece la notificación. Repetir el HTTP con la misma clave
   devuelve **200** y el mismo ID.
4. Coloca en DLQ un evento válido de fixture que referencia esa transferencia,
   con una identidad nueva. Esto permite observar un nuevo inbox después del
   replay y comprobar que la unicidad de transferencia evita otro efecto.
5. Ejecuta el CLI real: las escrituras están rechazadas sin el flag; el dry run
   muestra el evento y no escribe auditoría; el replay habilitado deja una
   auditoría confirmada con operador y motivo. Espera el inbox de la nueva
   identidad, verifica **una sola notificación** y saldos **75/25**.

Las líneas `PROOF` aparecen sólo después de sus assertions:

```text
PROOF broker stopped: HTTP 201, balances 75/25, durable outbox pending
PROOF broker recovered: outbox published, notification persisted, identical HTTP retry returns 200
PROOF CLI: guarded writes, dry run, confirmed audit, one notification and unchanged balances
```

El resultado debe terminar en `PASS`. Son invariantes comprobadas, no benchmarks.
La terminación del proceso al limpiar esta prueba es forzada y acotada; no es
evidencia de drenaje por señal. HTTP y workers tienen pruebas específicas de
apagado. El broker es un nodo local: este ensayo no prueba failover de clúster,
pérdida de disco ni alta disponibilidad.

## Ventanas de fallo, reintentos y DLQ

```powershell
go test -tags=integration ./tests/integration/... -run TestReliableMessaging -count=1 -timeout=5m -v
```

Esta demostración usa los adaptadores reales con inyección controlada de fallos:
confirm antes de marcar el outbox, efecto confirmado antes de ACK, rollback de
inbox, versión inválida, confirm incierto de DLQ, agotamiento de reintentos,
inspección/replay y trazas desde HTTP hasta el consumidor. A diferencia del fixture
del demo de proceso, aquí se verifica redelivery del **mismo event ID** y se llega
a DLQ por agotamiento real del presupuesto. Los TTL se acortan en el test; el
runtime mantiene 5 s, 30 s y 2 min con jitter. No se acelera el código productivo.

## Dinero, autorización y concurrencia

```powershell
# Ejecuta toda la evidencia: invariantes, concurrencia, fallos, HTTP y proceso real.
go test -tags=integration ./tests/integration/... -count=1 -timeout=10m -v
```

| Evidencia | Test |
|---|---|
| Commit atómico, rollback, 100 gastos y transferencias opuestas | `TestAtomicTransfers` |
| Idempotencia entre pools y resultado de commit incierto | Tests de `idempotency_test.go` |
| Leases vencidos, fencing, versiones y Redis detenido | `TestReconciliationCoordination` |
| JWT, propiedad, paginación y pérdida TCP tras commit | `TestAuthenticatedFinancialHTTP` |
| Confirms, inbox, retry/DLQ, trazas y workers | `TestReliableMessaging` |
| Binario, RabbitMQ detenido/reiniciado y CLI | `TestProcessRecoveryDemo` |
| Migraciones y protecciones del ledger | Tests de `schema_test.go` y `migrations_test.go` |

El `-race` de CI ejecuta los tests Go sin build tag de integración. Las pruebas
de concurrencia de PostgreSQL comprueban invariantes distribuidas por separado;
el detector de carreras no sustituye esas assertions. El binario hijo del demo
se compila normalmente, sin `-race`. Las réplicas de idempotencia anteriores son
dos pools independientes dentro de un proceso de prueba.

## Diagnóstico y repetición

Esperar a que Docker Desktop esté listo y use contenedores Linux. El primer run
descarga imágenes y módulos. Los tests esperan estados observables con plazos;
no consideran que un simple sleep pruebe recuperación. Un fallo en limpieza de
contenedores también hace fallar el test y debe distinguirse de las assertions.
Volver a ejecutar después de corregir el problema genera datos aislados nuevos.

Para operar una DLQ persistente de Compose, usar los comandos y las garantías de
[fase 4](phase4.md). No copiar los pasos de inyección del test sobre datos que se
quieran conservar. Para configurar la API y OTLP fuera del demo, ver
[fase 6](phase6.md).

CI incluye este test en `make integration` dentro de
[ci.yaml](../.github/workflows/ci.yaml), junto con unitarios, race detector, vet,
build y validación de contenedor. Que esté configurado no implica que exista
una ejecución de GitHub Actions: los resultados registrados en el plan son locales.
