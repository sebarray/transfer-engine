# Fase 4: eventos confiables

La mensajería procesa una **notificación interna simulada**: una fila en
`transfer_notifications`. No envía correos, no contacta proveedores y no modifica
saldos. PostgreSQL sigue siendo la fuente de verdad financiera.

## Activación y migración

Con `DATABASE_URL` configurada, una base que ya usa el runner de migraciones se
actualiza con:

```powershell
go run ./cmd/api -migrate
$env:MESSAGING_ENABLED = 'true'
$env:RABBITMQ_URL = 'amqp://transfer:transfer_dev@localhost:5672/'
go run ./cmd/api
```

La migración 000003 agrega notificaciones y auditoría de replay. Si la base fue
creada con los scripts de bootstrap de Compose y carece de `schema_migrations`,
primero verificar cuáles scripts están realmente aplicados: usar `-migrate
-baseline=2` si tiene exactamente 000001 y 000002; `-baseline=3` si ya tiene los
tres. El baseline registra esa afirmación; no verifica ni repara el esquema.
Nunca adoptarlo sin comprobar el estado. Los volúmenes nuevos de Compose ejecutan
los tres scripts; reiniciar un volumen existente no aplica migraciones pendientes.

Para Compose, definir `MESSAGING_ENABLED=true` en el entorno o `.env` y ejecutar
`docker compose -f deployments/compose.yaml up --build -d --wait` después de
actualizar el esquema. La API no carga `.env` al ejecutarse directamente con Go.
Los consumidores se deshabilitan por defecto. RabbitMQ no gobierna readiness;
su caída permite conservar transferencias confirmadas y eventos pendientes.

## Publicación y consumo

- El movimiento financiero ya guarda el outbox en la misma transacción. El
  publicador toma una fila con `FOR UPDATE SKIP LOCKED`, mantiene el lock durante
  una publicación acotada y marca `published_at` sólo tras el confirm positivo
  de RabbitMQ y sin devolución por ruta inexistente (`mandatory`). Si el proceso
  cae, PostgreSQL libera el lock; publicar y fallar antes de marcar puede duplicar.
- El envelope v1 incluye identidad, tipo, versión, agregado, fecha y payload con
  transferencia, moneda e importe entero. Se rechazan versiones desconocidas,
  campos inesperados, cuerpos mayores de 64 KiB y datos inválidos.
- Cada consumidor guarda inbox y notificación en una sola transacción. Comprueba
  la transferencia contra PostgreSQL. La identidad del evento deduplica entregas;
  la unicidad de transferencia protege además el efecto. Un ACK perdido después
  del commit provoca redelivery sin duplicar la notificación.
- Las colas `transfer.completed.v1`, `.retry.1`, `.retry.2`, `.retry.3` y `.dlq`
  son quorum durables, con mensajes persistentes, límite de 10.000 y
  `reject-publish`. Los dead letters de principal y retry usan estrategia
  `at-least-once`. Un solo broker local no constituye alta disponibilidad.
- Hay un intento inicial y tres reintentos: 5 s, 30 s y 2 min, con jitter de
  80–120 %. Se usa TTL por mensaje: un mensaje puede esperar detrás de otro de
  mayor TTL; no se promete una hora exacta. Errores permanentes van directo a
  DLQ; los transitorios agotan el presupuesto. Metadata: intento, replays y motivo.
- Se hace ACK sólo después del efecto confirmado o de la publicación confirmada
  en retry/DLQ. Ante confirm incierto se cierra la conexión y queda el original
  sin ACK. Puede haber duplicados. No hay bucle de NACK inmediato. El límite de
  redelivery del broker (20) permite aislar caídas repetidas, con metadata
  `x-death` administrada por RabbitMQ.
- `MESSAGE_WORKERS` admite 1–16 (default 4), `MESSAGE_PREFETCH` 1–64 (default 1).
  Cada worker procesa un mensaje a la vez. Al detenerse deja de admitir trabajo,
  da hasta 6 s al handler activo y reencola los prefetched sin ACK. El supervisor
  reconecta con backoff y jitter. Los efectos son independientes del orden;
  no se garantiza orden global entre consumidores y reintentos.

El publicador usa una conexión por publicación para mantener inequívoca la
asociación entre returns y confirms. Es una decisión de simplicidad, con costo de
conexión y una transacción abierta durante el confirm (hasta 8 s en el outbox).
No se presentan resultados de throughput. Las publicaciones fallidas permanecen
en outbox con backoff acotado; no se descartan por agotar un contador.

## Inspección y replay de DLQ

CLI administrativo, protegido por acceso al host y credenciales PostgreSQL/AMQP;
`-operator` es una identidad declarada para auditoría, no autenticación. No hay
endpoint HTTP de replay. Limitar esas credenciales a operadores autorizados.

```powershell
# Sólo metadata; no publica ni hace ACK. Default: dry-run=true.
go run ./cmd/api -replay-dlq -limit=10

# Tras resolver la causa del fallo:
$env:DLQ_REPLAY_ENABLED = 'true'
go run ./cmd/api -replay-dlq -dry-run=false -limit=10 -operator=local-admin -reason='dependency recovered'
```

El lote admite 1–100 mensajes y valida el contrato antes de republicar. Conserva
`event_id`, reinicia los intentos y permite hasta cinco ciclos por copia de mensaje.
Registra operador y motivo antes de publicar, y confirmación antes del ACK.
Una auditoría sin `confirmed_at` representa un resultado no confirmado, no prueba
de que no se haya publicado. Las ventanas de fallo pueden duplicar también el
replay; el inbox protege el efecto. El límite por copia no es una cuota global
de replay por evento. Los mensajes inválidos detienen el replay activo y quedan
sin ACK; requieren diagnóstico, no modificación automática del contrato.
El dry run toma mensajes temporalmente sin ACK y los devuelve al cerrar conexión:
no elimina mensajes, pero puede cambiar su orden/redelivery metadata.

## Evidencia y límites

`TestReliableMessaging` usa PostgreSQL y RabbitMQ reales de Testcontainers:
publicación sin ruta, endpoint de broker inaccesible y recuperación, fallo SQL
inyectado después del confirm, cierre del consumidor después del commit antes
del ACK, rollback de inbox y efecto, versión venenosa, confirm incierto de DLQ,
TTL de reintentos, agotamiento, replay auditado y drenaje de workers acotados.
Los delays de integración son menores para verificar el mecanismo sin esperar
minutos; los unitarios verifican el jitter. El test de caída del broker usa un
endpoint inaccesible, no una prueba de pérdida de disco o failover de clúster.

```sh
go test -tags=integration ./tests/integration/... -run TestReliableMessaging -count=1 -v
```

La [fase 6](phase6.md) agrega HTTP financiero/JWT y verifica la propagación
OpenTelemetry de extremo a extremo mediante `traceparent`.
La [fase 5](phase5.md) completa la política de resiliencia y el circuit breaker auxiliar.

Referencias de transporte: [confirms y ACKs](https://www.rabbitmq.com/docs/confirms),
[dead letters](https://www.rabbitmq.com/docs/dlx) y
[quorum queues 4.3](https://www.rabbitmq.com/docs/4.3/quorum-queues).
