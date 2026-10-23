# Fase 5: resiliencia

La dependencia protegida por el circuit breaker es Redis, usado exclusivamente
para coordinar la conciliación auxiliar. No hay breaker en el camino financiero
de PostgreSQL. Se activa junto con `RECONCILIATION_ENABLED=true`; no requiere
migraciones ni nuevas variables de entorno.

## Circuit breaker de leases

`redis.NewProtectedLease` implementa el puerto de negocio `reconciliation.Lease`:

- Tras tres fallos de disponibilidad consecutivos, rechaza adquisiciones durante
  diez segundos con `ErrUnavailable`, sin contactar Redis.
- Al vencer ese plazo permite una sola llamada de prueba. El éxito o `ErrBusy`
  cierran el circuito: la contención demuestra que Redis respondió. Otro fallo
  abre nuevamente por diez segundos. La cancelación del padre no cuenta como
  fallo de disponibilidad y libera una prueba en curso.
- Una generación interna impide que una respuesta atrasada de una llamada
  anterior cierre un circuito abierto más recientemente. El estado está protegido
  por mutex y sólo se comparte dentro del proceso.
- Cada adquisición tiene un plazo máximo de un segundo, sin ampliar el plazo del
  padre. La liberación compara el token y tiene su propio plazo de un segundo;
  se permite aunque el circuito esté abierto. No se repite automáticamente un
  `SET NX` cuyo resultado podría ser incierto. Un lease huérfano vence por TTL.

PostgreSQL sigue verificando expiración y fencing antes de guardar un reporte.
El breaker reduce llamadas a Redis durante una caída; no sustituye esas garantías.
Un estado abierto se informa por la señal existente `reconciliation_failed`, sin
tokens ni credenciales. No se agregan aún métricas o trazas de transiciones;
la instrumentación pertenece a la fase 6.

## Plazos y reintentos

| Operación | Presupuesto y tratamiento |
|---|---|
| Transacción financiera | Máximo 10 s; lock timeout 3 s y statement timeout 8 s; sin repetición automática del movimiento |
| Resolución de commit incierto | Consulta independiente de hasta 3 s, conservando valores de contexto; si no puede resolver, mantiene resultado desconocido |
| Configuración de conciliación | 3 s en el servicio y adaptador; respeta un plazo padre menor |
| Conciliación | 5 s en la composición actual, más limpieza independiente de hasta 1 s; nunca modifica saldos |
| Publicación RabbitMQ | Hasta 5 s; conexión respeta el plazo del padre; una confirmación incierta conserva el original/outbox |
| Declaración de topología | Contexto global de hasta 10 s |
| Outbox | Transacción de hasta 8 s; sólo marca publicado tras confirm |
| Efecto consumidor | Transacción de hasta 4 s dentro del presupuesto de handler de 6 s |

La siguiente ejecución del scheduler tras un fallo de disponibilidad usa backoff exponencial de
1, 2, 4, 8, 16 y 32 s, con jitter de 80–120 %, y vuelve al intervalo configurado
al recuperarse. No repite pasos individuales de una operación ambigua. Las
ejecuciones siguen siendo secuenciales y los fences protegen los reportes.

La mensajería conserva su presupuesto inicial más tres reintentos y DLQ de la
fase 4. Los eventos pendientes en outbox no se descartan por cantidad de intentos;
se reprograman con espera acotada y cada publicación es cancelable. Los retries
pueden duplicar mensajes, cuyo efecto está protegido por el inbox. La reconexión
del supervisor es independiente del presupuesto de intentos de cada mensaje.

## Apagado y presión de trabajo

Al recibir terminación se deja de admitir HTTP, readiness deja de estar listo y
se drenan solicitudes aceptadas hasta `SHUTDOWN_TIMEOUT`. Los workers reciben
cancelación, dejan de tomar trabajo y terminan antes de cerrar sus dependencias.
El consumidor permite terminar su handler activo hasta seis segundos; los
mensajes sin ACK quedan para redelivery. La conciliación cancela su consulta y
libera el token bajo un contexto de limpieza acotado. Se espera a los workers
antes de cerrar Redis y PostgreSQL.

La concurrencia y prefetch siguen limitados; no se crean goroutines por mensaje.
Las colas tienen límite de longitud con rechazo de publicación. Estos límites
proporcionan presión al productor sin convertir una caída de RabbitMQ o Redis
en rollback de una transferencia ya confirmada. Ver detalles de TTL, quorum,
confirmaciones y replay en [fase 4](phase4.md).

## Verificación

Los unitarios del breaker usan reloj controlado y barreras, sin sleeps para
decidir corrección: apertura, prueba única frente a 100 llamadas concurrentes,
recuperación, prueba fallida, cancelación, limpieza con circuito abierto y
respuesta vieja después de la apertura. Se comprueba propagación de valores,
cancelación y plazo padre en la configuración del scheduler.

La integración detiene Redis real, acumula fallos en el lease protegido y
verifica que una transferencia sigue confirmándose y conciliando. Las pruebas
anteriores conservan fencing, cierre de workers, prefetched sin ACK,
confirmaciones inciertas y rollback. La recuperación temporal del breaker se
prueba con fake y reloj controlado; no se afirma un ensayo de failover distribuido.

La política es local al proceso y usa valores fijos deliberadamente. No hay
integraciones bancarias ni proveedores externos de notificaciones. HTTP financiero,
JWT, OpenAPI y OpenTelemetry se completan y documentan en la [fase 6](phase6.md).
