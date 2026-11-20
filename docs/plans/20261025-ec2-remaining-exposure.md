# Lo que sigue expuesto si cae la EC2

Estado: pendiente. Las credenciales de IOL, PPI y Binance ya no están en la EC2 y su rol no puede leerlas. Una entrada a esa máquina igual alcanza otros secretos y toda la cartera guardada.

## Qué se llevaría

Quien entre a la EC2 no saca las claves de los brokers. Sí puede alcanzar:

- la contraseña de login y `JWT_SECRET`, que el backend carga desde `/fintracker/web`;
- las claves de Etherscan y Blockcypher, también en esa configuración web;
- la cartera persistida en el disco de la EC2 y los snapshots que el backend sube a S3.

La cartera en disco es el producto y vive ahí a propósito. Rotar secretos no la saca del servidor.

## Pendientes

- [ ] Rotar la contraseña de login y `JWT_SECRET` en `/fintracker/web`, sin copiarlos al host, al repo ni a los logs.
- [ ] Reiniciar solo el backend de Fintracker para que deje de usar los valores anteriores. No recrear Nginx, el frontend ni watering-system.
- [ ] Confirmar que las sesiones viejas dejan de servir y que el login nuevo entra.
- [ ] Decidir aparte si las claves de Etherscan y Blockcypher también se rotan. Su daño es de cuota o de uso de esas APIs, no de los brokers.

## Qué no hace falta reabrir

No volver a poner credenciales de brokers en la EC2. No leer `.env`. La rotación de las claves de IOL, PPI y Binance es otra decisión: el aislamiento no las invalida, pero no forma parte de este pendiente.
