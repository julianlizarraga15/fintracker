# Dejar de entrar a AWS como root

Estado: pendiente. El acceso del 2026-10-25 a la cuenta `875551125619` fue el usuario root. Sirvió para inventariar el host y no amplió los permisos de EC2. No es la identidad que hay que usar en el día a día.

## Por qué

Una sesión root en esta máquina puede borrar o cambiar cualquier recurso de la cuenta: IAM, Lambdas, parámetros, ECR, S3 y la EC2 donde también corre watering-system. El rol de EC2 sigue sin poder leer las credenciales de los brokers. Este pendiente es sobre la identidad administrativa, no sobre ese rol.

## Pendientes

- [ ] Crear un usuario IAM de administración, con MFA, para el trabajo administrativo de Fintracker.
- [ ] Usar ese usuario en los próximos accesos temporales. No volver a autorizar la CLI con root.
- [ ] Guardar el root sin uso diario. Dejarlo solo para las pocas acciones que AWS exige hacer como root.
- [ ] Confirmar que el usuario nuevo puede hacer el trabajo administrativo necesario sin ser root, y que no reemplaza al rol acotado de EC2.
- [ ] Cerrar la sesión root local cuando el usuario nuevo esté listo. Un token ya cargado puede seguir vigente unos minutos después del logout.

## Qué no cambia

El aislamiento de las credenciales de IOL, PPI y Binance queda como está. Este usuario no debe recibir permiso para leer los parámetros de los workers, ni hay que volver a poner esas credenciales en la EC2.
