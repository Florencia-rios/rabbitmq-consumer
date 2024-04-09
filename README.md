## Instalación de RabbitMQ Mac:

* Primero, hay que instalar una herramienta que necesita RabbitMQ, llamada Erlang OTP (Open Telecom Platform), que es un conjunto de herramientas y bibliotecas diseñadas para desarrollar sistemas distribuidos, concurrentes y tolerantes a fallos.
https://www.erlang.org/downloads <br>
Para esto, se puede descargar homebrew y luego ejecutar: 


  ```
  brew install erlang
  ```
 
* Hay que instalar RabbitMQ
https://www.rabbitmq.com/docs/install-homebrew <br>
RabbitMQ da dos opciones al terminar de instalarlo:
Que se corra en segundo plano como servicio automáticamente con el comando:


  ```
  brew services start rabbitmq
  ```

Que lo corramos a mano con el comando:

  ```
CONF_ENV_FILE="/usr/local/etc/rabbitmq/rabbitmq-env.conf" /usr/local/opt/rabbitmq/sbin/rabbitmq-server
  ```
* Abrir: localhost:15672 con el user y pass guest

* Crear otro usuario, en la sección de Admin, le damos a Add a user (en tags se agrega el rol, por ejemplo administrator.
Luego, seleccionamos el usuario y le damos al botón Set permission. 

* Verificar en la sección Admin, que el usuario haya quedado igual al por default (guest). Cerramos la sesión e ingresamos con el nuevo usuario.
Por último, eliminamos el usuario guest.

Imagen de Docker de RabbitMQ

https://hub.docker.com/_/rabbitmq/ 
