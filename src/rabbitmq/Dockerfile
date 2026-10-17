FROM rabbitmq:3.9.16-management-alpine
RUN apk update && apk add curl
COPY logging.conf /etc/rabbitmq/conf.d/20-logging.conf
