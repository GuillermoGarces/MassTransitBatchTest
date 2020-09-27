#!/bin/bash

container=$(docker ps | grep "rabbitmq" | awk '{ print $1 }')

docker exec $container bash -c "rabbitmqctl stop_app; rabbitmqctl reset; rabbitmqctl start_app"

(sleep 2s; docker exec $container bash -c "rabbitmqctl close_all_connections 'simulating disconnection'" )&

dotnet run
