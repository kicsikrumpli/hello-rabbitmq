#!/bin/bash

echo "usage: "
echo "./entrypoint.sh producer"
echo "or"
echo "./entrypoint.sh consumer"
echo "----"

poetry run python -m hello_rabbitmq.${1}