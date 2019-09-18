#!/bin/bash

./mvnw install

cd cart-producer
./../mvnw exec:java -Dexec.mainClass="org.bd.cart.CartProducer" -Dexec.args="ex"