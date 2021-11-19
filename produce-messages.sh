#!/usr/bin/env bash

for i in {0..10000000}; do
  echo "$i: THIS IS $i";
done | kafkacat -b localhost:9092 -t inbox -P -K: