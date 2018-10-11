#!/bin/bash

kubectl delete stream demo

kubectl delete ksvc hello
kubectl delete ksvc square

kubectl delete channel demo-hello
kubectl delete channel demo-square

kubectl delete subscription hello
kubectl delete subscription square

