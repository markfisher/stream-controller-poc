#!/bin/bash

riff function register square --repo https://github.com/markfisher/riff-square.git --artifact square.js --image gcr.io/cf-spring-funkytown/mf-riff-square:v1

riff function register hello --image gcr.io/cf-spring-funkytown/mf-riff-sample-hello:v1

riff stream create demo --definition "square | hello --greeting=Bonjour"

