#!/bin/bash

riff service invoke correlator /demo-square --json -- \
  -H "knative-blocking-request:true" \
  -w '\n' \
  -d 7

