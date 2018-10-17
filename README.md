# stream-controller PoC

setup the controller and CRDs:
`kubectl apply -f config/`

build this branch of the riff CLI:
https://github.com/markfisher/riff/tree/streampoc

create the "hello" and "square" functions and the "demo" stream by running:
`./demo/riff.sh`

invoke with: `./demo/invoke.sh`

teardown with: `./demo/teardown.sh`

