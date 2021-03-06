/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"flag"
	"time"

	"github.com/golang/glog"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	eventingclientset "github.com/knative/eventing/pkg/client/clientset/versioned"
	servingclientset "github.com/knative/serving/pkg/client/clientset/versioned"
	servinginformers "github.com/knative/serving/pkg/client/informers/externalversions"

	clientset "github.com/projectriff/stream-controller/pkg/client/clientset/versioned"
	informers "github.com/projectriff/stream-controller/pkg/client/informers/externalversions"
	"github.com/projectriff/stream-controller/pkg/controllers/stream"

	"github.com/projectriff/stream-controller/pkg/signals"
)

var (
	masterURL  string
	kubeconfig string
)

func main() {
	flag.Parse()

	// set up signals so we handle the first shutdown signal gracefully
	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		glog.Fatalf("Error building kubeconfig: %s", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	servingClient, err := servingclientset.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Error building serving clientset: %s", err.Error())
	}

	eventingClient, err := eventingclientset.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Error building eventing clientset: %s", err.Error())
	}

	streamClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("Error building example clientset: %s", err.Error())
	}

	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	streamInformerFactory := informers.NewSharedInformerFactory(streamClient, time.Second*30)
	servingInformerFactory := servinginformers.NewSharedInformerFactory(servingClient, time.Second*30)

	controller := stream.NewController(kubeClient, servingClient, eventingClient, streamClient,
		streamInformerFactory.Projectriff().V1alpha1().Streams(),
		streamInformerFactory.Projectriff().V1alpha1().Functions(),
		servingInformerFactory.Serving().V1alpha1().Services())

	go kubeInformerFactory.Start(stopCh)
	go streamInformerFactory.Start(stopCh)

	if err = controller.Run(2, stopCh); err != nil {
		glog.Fatalf("Error running controller: %s", err.Error())
	}
}

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.StringVar(&masterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
}
