/*
Copyright 2026.

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
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	hyperfleetdb "github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-db"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	v1alpha1 "github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/api/v1alpha1"
	"github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/internal/controller"
	"github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/internal/dynamo"
	"github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/internal/dynamo/statusstream"
	"github.com/openshift-online/rosa-hyperfleet-api/hyperfleet-operator/internal/render"
)

var setupLog = ctrl.Log.WithName("setup")

func main() {
	var metricsAddr string
	var probeAddr string
	var awsRegion string
	var baseDomain string
	var maxConcurrentReconciles int

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&awsRegion, "aws-region", "", "AWS region for DynamoDB and EKS (required).")
	flag.StringVar(&baseDomain, "base-domain", "", "DNS base domain for hosted clusters (required).")
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 10,
		"Maximum number of concurrent reconciles per controller.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if awsRegion == "" {
		setupLog.Error(nil, "--aws-region is required")
		os.Exit(1)
	}
	if baseDomain == "" {
		setupLog.Error(nil, "--base-domain is required")
		os.Exit(1)
	}

	tablePrefix := os.Getenv("HYPERFLEET_DB_TABLE_PREFIX")
	if tablePrefix == "" {
		setupLog.Error(nil, "HYPERFLEET_DB_TABLE_PREFIX environment variable is required")
		os.Exit(1)
	}

	replicaCount := envInt("REPLICA_COUNT", 1)
	ordinal, err := podOrdinal()
	if err != nil {
		setupLog.Error(err, "Failed to parse pod ordinal from hostname")
		os.Exit(1)
	}

	setupLog.Info("shard config",
		"replicaCount", replicaCount,
		"ordinal", ordinal,
	)

	signalCtx := ctrl.SetupSignalHandler()
	ctx := context.Background()

	// Load AWS config — credentials come from Pod Identity / IRSA automatically.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(awsRegion))
	if err != nil {
		setupLog.Error(err, "Failed to load AWS config")
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		setupLog.Error(err, "Failed to add v1alpha1 to scheme")
		os.Exit(1)
	}

	log := ctrl.Log.WithName("hyperfleetdb")

	dynamoDBClient := dynamodb.NewFromConfig(awsCfg)

	mgr, err := hyperfleetdb.NewManager(hyperfleetdb.Options{
		Scheme:      scheme,
		DynamoDB:    dynamoDBClient,
		TablePrefix: tablePrefix,
		Shard: &hyperfleetdb.ShardConfig{
			Mod:   replicaCount,
			Owned: []int{ordinal},
			UnshardedGVKs: []schema.GroupVersionKind{
				v1alpha1.SchemeGroupVersion.WithKind("ManagementCluster"),
			},
		},
		Logger:                 log,
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "Failed to create hyperfleetdb manager")
		os.Exit(1)
	}

	dynamoClient := dynamo.NewClient(dynamoDBClient)

	rcfg := render.RegionalConfig{
		BaseDomain: baseDomain,
		AWSRegion:  awsRegion,
	}

	eventRouter := controller.NewEventRouter()
	clusterStatusEvents := make(chan event.GenericEvent, 256)
	nodePoolStatusEvents := make(chan event.GenericEvent, 256)
	manifestStatusEvents := make(chan event.GenericEvent, 256)

	if err := (&controller.ClusterReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Dynamo:                  dynamoClient,
		RegionalConfig:          rcfg,
		StatusEvents:            clusterStatusEvents,
		EventRouter:             eventRouter,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "cluster")
		os.Exit(1)
	}
	if err := (&controller.NodePoolReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Dynamo:                  dynamoClient,
		StatusEvents:            nodePoolStatusEvents,
		EventRouter:             eventRouter,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "nodepool")
		os.Exit(1)
	}
	if err := (&controller.PlacementReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "placement")
		os.Exit(1)
	}
	if err := (&controller.ManifestReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		Dynamo:                  dynamoClient,
		StatusEvents:            manifestStatusEvents,
		EventRouter:             eventRouter,
		MaxConcurrentReconciles: maxConcurrentReconciles,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "manifest")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	streamMgr := statusstream.NewManager(
		dynamoDBClient,
		mgr.GetClient(),
		[]string{dynamo.TableSuffixStatusApplyDesires, dynamo.TableSuffixStatusReadDesires},
		func(documentID string) { eventRouter.Dispatch(documentID) },
		slog.Default().With("component", "statusstream"),
	)
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	go streamMgr.Run(watchCtx, 5*time.Second)

	setupLog.Info("Starting hyperfleetdb manager")
	if err := mgr.Start(signalCtx); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

func envInt(key string, fallback int) int {
	s := os.Getenv(key)
	if s == "" {
		return fallback
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		setupLog.Error(err, "Invalid integer env var", "key", key, "value", s)
		os.Exit(1)
	}
	return v
}

// podOrdinal extracts the StatefulSet ordinal from the hostname.
// e.g. "hyperfleet-operator-2" → 2. Returns 0 if no trailing number.
func podOrdinal() (int, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return 0, fmt.Errorf("get hostname: %w", err)
	}
	parts := strings.Split(hostname, "-")
	last := parts[len(parts)-1]
	ordinal, err := strconv.Atoi(last)
	if err != nil {
		return 0, nil
	}
	return ordinal, nil
}
