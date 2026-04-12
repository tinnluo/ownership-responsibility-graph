# Kubernetes Deployment

These manifests run the demo pipeline in a local Kubernetes cluster:

- `neo4j` runs as a single-replica StatefulSet with persistent data and logs.
- `ownership-graph-build` writes normalized and staged graph files to a shared PVC.
- `ownership-graph-analyze` writes the analysis artifacts required by the loader.
- `ownership-graph-load-neo4j` loads the staged graph and attribution relationships into Neo4j.

The `graph-output` PVC is declared in `neo4j/pvc.yaml` with the Neo4j PVCs for a single setup step, even though it is shared by the graph, analysis, and loader Jobs rather than the Neo4j StatefulSet.

## Prerequisites

- A local Kubernetes cluster, such as minikube or kind.
- `kubectl` configured for that cluster.
- A locally built `ownership-responsibility-graph:latest` image available inside the cluster.

Build the image:

```bash
docker build -t ownership-responsibility-graph:latest .
```

Load it into your local cluster:

```bash
minikube image load ownership-responsibility-graph:latest
```

For kind, use:

```bash
kind load docker-image ownership-responsibility-graph:latest
```

## Validate

Render the manifests locally and run a client-side manifest dry run when a Kubernetes API server is reachable:

```bash
make k8s-dry-run
```

## Apply

Apply the resources in dependency order:

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/neo4j/pvc.yaml
kubectl apply -f k8s/neo4j/service.yaml
kubectl apply -f k8s/neo4j/statefulset.yaml
kubectl rollout status statefulset/neo4j -n ownership-graph --timeout=180s

kubectl apply -f k8s/graph/job.yaml
kubectl wait --for=condition=complete job/ownership-graph-build -n ownership-graph --timeout=120s

kubectl apply -f k8s/graph/analyze-job.yaml
kubectl wait --for=condition=complete job/ownership-graph-analyze -n ownership-graph --timeout=120s

kubectl apply -f k8s/neo4j-loader/job.yaml
kubectl wait --for=condition=complete job/ownership-graph-load-neo4j -n ownership-graph --timeout=180s
```

The loader runs with `--wipe`, so it deletes existing Neo4j graph data before loading the demo dataset.

## Monitor

```bash
kubectl get pods -n ownership-graph
kubectl logs -n ownership-graph job/ownership-graph-build
kubectl logs -n ownership-graph job/ownership-graph-analyze
kubectl logs -n ownership-graph job/ownership-graph-load-neo4j
```

Open Neo4j Browser locally:

```bash
kubectl port-forward -n ownership-graph service/neo4j-service 7474:7474 7687:7687
```

The browser is then available at `http://localhost:7474`.

## Teardown

```bash
kubectl delete namespace ownership-graph
```
