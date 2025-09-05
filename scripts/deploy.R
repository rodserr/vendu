

docker build -t us-east1-docker.pkg.dev/vendu-tech/my-repo/alert-not-sales-r:latest .
docker push us-east1-docker.pkg.dev/vendu-tech/my-repo/alert-not-sales-r:latest

gcloud run jobs create alert-not-sales-r-job-arg `
--image us-east1-docker.pkg.dev/vendu-tech/my-repo/alert-not-sales-r:latest `
--region us-east1 `
--args "not_sales_alert.R" `
--memory 1Gi `
--cpu 1 `
--task-timeout 600s `
--project vendu-tech
