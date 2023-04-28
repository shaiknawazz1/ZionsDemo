# Zions RFP Demo for Data Stage to pySpark Migration

## About

## Setting up minimal development environment

*Required Tools*
   * Git 1.7.1+
   * Java 1.7+
   * Docker 1.12+
   * Docker Compose 1.12+
   * python
   * pySPark
   * google cloud sdk (https://cloud.google.com/sdk/docs/install)
   *

1. Clone this Git repository, best using SSH.
2. build the project with `docker-compose build`.
3. run the image with `docker-compose up`.
4. you should be able to see the app running locally on http://127.0.0.1:5000/


## Configuring the gke env using terraform
1. navigate to `terraform-gke` folder from the root.
2. download the credentials file from google cloud account (make sure this service account has necessary permissions required)
3. rename the file credentials.json and place in `terraform-gke` folder and run the below commands 
   
   ```
   terraform init
   terraform plan
   terraform apply
   ```
4. user `terraform destoy` command to clean up the gke cluster.
   
## Building docker image locally and deployed to GKE environment
(assuming the use of linux or for windows using Mingw shell for all below commands)

1. Login to docker repository 
   ```
   cat KEY-FILE | docker login -u _json_key --password-stdin \
   https://HOSTNAME
   ```
2. Run the following command to pull and push the image to gcr:
   ```
   docker pull zions-rfp-demo
   docker tag zions-rfp-demo gcr.io/mit-zions-rfp/zions-rfp-demo
   docker push gcr.io/mit-zions-rfp/zions-rfp-demo
   ```
3. deploying image from gcr to gke

    navigate to `terraform-gke/deployment` folder from the root and run belwo commands.

    ```
   terraform init
   terraform plan
   terraform apply
   ```
you can access the application from external IP given by image in GKE.

## you are all done happy coding :-)
## Thank You Very much !!!! !!
