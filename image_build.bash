#! /bin/bash

#Arguments:
#1: Dockerfile (i.e. "pcap2bin.Dockerfile")
#2: Name of the image, including the tag (i.e. "bip:simple-pipeline")
#3: environment the image is going to ("dev" or "prod")
#Will append the hash of the current commit to the image name (i.e. "c5aac90")
set -e # exit on errors

GIT_COMMIT=$(git log --pretty=format:'%h' -n 1)

ENV_NAME=$3

if [[ "$ENV_NAME" == "dev" ]]
then
    ECR=998806663306.dkr.ecr.us-east-1.amazonaws.com
elif [[ "$ENV_NAME" == "prod" ]]
then
    ECR=264535627462.dkr.ecr.us-east-1.amazonaws.com
fi

#login to our AWS ECR
"/mnt/c/Program Files/Amazon/AWSCLIV2/aws.exe" ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ECR

DOCKERFILE=$1
IMAGE=$2-$GIT_COMMIT
ECR_IMAGE="$ECR"/"$IMAGE"

docker build -f $DOCKERFILE -t $IMAGE .
docker tag $IMAGE $ECR_IMAGE
docker push $ECR_IMAGE

echo $ECR_IMAGE