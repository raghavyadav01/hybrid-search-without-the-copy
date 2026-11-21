#!/bin/bash
# Provision the EC2 benchmark: 3 identical SUT boxes (pinot, es, os) + 1 load-generator, one AZ, one SG.
# Writes ec2/hosts.env (public/private IPs) and ec2/setup.json (exact setup for the paper).
set -euo pipefail
cd "$(dirname "$0")"
REGION=${REGION:-us-west-2}
SUT_TYPE=${SUT_TYPE:-r7g.2xlarge}      # 8 vCPU Graviton3, 64 GiB
CLIENT_TYPE=${CLIENT_TYPE:-c7g.2xlarge} # 8 vCPU, 16 GiB
TAG=isir-bench
export AWS_REGION=$REGION AWS_PAGER=""

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-isir-bench-$ACCOUNT-$REGION}
AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --query Parameter.Value --output text)
AZ=$(aws ec2 describe-availability-zones --query 'AvailabilityZones[0].ZoneName' --output text)

aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null || \
  aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=$REGION >/dev/null

KEY=~/.ssh/$TAG.pem
if ! aws ec2 describe-key-pairs --key-names $TAG >/dev/null 2>&1; then
  aws ec2 create-key-pair --key-name $TAG --query KeyMaterial --output text > $KEY; chmod 600 $KEY
fi

VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$TAG Name=vpc-id,Values=$VPC \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
if [ "$SG" = "None" ] || [ -z "$SG" ]; then
  SG=$(aws ec2 create-security-group --group-name $TAG --description "$TAG" --vpc-id $VPC --query GroupId --output text)
  MYIP=$(curl -s https://checkip.amazonaws.com)
  aws ec2 authorize-security-group-ingress --group-id $SG --protocol tcp --port 22 --cidr $MYIP/32 >/dev/null
  aws ec2 authorize-security-group-ingress --group-id $SG --protocol -1 --source-group $SG >/dev/null
fi

# Instance role for S3 (Pinot reads the lake directly; ES/OS loaders read the same files)
if ! aws iam get-instance-profile --instance-profile-name $TAG >/dev/null 2>&1; then
  aws iam create-role --role-name $TAG --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam put-role-policy --role-name $TAG --policy-name s3 --policy-document \
    "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]}]}"
  aws iam create-instance-profile --instance-profile-name $TAG >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name $TAG --role-name $TAG
  sleep 10
fi

launch() { # name type disk_gb
  aws ec2 run-instances --image-id $AMI --instance-type $2 --key-name $TAG --security-group-ids $SG \
    --placement AvailabilityZone=$AZ --iam-instance-profile Name=$TAG \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$3,VolumeType=gp3,Iops=6000,Throughput=500}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG-$1},{Key=project,Value=$TAG}]" \
    --query 'Instances[0].InstanceId' --output text
}
PINOT_ID=$(launch pinot $SUT_TYPE 200); ES_ID=$(launch es $SUT_TYPE 200); OS_ID=$(launch os $SUT_TYPE 200)
CLIENT_ID=$(launch client $CLIENT_TYPE 60)
aws ec2 wait instance-running --instance-ids $PINOT_ID $ES_ID $OS_ID $CLIENT_ID

: > hosts.env
for n in PINOT ES OS CLIENT; do
  id=$(eval echo \$${n}_ID)
  set -- $(aws ec2 describe-instances --instance-ids $id \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
  echo "${n}_ID=$id ${n}_PUB=$1 ${n}_PRIV=$2" >> hosts.env
done
echo "BUCKET=$BUCKET REGION=$REGION KEY=$KEY" >> hosts.env
cat > setup.json <<J
{"region":"$REGION","az":"$AZ","ami":"$AMI","sut_instance":"$SUT_TYPE","client_instance":"$CLIENT_TYPE",
 "ebs":"gp3 200 GiB, 6000 IOPS, 500 MB/s","bucket":"$BUCKET","created":"$(date -u +%FT%TZ)"}
J
cat hosts.env
