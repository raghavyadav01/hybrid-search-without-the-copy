#!/bin/bash
# Camera-ready runs (2026-11-21): two identical Pinot boxes (one full replica each) + one load generator.
# Same instance types as the 1.2M-product runs in the paper (r7g.2xlarge SUT, c7g.8xlarge mpbench client).
# Writes ec2/hostsb.env. Every resource is tagged Owner/CreatedBy/Project/Purpose/Expiry.
set -euo pipefail
cd "$(dirname "$0")"
REGION=${REGION:-us-west-2}
SUT_TYPE=${SUT_TYPE:-r7g.2xlarge}
CLIENT_TYPE=${CLIENT_TYPE:-c7g.8xlarge}
TAG=isir-bench
export AWS_PROFILE=${AWS_PROFILE:-isir-bench} AWS_REGION=$REGION AWS_PAGER=""
TAGS="{Key=Owner,Value=${OWNER:-me}},{Key=CreatedBy,Value=claude-code},{Key=Project,Value=isir-camera-ready},{Key=Purpose,Value=ISIR-eCom camera-ready reruns},{Key=Expiry,Value=2026-11-27}"
TAGS_CLI="Key=Owner,Value=${OWNER:-me} Key=CreatedBy,Value=claude-code Key=Project,Value=isir-camera-ready Key=Purpose,Value=ISIR-eCom-camera-ready-reruns Key=Expiry,Value=2026-11-27"

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
[ -z "${EXPECT_ACCOUNT:-}" ] || [ "$ACCOUNT" = "$EXPECT_ACCOUNT" ] || { echo "wrong account $ACCOUNT"; exit 1; }
BUCKET=${BUCKET:-isir-bench-$ACCOUNT-$REGION}
AMI=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --query Parameter.Value --output text)
AZ=${AZ:-${REGION}a}

if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint=$REGION >/dev/null
  aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging \
    "TagSet=[{Key=Owner,Value=${OWNER:-me}},{Key=CreatedBy,Value=claude-code},{Key=Project,Value=isir-camera-ready},{Key=Purpose,Value=ISIR-eCom camera-ready lake},{Key=Expiry,Value=2026-11-27}]"
fi

KEY=~/.ssh/$TAG.pem
if ! aws ec2 describe-key-pairs --key-names $TAG >/dev/null 2>&1; then
  aws ec2 create-key-pair --key-name $TAG --tag-specifications "ResourceType=key-pair,Tags=[$TAGS]" \
    --query KeyMaterial --output text > $KEY.new
  mv $KEY.new $KEY; chmod 600 $KEY
fi

VPC=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=$TAG Name=vpc-id,Values=$VPC \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
if [ "$SG" = "None" ] || [ -z "$SG" ]; then
  SG=$(aws ec2 create-security-group --group-name $TAG --description "$TAG" --vpc-id $VPC \
    --tag-specifications "ResourceType=security-group,Tags=[$TAGS]" --query GroupId --output text)
  MYIP=$(curl -s https://checkip.amazonaws.com)
  aws ec2 authorize-security-group-ingress --group-id $SG --protocol tcp --port 22 --cidr $MYIP/32 >/dev/null
  aws ec2 authorize-security-group-ingress --group-id $SG --protocol -1 --source-group $SG >/dev/null
fi

if ! aws iam get-instance-profile --instance-profile-name $TAG >/dev/null 2>&1; then
  aws iam create-role --role-name $TAG --tags $TAGS_CLI --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam put-role-policy --role-name $TAG --policy-name s3 --policy-document \
    "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:*\",\"Resource\":[\"arn:aws:s3:::$BUCKET\",\"arn:aws:s3:::$BUCKET/*\"]},{\"Effect\":\"Allow\",\"Action\":\"s3:ListAllMyBuckets\",\"Resource\":\"*\"}]}"
  aws iam create-instance-profile --instance-profile-name $TAG --tags $TAGS_CLI >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name $TAG --role-name $TAG
  sleep 15
fi

launch() { # name type disk_gb
  aws ec2 run-instances --image-id $AMI --instance-type $2 --key-name $TAG --security-group-ids $SG \
    --placement AvailabilityZone=$AZ --iam-instance-profile Name=$TAG \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$3,VolumeType=gp3,Iops=6000,Throughput=500}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$TAG-$1},$TAGS]" "ResourceType=volume,Tags=[{Key=Name,Value=$TAG-$1},$TAGS]" \
    --query 'Instances[0].InstanceId' --output text
}
P1_ID=$(launch pinot1 $SUT_TYPE 200); P2_ID=$(launch pinot2 $SUT_TYPE 200); CLIENT_ID=$(launch client $CLIENT_TYPE 60)
aws ec2 wait instance-running --instance-ids $P1_ID $P2_ID $CLIENT_ID

: > hostsb.env
for n in P1 P2 CLIENT; do
  id=$(eval echo \$${n}_ID)
  set -- $(aws ec2 describe-instances --instance-ids $id \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
  echo "${n}_ID=$id ${n}_PUB=$1 ${n}_PRIV=$2" >> hostsb.env
done
echo "BUCKET=$BUCKET REGION=$REGION KEY=$KEY" >> hostsb.env
cat hostsb.env
