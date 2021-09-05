import json
import time
from dataclasses import dataclass
from typing import Optional

import boto3
import botocore.exceptions
import psycopg2
from psycopg2.extensions import connection
from mypy_boto3_ec2 import EC2ServiceResource
from mypy_boto3_iam import IAMClient
from mypy_boto3_redshift import RedshiftClient
from mypy_boto3_redshift.type_defs import ClusterTypeDef

from common import EtlConfig
from common import get_static_config_instance


@dataclass
class RedshiftClusterMetadata:
    endpoint: str
    role_arn: str
    vpc_id: str


def prepare_iam_role_arn(iam_client: IAMClient, iam_role_name: str) -> str:
    try:
        iam_client.create_role(
            Path='/',
            RoleName=iam_role_name,
            Description='Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [
                    {
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'redshift.amazonaws.com'
                        }
                    }
                ],
                'Version': '2012-10-17'
            })
        )
    except iam_client.exceptions.EntityAlreadyExistsException as e:
        print(f'Role with name {iam_role_name} already exists. Continue next steps with expected exception = {e}')

    iam_client.attach_role_policy(
        RoleName=iam_role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )

    return iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']


def spin_up_and_wait_redshift_cluster(
        redshift_client: RedshiftClient,
        etl_config: EtlConfig,
        iam_role_arn: str
) -> RedshiftClusterMetadata:
    redshift_client.create_cluster(
        ClusterType=etl_config.redshift_cluster.cluster_type,
        NodeType=etl_config.redshift_cluster.node_type,
        NumberOfNodes=etl_config.redshift_cluster.num_nodes,

        DBName=etl_config.redshift_cluster.db_name,
        ClusterIdentifier=etl_config.redshift_cluster.cluster_identifier,
        MasterUsername=etl_config.redshift_cluster.db_user,
        MasterUserPassword=etl_config.redshift_cluster.db_password,

        IamRoles=[iam_role_arn]
    )

    def get_available_cluster_props() -> ClusterTypeDef:
        cp: ClusterTypeDef = redshift_client.describe_clusters(
            ClusterIdentifier=etl_config.redshift_cluster.cluster_identifier
        )['Clusters'][0]

        cluster_status = cp['ClusterStatus']

        if cluster_status.lower() == 'available':
            return cp
        else:
            identifier = etl_config.redshift_cluster.cluster_identifier
            print(f"Redshift cluster with identifier = {identifier} is {cluster_status}. Let's wait for 5 seconds.")
            time.sleep(5)
            return get_available_cluster_props()

    cluster_props: ClusterTypeDef = get_available_cluster_props()

    return RedshiftClusterMetadata(
        endpoint=cluster_props['Endpoint']['Address'],
        role_arn=cluster_props['IamRoles'][0]['IamRoleArn'],
        vpc_id=cluster_props['VpcId']
    )


def open_and_verify_tcp_connection(
        ec2_resource: EC2ServiceResource,
        etl_config: EtlConfig,
        cluster_metadata: RedshiftClusterMetadata
) -> None:
    vpc = ec2_resource.Vpc(id=cluster_metadata.vpc_id)
    default_security_group = list(vpc.security_groups.all())[0]
    print(default_security_group)

    try:
        default_security_group.authorize_ingress(
            GroupName=default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=etl_config.redshift_cluster.db_port,
            ToPort=etl_config.redshift_cluster.db_port
        )
    except botocore.exceptions.ClientError as e:
        # It is possible that ingress rule already exists.
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            print(e.response['Error']['Message'])
        else:
            raise e

    conn: Optional[connection] = None
    try:
        conn_string = 'host={endpoint} dbname={db_name} user={db_user} password={db_password} port={db_port}'.format(
            db_user=etl_config.redshift_cluster.db_user,
            db_password=etl_config.redshift_cluster.db_password,
            endpoint=cluster_metadata.endpoint,
            db_port=etl_config.redshift_cluster.db_port,
            db_name=etl_config.redshift_cluster.db_name
        )
        conn = psycopg2.connect(conn_string)
        print(
            f'Successfully create connection to DB with endpoint = {cluster_metadata.endpoint}, ' +
            f'name = {etl_config.redshift_cluster.db_name}, '
            f'port = {etl_config.redshift_cluster.db_port} and ' +
            f'user = {etl_config.redshift_cluster.db_user}'
        )
    finally:
        if conn is not None:
            conn.close()
            print('Successfully close connection to DB')


def main() -> None:
    etl_config: EtlConfig = get_static_config_instance()
    iam_client: IAMClient = boto3.client('iam')
    redshift_client: RedshiftClient = boto3.client('redshift')
    ec2_resource: EC2ServiceResource = boto3.resource('ec2')

    iam_role_arn = prepare_iam_role_arn(
        iam_client=iam_client,
        iam_role_name=etl_config.redshift_cluster.iam_role_name
    )

    start_epoch = time.time()
    redshift_cluster_metadata = spin_up_and_wait_redshift_cluster(
        redshift_client,
        etl_config=etl_config,
        iam_role_arn=iam_role_arn
    )
    end_epoch = time.time()
    print(f'Redshift cluster preparation took {int(end_epoch - start_epoch)} seconds.')

    open_and_verify_tcp_connection(
        ec2_resource,
        etl_config=etl_config,
        cluster_metadata=redshift_cluster_metadata
    )
    print(f'Successfully create cluster with metadata = {redshift_cluster_metadata}')


if __name__ == '__main__':
    main()
