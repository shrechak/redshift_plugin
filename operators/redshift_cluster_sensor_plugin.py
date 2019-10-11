import logging

import boto3
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook


class RedshiftClusterStatusSensor(BaseSensorOperator):
    """
    Check if the cluster is in `available` status to be able to run queries
    :param cluster_id:              The redshift cluster id.
    :type cluster_id:               string
    :param region_name:             The AWS region in which the cluster resides.
    :type redshift_schema:          string
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#daf7a6'

    @apply_defaults
    def __init__(self,
                 cluster_id,
                 region_name='eu-central-1',
                 aws_conn_id=None,
                 *args, **kwargs):
        self.cluster_id = cluster_id
        self.region_name = region_name
        self.aws_conn_id = aws_conn_id
        super(RedshiftClusterStatusSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        conn = BaseHook.get_connection(self.aws_conn_id)
        redshift_client = boto3.client('redshift',
                                       region_name=self.region_name,
                                       aws_access_key_id=conn.login,
                                       aws_secret_access_key=conn.password)
        response = redshift_client.describe_clusters(
            ClusterIdentifier=self.cluster_id)

        status = response['Clusters'][0]['ClusterStatus']

        if status != 'available':
            message = ("The redshift cluster {cluster_id} "
                       "is not available for running queries!!")
            logging.info(message.format(cluster_id=self.cluster_id))
            return False

        return True


class RedshiftClusterStatusPlugin(AirflowPlugin):
    name = "redshift_cluster_status_sensor"
    operators = [RedshiftClusterStatusSensor]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []