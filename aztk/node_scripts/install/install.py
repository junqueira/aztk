import os
from core import config
from install import pick_master, spark, scripts, create_user, plugins, spark_container
import wait_until_master_selected
from aztk.models.plugins import PluginTarget
from aztk.internal.cluster_data import ClusterData

def read_cluster_config():
    data = ClusterData(config.blob_client, config.pool_id)
    config = data.read_cluster_config()
    print("Got cluster config", config)
    return config

def setup_host(docker_repo: str):
    """
    Code to be run on the node(NOT in a container)
    """
    client = config.batch_client

    create_user.create_user(batch_client=client)
    if os.environ['AZ_BATCH_NODE_IS_DEDICATED'] == "true" or os.environ['AZTK_MIXED_MODE'] == "False":
        is_master = pick_master.find_master(client)
    else:
        is_master = False
        wait_until_master_selected.main()

    is_worker = not is_master or os.environ["AZTK_WORKER_ON_MASTER"]
    master_node_id = pick_master.get_master_node_id(config.batch_client.pool.get(config.pool_id))
    master_node = config.batch_client.compute_node.get(config.pool_id, master_node_id)

    if is_master:
        os.environ["AZTK_IS_MASTER"] = "1"
    if is_worker:
        os.environ["AZTK_IS_WORKER"] = "1"

    os.environ["AZTK_MASTER_IP"] = master_node.ip_address

    cluster_conf = read_cluster_config()

    spark_container.start_spark_container(
        docker_repo=docker_repo,
        gpu_enabled=os.environ.get("AZTK_GPU_ENABLED") == "true",
        plugins=cluster_conf.plugins,
    )
    plugins.setup_plugins(target=PluginTarget.Host, is_master=is_master, is_worker=is_worker)


def setup_spark_container():
    """
    Code run in the main spark container
    """
    is_master = os.environ["AZTK_IS_MASTER"]
    is_worker = os.environ["AZTK_IS_WORKER"]
    print("Setting spark container. Master: ", is_master, ", Worker: ", is_worker)

    print("Copying spark setup config")
    spark.setup_conf()
    print("Done copying spark setup config")

    master_node_id = pick_master.get_master_node_id(config.batch_client.pool.get(config.pool_id))
    master_node = config.batch_client.compute_node.get(config.pool_id, master_node_id)

    spark.setup_connection()

    if is_master:
        spark.start_spark_master()

    if is_worker:
        spark.start_spark_worker()

    plugins.setup_plugins(target=PluginTarget.SparkContainer, is_master=is_master, is_worker=is_worker)
    scripts.run_custom_scripts(is_master=is_master, is_worker=is_worker)

    open("/tmp/setup_complete", 'a').close()