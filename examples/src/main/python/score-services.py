import sys, os, time
import aztk.spark
from aztk.error import AztkError
import datetime




#UPDATE this section to set your secrets 
secrets_confg = aztk.spark.models.SecretsConfiguration(
    service_principal=aztk.spark.models.ServicePrincipalConfiguration(
        tenant_id="ed35e03c-6f6e-4fd4-8367-e30e5fd6dd38",    #"Update with AAD Directory ID",
        client_id="fbbb5259-f89e-4d01-ad38-73c78180520e",    #"Update with App Registration ID",
        credential="k.:b3X:OE/I0hXGp4vBA5SnAO_:MtW8:",                           #"Update with AAD app password",
        batch_account_resource_id="/subscriptions/f0aacc32-5eb2-4eae-a9b2-b510094e20f3/resourceGroups/cloud-shell-storage-eastus/providers/Microsoft.Batch/batchAccounts/test333",               #"Update with batch account resource Id",
        storage_account_resource_id="/subscriptions/f0aacc32-5eb2-4eae-a9b2-b510094e20f3/resourceGroups/cloud-shell-storage-eastus/providers/Microsoft.Storage/storageAccounts/cs210033fff9c7dc349",
    ),
    #Optional ssh_pub_key
    #ssh_pub_key="../../rsa_key.pub" 
)

#UPDATE this line to set path to root of repository to reference files
ROOT_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__)))

print(ROOT_PATH)

# create a spark client
client = aztk.spark.Client(secrets_confg)

# list available clusters
# clusters = client.list_clusters()
clusters = client.cluster.list()

# define a custom script
# custom_script = aztk.spark.models.CustomScript(
# custom_script = aztk.spark.models.ClusterConfiguration(
#     name="pythondependencies.sh",
#     script=os.path.join(ROOT_PATH, 'custom-scripts', 'pythondependencies.sh'),
#     run_on="all-nodes")

# define spark configuration
spark_conf = aztk.spark.models.SparkConfiguration(
    spark_defaults_conf=os.path.join(ROOT_PATH, '.aztk', 'spark-defaults.conf'),
    spark_env_sh=os.path.join(ROOT_PATH, '.aztk', 'spark-env.sh'),
    core_site_xml=os.path.join(ROOT_PATH, '.aztk', 'core-site.xml'),
    jars=[os.path.join(ROOT_PATH,  '.aztk', 'jars', jar) 
        for jar in os.listdir(os.path.join(ROOT_PATH, '.aztk', 'jars'))
    ]
)

userconfig = aztk.spark.models.UserConfiguration(
     username="spark",
     ssh_key="~/.ssh/id_rsa.pub" ,
     password="spark"
)

toolkit=aztk.models.Toolkit(software="spark", version="2.3.0") 

# configure my cluster
# cluster_config = aztk.spark.models.ClusterConfiguration(
#     cluster_id="myfirstcluster", #Cluster should not include capital letters
#     size = 4,    
#     vm_size="Standard_E4s_v3", #standard_g2 4cpu, 56Gib,  Standard_E8s_v3 : 8cores, 64Gb
#     custom_scripts=[custom_script],
#     spark_configuration=spark_conf,
#     user_configuration=userconfig,
#     toolkit=toolkit
# )

cluster_config = aztk.spark.models.ClusterConfiguration(
    cluster_id="myfirstcluster", #Cluster should not include capital letters
    size = 4,    
    vm_size="Standard_E4s_v3", #standard_g2 4cpu, 56Gib,  Standard_E8s_v3 : 8cores, 64Gb
    spark_configuration=spark_conf,
    user_configuration=userconfig,
    toolkit=toolkit
)

# create a cluster, and wait until it is ready
try:
    print("Cluster configured creating cluster now :", datetime.datetime.now())
    # cluster = client.create_cluster(cluster_config)

    import ipdb; ipdb.set_trace()
    # No such file or directory: './.aztk/config/spark-env.sh
    cluster = client.cluster.create(cluster_config)
    
    # cluster = client.get_cluster(cluster_config.cluster_id)
    cluster = client.cluster.get(cluster_config.cluster_id)
    print("getting cluster for", cluster_config.cluster_id)
    
    # The cluster will take 5 to 10 minutes to be ready depending on the size and availability in the batch account
    # cluster = client.wait_until_cluster_is_ready(cluster.id)
    # cluster = client.cluster.wait(cluster.id, )
except AztkError as e:
    print(e)
    sys.exit()

print("Cluser is ready now:", datetime.datetime.now())

# create a user for the cluster if you need to connect to it
#client.create_user(cluster.id, "userone", "chooseyourpassword")

# create the app to run
app1 = aztk.spark.models.ApplicationConfiguration(
    name="scoring",
    application= os.path.join(ROOT_PATH, 'main_scoring.py'),
    driver_memory = "30g",
    executor_memory = "30g", #the memory you think your app needs. make sure that each node has at least that amount of memory 
    py_files =['utils.py', 'datastorage.py'] #Update this list if your main app has dependencies on other modules
)

# submit an app and wait until it is finished running
client.submit(cluster.id, app1)
print("Submitted app. Waiting ...")

#wait
client.wait_until_application_done(cluster.id, app1.name)

# get logs for app, print to console
#app1_logs1 = client.get_application_log(cluster_id=cluster_config.cluster_id, application_name=app1.name)
#print(app1_logs1.log)

print("Done waiting time now is:", datetime.datetime.now())
# get status of app
status = client.get_application_status(cluster_config.cluster_id, app1.name)

# stream logs of app, print to console as it runs
current_bytes = 0
while True:
    app1_logs = client.get_application_log(
        cluster_id=cluster_config.cluster_id,
        application_name=app1.name,
        tail=True,
        current_bytes=current_bytes)

    print(app1_logs.log, end="")

    if app1_logs.application_state == 'completed':
        break
    current_bytes = app1_logs.total_bytes
    time.sleep(1)

# wait until all jobs finish, then delete the cluster
client.wait_until_applications_done(cluster.id)

print("All jobs finished let's delete the cluster")
client.delete_cluster(cluster.id)
