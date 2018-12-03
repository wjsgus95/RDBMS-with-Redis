import subprocess

redis_dir = 'redis'
cluster_dir = f'{redis_dir}/utils/create-cluster'

subprocess.run([f"./{cluster_dir}/create-cluster", "stop"])
subprocess.run([f"./{cluster_dir}/create-cluster", "clean"])
