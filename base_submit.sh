
cd /eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/$myDIR


source  /eagle/projects/radix-io/sockerman/spack/share/spack/setup-env.sh
spack env activate flowceptMofka

module use /soft/spack/gcc/0.6.1/install/modulefiles/Core
module load apptainer

#For compute nodes set proxy variable
export HTTP_PROXY=http://proxy.alcf.anl.gov:3128
export HTTPS_PROXY=http://proxy.alcf.anl.gov:3128
export http_proxy=http://proxy.alcf.anl.gov:3128
export https_proxy=http://proxy.alcf.anl.gov:3128
export MONGO_ENABLED=false
export HG_LOG_LEVEL=error
export FI_LOG_LEVEL=Trace

module use /soft/modulefiles 
module load cudatoolkit-standalone/12.2.2

target="/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/${myDIR}/examples/llm_complex"
export PYTHONPATH="${target}:$PYTHONPATH"
export FLOWCEPT_SETTINGS_PATH=/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/$myDIR/resources/multi_node_settings.yaml
# export FLOWCEPT_SETTINGS_PATH=/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/resources/multi_node_settings.yaml

total=$((4 * nodes))
readarray -t all_nodes < "$PBS_NODEFILE"
base_node=${all_nodes[0]}
dask_node=${all_nodes[1]}
echo $base_node > base_node.txt
echo $dask_node > dask_node.txt

tail -n +3 $PBS_NODEFILE > worker_nodefile.txt

# replace local host in setttings with the IP of the node running mq and inserts batch size
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node python3 mq_setup_params.py -f $targetFile.yaml -bs $batch_size
echo "Setup yaml file"


# if we are using mofka/kafka, replace localhost with node-IP with redis KV-store
if [[ "$mode" == *"mofka"* || "$mode" == *"kafka"* ]]; then
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node python3 kv_setup_params.py -f $targetFile.yaml -bs $batch_size
fi




# *************** Warmup run ***********************************************

# if redis is mq - use the base node for mq
if [[ "$mode" == *"redis"* ]]; then
mkdir -p $PWD/redisdata
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
fi

# if mofka/kafka is mq, use dask node for redis
if [[ "$mode" == *"mofka"* || "$mode" == *"kafka"* ]]; then
mkdir -p $PWD/redisdata
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
fi
sleep 3

# apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redisdata:/data redis.sif flowcept_redis --port 6379 --appendonly yes &


# if we are using mofka, launch the bedrock server
if [[ "$mode" == *"mofka"* ]]; then
    # launch server
    mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash resources/mofka/bedrock_setup.sh 2> bedrock.txt 1> bedrock.txt & 
    Bedrock_PID=$!
    
    # File to watch
    FLAG_FILE="flag.txt"

    # Wait until the file exists
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1  # Check every second
    done

    # Remove the file once detected
    rm "$FLAG_FILE"
    echo "Launched bedrocker server"
fi

# if we are using kafka, launch the kafka server
if [[ "$mode" == *"kafka"* ]]; then
    # launch server
    # mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash launch_app_kofka.sh & 
    bash launch_app_kofka.sh & 
    
    
    # File to watch
    FLAG_FILE="flag.txt"

    # Wait until the file exists
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1  # Check every second
    done

    # Remove the file once detected
    rm "$FLAG_FILE"
    sleep 10
    echo "Launched kofka server"
fi

# if we are using redis, launch the consumer 
# so it subscribes and then goes to sleep
if [[ "$mode" == *"redis"* ]]; then
    mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 redis_consumer.py 2> consumer.txt 1> consumer.txt &
    
    FLAG_FILE="sub.txt"
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1
    done
    rm $FLAG_FILE

    echo "Redis consumer online"
fi

# launch scheduler
rm cluster.info
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni bash -c 'export CUDA_VISIBLE_DEVICES=""; dask scheduler --scheduler-file cluster.info' 2> scheduler.txt 1> scheduler.txt &

# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts localhost --no-vni bash -c 'export CUDA_VISIBLE_DEVICES=""; dask scheduler --scheduler-file cluster.info' 2> scheduler.txt 1> scheduler.txt &


FLAG_FILE="cluster.info"
while [ ! -f "$FLAG_FILE" ]; do
    sleep 1
done
echo "Scheduler online"


mpiexec -n $total --ppn 4 --cpu-bind none --hostfile worker_nodefile.txt --no-vni bash -c 'source setGPU.sh; dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB'  1> worker.txt 2> worker.txt & 

# mpiexec -n 4 --ppn 4 --cpu-bind none --hosts localhost --no-vni bash -c 'source setGPU.sh; dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB'  1> worker.txt 2> worker.txt & 



echo "$total workers launched" 
sleep 30

# dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 

echo "Launching Client"
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 examples/llm_complex/llm_main_example.py --dask-map-gpus true --scheduler-file cluster.info --with-persistence false --workflow-params='{"input_data_dir": "/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/'$myDIR'/input_data/", "batch_size": 20, "eval_batch_size": 10, "emsize": [200, 400], "nhid": [200, 400], "nlayers": [2, 4, 8], "nhead": [2, 4], "dropout": [0.2], "lr": [0.1], "pos_encoding_max_len": [5000], "subset_size": 10, "epochs": 1, "max_runs": null, "delete_after_run": true, "random_seed": 0, "tokenizer_type": "basic_english"}'
sleep 5

# python3 examples/llm_complex/llm_main_example.py --dask-map-gpus true --scheduler-file cluster.info --with-persistence false --workflow-params='{"input_data_dir": "/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/'$myDIR'/input_data/", "batch_size": 20, "eval_batch_size": 10, "emsize": [200, 400], "nhid": [200, 400], "nlayers": [2, 4, 8], "nhead": [2, 4], "dropout": [0.2], "lr": [0.1], "pos_encoding_max_len": [5000], "subset_size": 10, "epochs": 10, "max_runs": null, "delete_after_run": true, "random_seed": 0, "tokenizer_type": "basic_english"}'


if [[ "$mode" == *"redis"* ]]; then
    touch redis_consumer_flag.txt
    
    FLAG_FILE="writeoutComplete.txt"
    
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1  # Check every second
    done

    rm redis_consumer_flag.txt
    rm $FLAG_FILE

fi 


# launch mofka consumer
if [[ "$mode" == *"mofka"* ]]; then
    echo "Launching mofka consumer"

    if [[ "$targetFile" == *"mofka_min_tel"* ]]; then
        mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 consumer.py 2> consumer.txt 1> consumer.txt
    fi
    
    if [[ "$targetFile" == *"mofka_max_tel"* ]]; then
        mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 batched_consumer.py 2> consumer.txt 1> consumer.txt

    fi
fi 

if [[ "$mode" == *"kafka"* ]]; then
    echo "Launching kafka consumer"
    mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 kafka_consumer.py 2> consumer.txt 1> consumer.txt
fi 


echo "Warmup complete"
if [[ "$mode" == *"mofka"* ]]; then
    kill -9 $(cat script.pid)
    kill -9 $Bedrock_PID
    pkill -9 bedrock
fi




# close out all apptainers (there are better ways to do this but it works)
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node bash stop_apps.sh 1> base_node_clear.txt 2> base_node_clear.txt
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node bash stop_apps.sh 1> dask_node_clear.txt 2> dask_node_clear.txt




rm *.csv
rm cluster.info
mv time.txt warmup_time.txt
rm data.json
sleep 30
rm bedrock.txt 






# *************** Test run ***************************
# if redis is mq - use the base node for mq
if [[ "$mode" == *"redis"* ]]; then
mkdir -p $PWD/redisdata
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
fi

# if mofka/kafka is mq, use dask node for redis
if [[ "$mode" == *"mofka"* || "$mode" == *"kafka"* ]]; then
mkdir -p $PWD/redisdata
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
fi
sleep 3

# apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redisdata:/data redis.sif flowcept_redis --port 6379 --appendonly yes &


# if we are using mofka, launch the bedrock server
if [[ "$mode" == *"mofka"* ]]; then
    # launch server
    mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash resources/mofka/bedrock_setup.sh 2> bedrock.txt 1> bedrock.txt & 
    Bedrock_PID=$!
    
    # File to watch
    FLAG_FILE="flag.txt"

    # Wait until the file exists
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1  # Check every second
    done

    # Remove the file once detected
    rm "$FLAG_FILE"
    echo "Launched bedrocker server"
fi

# if we are using kafka, launch the kafka server
if [[ "$mode" == *"kafka"* ]]; then
    # launch server
    # mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash launch_app_kofka.sh & 
    bash launch_app_kofka.sh & 
    
    
    # File to watch
    FLAG_FILE="flag.txt"

    # Wait until the file exists
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1  # Check every second
    done

    # Remove the file once detected
    rm "$FLAG_FILE"
    sleep 10
    echo "Launched kofka server"
fi

# if we are using redis, launch the consumer 
# so it subscribes and then goes to sleep
if [[ "$mode" == *"redis"* ]]; then
    mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 redis_consumer.py 2> consumer.txt 1> consumer.txt &
    
    FLAG_FILE="sub.txt"
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1
    done
    rm $FLAG_FILE

    echo "Redis consumer online"
fi









# launch scheduler
rm cluster.info
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni bash -c 'export CUDA_VISIBLE_DEVICES=""; dask scheduler --scheduler-file cluster.info' 2> scheduler.txt 1> scheduler.txt &


FLAG_FILE="cluster.info"
while [ ! -f "$FLAG_FILE" ]; do
    sleep 1
done
echo "Scheduler online"

mpiexec -n $total --ppn 4 --cpu-bind none --hostfile worker_nodefile.txt --no-vni bash -c 'source setGPU.sh; dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB'  1> worker.txt 2> worker.txt & 
echo "$total workers launched" 
sleep 30

# dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 

echo "Launching Client"
mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 examples/llm_complex/llm_main_example.py --dask-map-gpus true --scheduler-file cluster.info --with-persistence false --workflow-params='{"input_data_dir": "/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/'$myDIR'/input_data/", "batch_size": 20, "eval_batch_size": 10, "emsize": [200, 400], "nhid": [200, 400], "nlayers": [2, 4, 8], "nhead": [2, 4], "dropout": [0.2], "lr": [0.1], "pos_encoding_max_len": [5000], "subset_size": null, "epochs": 4, "max_runs": null, "delete_after_run": true, "random_seed": 0, "tokenizer_type": "basic_english"}'
sleep 5


if [[ "$mode" == *"redis"* ]]; then
    touch redis_consumer_flag.txt
    
    FLAG_FILE="writeoutComplete.txt"
    
    while [ ! -f "$FLAG_FILE" ]; do
        sleep 1  # Check every second
    done

    rm redis_consumer_flag.txt
    rm $FLAG_FILE

fi 


# launch mofka consumer
if [[ "$mode" == *"mofka"* ]]; then
    echo "Launching mofka consumer"
    # mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 consumer.py 2> consumer.txt 1> consumer.txt
    if [[ "$targetFile" == *"mofka_min_tel"* ]]; then
        mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 consumer.py 2> consumer.txt 1> consumer.txt
    fi
    
    if [[ "$targetFile" == *"mofka_max_tel"* ]]; then
        mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 batched_consumer.py 2> consumer.txt 1> consumer.txt

    fi
fi 

if [[ "$mode" == *"kafka"* ]]; then
    echo "Launching kafka consumer"
    mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $dask_node --no-vni python3 kafka_consumer.py 2> consumer.txt 1> consumer.txt
fi 

cp -r /tmp .

chmod -R g+w ./tmp


# ********************************* old stuff *********************************
# # launch redis container
# mkdir -p $PWD/redisdata
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
# sleep 3
# echo "Launched redis container"

# # apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
# # mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redisdata:/data redis.sif flowcept_redis --port 6379 --appendonly yes &



# # if we are using mofka, launch the bedrock server
# if [[ "$mode" == *"mofka"* ]]; then
#     # launch server
#     mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash resources/mofka/bedrock_setup.sh & 
#     Bedrock_PID=$!
    
#     # File to watch
#     FLAG_FILE="flag.txt"

#     # Wait until the file exists
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1  # Check every second
#     done

#     # Remove the file once detected
#     rm "$FLAG_FILE"
#     echo "Launched bedrocker server"
# fi

# # if we are using kafka, launch the kafka server

# if [[ "$mode" == *"kafka"* ]]; then
#     # launch server
#     # mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash launch_app_kofka.sh & 
#     bash launch_app_kofka.sh & 
    
    
#     # File to watch
#     FLAG_FILE="flag.txt"

#     # Wait until the file exists
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1  # Check every second
#     done

#     # Remove the file once detected
#     rm "$FLAG_FILE"
#     sleep 10
#     echo "Launched kofka server"
# fi

# # if we are using redis, launch the consumer 
# # so it subscribes and then goes to sleep
# if [[ "$mode" == *"redis"* ]]; then
#     mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni python3 redis_consumer.py 2> redis_consumer.txt 1> redis_consumer.txt &
    
#     FLAG_FILE="sub.txt"
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1
#     done
#     rm $FLAG_FILE

#     echo "Redis consumer online"
# fi

# # launch scheduler
# rm cluster.info
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni dask scheduler --scheduler-file cluster.info 2> scheduler.txt 1> scheduler.txt &


# FLAG_FILE="cluster.info"
# while [ ! -f "$FLAG_FILE" ]; do
#     sleep 1
# done
# echo "Scheduler online"


# mpiexec -n $total --ppn 4 --cpu-bind none --hostfile worker_nodefile.txt --no-vni dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 
# echo "$total workers launched" 
# sleep 30

# # dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 

# echo "Launching Client"
# python3 examples/llm_complex/llm_main_example.py --dask-map-gpus true --scheduler-file cluster.info --with-persistence false --workflow-params='{"input_data_dir": "/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/'$myDIR'/input_data/", "batch_size": 20, "eval_batch_size": 10, "emsize": [200, 400], "nhid": [200, 400], "nlayers": [2, 4, 8], "nhead": [2, 4], "dropout": [0.2], "lr": [0.1], "pos_encoding_max_len": [5000], "subset_size": null, "epochs": 4, "max_runs": null, "delete_after_run": true, "random_seed": 0, "tokenizer_type": "basic_english"}'
# sleep 5


# if [[ "$mode" == *"redis"* ]]; then
#     touch redis_consumer_flag.txt
    
#     FLAG_FILE="writeoutComplete.txt"
    
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1  # Check every second
#     done

#     rm redis_consumer_flag.txt
#     rm $FLAG_FILE

# fi 


# # launch mofka consumer
# if [[ "$mode" == *"mofka"* ]]; then
#     echo "Launching mofka consumer"
#     python3 consumer.py
# fi 

# if [[ "$mode" == *"kafka"* ]]; then
#     echo "Launching kafka consumer"
#     python3 kafka_consumer.py
# fi 


# # launch redis container
# mkdir -p $PWD/redisdata
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
# sleep 3
# echo "Launched redis container"

# # apptainer instance run -C -B redis.conf:/etc/redis/redis.conf -B redisdata:/data redis.sif flowcept_redis redis-server /etc/redis/redis.conf --port 6379 --appendonly yes
# # mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redisdata:/data redis.sif flowcept_redis --port 6379 --appendonly yes &



# # if we are using mofka, launch the bedrock server
# if [[ "$mode" == *"mofka"* ]]; then
#     # launch server
#     mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash resources/mofka/bedrock_setup.sh & 
#     Bedrock_PID=$!
    
#     # File to watch
#     FLAG_FILE="flag.txt"

#     # Wait until the file exists
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1  # Check every second
#     done

#     # Remove the file once detected
#     rm "$FLAG_FILE"
#     echo "Launched bedrocker server"
# fi

# # if we are using redis, launch the consumer 
# # so it subscribes and then goes to sleep

# if [[ "$mode" == *"redis"* ]]; then
#     mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni python3 redis_consumer.py 2> redis_consumer.txt 1> redis_consumer.txt &
    
#     FLAG_FILE="sub.txt"
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1
#     done
#     rm $FLAG_FILE

#     echo "Redis consumer online"
# fi

# # launch scheduler
# rm cluster.info
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni dask scheduler --scheduler-file cluster.info 2> scheduler.txt 1> scheduler.txt &


# FLAG_FILE="cluster.info"
# while [ ! -f "$FLAG_FILE" ]; do
#     sleep 1
# done
# echo "Scheduler online"


# mpiexec -n $total --ppn 4 --cpu-bind none --hostfile worker_nodefile.txt --no-vni dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 
# echo "$total workers launched" 
# sleep 30

# # dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 

# echo "Launching Client"
# python3 examples/llm_complex/llm_main_example.py --dask-map-gpus true --scheduler-file cluster.info --with-persistence false --workflow-params='{"input_data_dir": "/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/'$myDIR'/input_data/", "batch_size": 20, "eval_batch_size": 10, "emsize": [200, 400], "nhid": [200, 400], "nlayers": [2, 4, 8], "nhead": [2, 4], "dropout": [0.2], "lr": [0.1], "pos_encoding_max_len": [5000], "subset_size": null, "epochs": 4, "max_runs": null, "delete_after_run": true, "random_seed": 0, "tokenizer_type": "basic_english"}'
# sleep 5


# if [[ "$mode" == *"redis"* ]]; then
#     touch redis_consumer_flag.txt
    
#     FLAG_FILE="writeoutComplete.txt"
    
#     while [ ! -f "$FLAG_FILE" ]; do
#         sleep 1  # Check every second
#     done

#     rm redis_consumer_flag.txt
#     rm $FLAG_FILE

# fi 


# # launch mofka consumer
# if [[ "$mode" == *"mofka"* ]]; then
#     echo "Launching mofka consumer"
#     python3 consumer.py
# fi 


# # launch redis container
# mkdir -p $PWD/redisdata
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni apptainer instance run -C -B redisdata:/data redis.sif flowcept_redis --port 6379 --appendonly yes &
# sleep 3
# echo "Launched redis container"

# # launch bedrock server
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni bash resources/mofka/bedrock_setup.sh & 
# Bedrock_PID=$!

# # File to watch
# FLAG_FILE="flag.txt"

# # Wait until the file exists
# while [ ! -f "$FLAG_FILE" ]; do
#     sleep 1  # Check every second
# done

# # Remove the file once detected
# rm "$FLAG_FILE"
# echo "Launched bedrocker server"

# # launch scheduler
# rm cluster.info
# mpiexec -n 1 --ppn 1 --cpu-bind none --hosts $base_node --no-vni dask scheduler --scheduler-file cluster.info 2> scheduler.txt 1> scheduler.txt &

# FLAG_FILE="cluster.info"
# # Wait until the file exists
# while [ ! -f "$FLAG_FILE" ]; do
#     sleep 1  # Check every second
# done
# echo "Scheduler online"


# mpiexec -n $total --ppn 4 --cpu-bind none --hostfile worker_nodefile.txt --no-vni dask worker --scheduler-file cluster.info --nthreads 1 --memory-limit 512GB  1> worker.txt 2> worker.txt & 
# sleep 10
# echo "$total workers launched" 

# echo "Launching Client"
# python3 examples/llm_complex/llm_main_example.py --dask-map-gpus true --scheduler-file cluster.info --with-persistence false --workflow-params='{"input_data_dir": "/eagle/projects/radix-io/sockerman/flowcept_mofka/flowcept/'$myDIR'/input_data/", "batch_size": 20, "eval_batch_size": 10, "emsize": [200, 400], "nhid": [200, 400], "nlayers": [2, 4, 8], "nhead": [2, 4], "dropout": [0.2], "lr": [0.1], "pos_encoding_max_len": [5000], "subset_size": null, "epochs": 4, "max_runs": null, "delete_after_run": true, "random_seed": 0, "tokenizer_type": "basic_english"}'


# echo "Client closed"

# echo "launching consumer"
# python3 consumer.py

