

nodes=(1)
gpus=(4)


# ["mofka", "redis", "kafka"]
# modes=("redis")
# modes=("redis" "kafka" "mofka")
modes=("kafka")
# modes=("mofka")

# batchSizes=(128)
batchSizes=(512)

# [max_tel, min_tel, small_message]
baseYaml=("min_tel")
# baseYaml=("max_tel")



for by in "${baseYaml[@]}"
do
    for bs in "${batchSizes[@]}"
    do
        for m in "${modes[@]}"
        do
            for num_nodes in "${nodes[@]}"
            do
                for num_gpus in "${gpus[@]}"
                do  
                    # echo $m,$num_nodes,$num_gpus
                    
                    total_nodes=$((num_nodes + 2))
                        # p_profile=false
                    DATE=$(date +"%Y-%m-%d_%H_%M_%S")
                    target_file="${m}_${num_nodes}_nodes.sh"
                    
                    
                    echo "#!/bin/bash" >> $target_file
                    echo "#PBS -l select=${total_nodes}:system=polaris" >> $target_file
                    echo "#PBS -l place=scatter" >> $target_file
                    echo "#PBS -l filesystems=home:eagle" >> $target_file

                    
                    # echo "#PBS -l walltime=00:05:00" >> $target_file
                    # echo "#PBS -l walltime=00:30:00" >> $target_file
                    # echo "#PBS -l walltime=01:00:00" >> $target_file
                    # echo "#PBS -l walltime=02:15:00" >> $target_file
                    # echo "#PBS -l walltime=03:00:00" >> $target_file
                    # echo "#PBS -l walltime=01:00:00" >> $target_file
                    # echo "#PBS -l walltime=02:30:00" >> $target_file
                    echo "#PBS -l walltime=00:30:00" >> $target_file
                    # echo "#PBS -l walltime=00:20:00" >> $target_file
                    # echo "#PBS -l walltime=00:15:00" >> $target_file
                    # echo "#PBS -l walltime=00:10:00" >> $target_file
                    
                    # echo "#PBS -q debug" >> $target_file
                    echo "#PBS -q debug-scaling" >> $target_file
                    # echo "#PBS -q prod" >> $target_file
                    # echo "#PBS -q preemptable" >> $target_file
                        
                    # echo "#PBS -q run_next" >> $target_file
                    

                    echo "#PBS -A recup" >> $target_file
                    echo "#PBS -o main.out" >> $target_file
                    echo "#PBS -e main.out" >> $target_file


                    echo "nodes=${num_nodes}" >> $target_file
                    echo "targetFile=${m}_${by}" >> $target_file
                    echo "batch_size=${bs}" >> $target_file
                    echo "mode=${m}" >> $target_file
                
                
                    dir="${m}_${num_nodes}_nodes_bs_${bs}_${by}_${DATE}"
                    echo "myDIR=${dir}" >> $target_file
                    
                    cat base_submit.sh >> $target_file
                    mkdir -p $dir


                    cp -r input_data/ $dir/
                    cp -r examples/ $dir/
                    cp -r resources/ $dir/
                    cp -r src/ $dir/
                    cp -r *.sif $dir/
                    cp launch_app_kofka.sh $dir/
                    cp mq_setup_params.py $dir/
                    cp kv_setup_params.py $dir/
                    cp consumer.py $dir/
                    cp batched_consumer.py $dir/
                    cp redis_consumer.py $dir/
                    cp kafka_consumer.py $dir/
                    cp redis.conf $dir/
                    cp stop_apps.sh $dir/
                    cp setGPU.sh $dir
                    mv $target_file $dir

                    chmod -R g+w $dir
                    cd $dir
             


                    echo "qsub $target_file"
                    jobid=$(qsub $target_file)
                    jobid_clean=${jobid%%.*}  # Remove everything after the first dot
                    sleep 2
                    cd .. 
                    echo $jobid_clean >> job_ids.txt
                done
            done
        done
    done
done
