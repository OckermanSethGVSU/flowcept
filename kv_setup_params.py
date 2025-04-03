import socket
import argparse

def replace_word_in_file(input_filename, output_filename, target_word, replacement_word):
    with open(input_filename, 'r', encoding='utf-8') as file:
        content = file.read()
    
    updated_content = content.replace(target_word, replacement_word)

    with open(output_filename, 'w', encoding='utf-8') as file:
        file.write(updated_content)




parser = argparse.ArgumentParser(description="Process a target filename.")
parser.add_argument("-f", "--filename", type=str, help="Path to the target file")
parser.add_argument("-bs", "--batch-size", type=str, help="Path to the target file")
args = parser.parse_args()

target_file = args.filename
bs = args.batch_size
hostname = socket.gethostname()
ip_address = socket.gethostbyname(hostname)

replace_word_in_file(f"resources/multi_node_settings.yaml", "resources/multi_node_settings.yaml","KVlocalhost", f"{ip_address}")

with open("dask_node_ip.txt", "w") as f:
    f.write(f"{ip_address}")
