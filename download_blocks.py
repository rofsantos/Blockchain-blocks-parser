import random
import csv
import time
import os
import tempfile
import shutil
import sys
from json import JSONEncoder

from datetime import datetime
from web3 import Web3

class HexBytesEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Web3.HexBytes):
            return obj.hex()
        return super().default(obj)

network = sys.argv[1]
#network = 'arb'
folder_path = f'{network}/'
tx_list = os.path.join(folder_path, f'transactions_list_{network}.csv')
tx_file = os.path.join(folder_path, f'transactions_{network}.csv')
last_fetched_block_file = os.path.join(folder_path, 'last_fetched_block.txt')
blocks_file = os.path.join(folder_path, f'blocks_{network}.csv')

file = open(f'rpc_endpoints_{network}.csv', "r")
rpc_endpoints = list(csv.reader(file, delimiter=","))
#print(rpc_endpoints)

def connect_to_rpc(endpoint):
    # Connect to the Ethereum RPC endpoint
    web3 = Web3(Web3.HTTPProvider(endpoint))
    return web3

def update_last_saved_block(number):
    # Save file into the corresponding network folder
    if os.path.exists(last_fetched_block_file):
        # Load the last fetched block number from the file
        with open(last_fetched_block_file, 'w') as file:
            file.write(str(number))
        print(f"Saved block number: {number}")

def convert_block_to_csv(obj):


    # Check if the file already exists
    file_exists = os.path.isfile(blocks_file)

    block_number = obj['number']
    print(f'Block {block_number}')
    #print(obj)

    # Convert timestamp to datetime format
    timestamp = obj['timestamp']
    dt = datetime.fromtimestamp(timestamp)

    # Convert block number to decimal
    number_value = obj['number']
    #block_number_decimal = int(str(number_value), 16)
    block_number_decimal = number_value

    # Create a new dictionary with additional columns
    new_obj = {
        'datetime': dt
        #,'blockNumberDecimal': block_number_decimal
    }
    new_obj.update(obj)  # Merge the new dictionary with the original object

    # Column Specific for Arbitrum network
    if network == 'arb':
        key_to_lookup = 'l1BlockNumber'
        if key_to_lookup in obj:
            # Convert L1 block number to decimal
            linumber_value = obj['l1BlockNumber']
            l1_block_number_decimal = int(str(linumber_value), 16)
            t_obj ={
                'l1BlockNumberDecimal' : l1_block_number_decimal
            }
        else:
            t_obj ={
                'l1BlockNumberDecimal' : 'NA'
            }

        new_obj.update(t_obj)

    # Extract the keys and values from the object
    keys = list(new_obj.keys())
    values = []
    for value in new_obj.values():
        if isinstance(value, bytes):
            # Convert HexBytes to a hexadecimal string
            values.append(value.hex())
        elif isinstance(value, list):
            # Convert list of HexBytes to a list of hexadecimal strings
            hex_values = [v.hex() for v in value]
            values.append(hex_values)
        else:
            values.append(value)
    

    if not os.path.exists(blocks_file):
        # Write the keys (column names) to the first line of the CSV file
        with open(blocks_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(keys)
    
    # Write the values to the second line of the CSV file
    with open(blocks_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(values)


    update_last_saved_block(block_number_decimal)

    print("Block data saved to CSV file")
 
def remove_value_from_csv(csv_file, value):
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    with open(csv_file, 'r') as file, temp_file:
        reader = csv.reader(file)
        writer = csv.writer(temp_file)
        removed = False

        for row in reader:
            if value not in row:
                writer.writerow(row)
            else:
                removed = True

    shutil.move(temp_file.name, csv_file)


def convert_transactions_to_csv(obj):

    # Column Specific for Arbitrum network
    #if network == 'arb':
        # Convert specific coulumn in arbitrum network

    # Extract the keys and values from the object
    new_obj = obj
    keys = list(obj.keys()) 
    values = []
    for value in obj.values():
        if isinstance(value, bytes):
            # Convert HexBytes to a hexadecimal string
            values.append(value.hex())
        elif isinstance(value, list):
            # Convert list of HexBytes to a list of hexadecimal strings
            hex_values = [v.hex() for v in value]
            values.append(hex_values)
        else:
            values.append(value)

    if not os.path.exists(tx_file):
        # Create the transactions csv file
        with open(tx_file, 'w') as file:
            file.write('')
        print("No transactions file found. Starting from the genesis block.")
        
        # Write the keys (column names) to the first line of the CSV file
        with open(tx_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(keys)


    # Write the values to the second line of the CSV file
    with open(tx_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(values)

    remove_value_from_csv(tx_list, new_obj['hash'].hex())

    print("Transaction data saved to CSV file")

def download_missing_transactions(web3):
    data = []

    if not os.path.exists(tx_list):
        # Create the transactions csv file
        with open(tx_list, 'w') as file:
            file.write('')
        print("No transactions list found. Starting from the genesis block.")

    with open(tx_list, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            data.append(row[0])

    for tx in data:
        print(f"Downloading transaction hash: {tx}")
        tx_info = web3.eth.get_transaction(tx)
        convert_transactions_to_csv(tx_info)


def download_transactions(txs, web3):
    hex_values = [v.hex() for v in txs]

    if not os.path.exists(tx_list):
        # Create the transactions csv file
        with open(tx_list, 'w') as file:
            file.write('')
        print("No transactions list found. Starting from the genesis block.")

    # Write the values to the second line of the CSV file
    with open(tx_list, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(hex_values)

    for tx in hex_values:
        #print(f"Downloading transaction hash: {tx}")
        tx_info = web3.eth.get_transaction(tx)

        convert_transactions_to_csv(tx_info)

def download_blocks(endpoint_file):
    rpc_endpoints = []
    with open(endpoint_file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            rpc_endpoints.append(row[0])

    connected = False

    # Check if the last_fetched_block file exists

    if os.path.exists(last_fetched_block_file):
        # Load the last fetched block number from the file
        with open(last_fetched_block_file, 'r') as file:
            last_fetched_block = int(file.read()) + 1
        print(f"Resuming from block number: {last_fetched_block}")
    else:
        # Create the last_fetched_block file and set it to -1
        last_fetched_block = -1
        with open(last_fetched_block_file, 'w') as file:
            file.write('-1')
        print("No existing blocks found. Starting from the genesis block.")

    while True:
        # Shuffle the list of RPC endpoints
        random.shuffle(rpc_endpoints)

        for endpoint in rpc_endpoints:
            try:
                print(f"Connecting to endpoint: {endpoint}")
                # Connect to the RPC endpoint
                web3 = connect_to_rpc(endpoint.strip())  # Remove leading/trailing whitespaces
                connected = True

                # Get the latest block number
                latest_block_number = web3.eth.block_number
                print(f"Latest block number: {latest_block_number}")

                block_number = last_fetched_block + 1
                counter = 0
                start = time.time()
                timesArr = []

                # Download blocks from the last successful block to the latest block
                while block_number < latest_block_number:
                    try:

                        #Downloading missing transactions from last run...
                        download_missing_transactions(web3)

                        print(f"Downloading block number: {block_number}")
                        block = web3.eth.get_block(block_number)

                        # Check if the block is not None before saving
                        if block is not None:
                            if not os.path.exists(folder_path):
                                os.makedirs(folder_path)

                            convert_block_to_csv(block)
                            last_fetched_block = block_number

                            if block['transactions']:
                                download_transactions(block['transactions'], web3)
                        counter += 1
                        if time.time() - start > 60:
                            timesArr.append(counter)
                            avg = round(sum(timesArr) / len(timesArr))
                            print('###################### AVG PERFORMANCE ######################')
                            print('{} blocks/m'.format(avg))
                            start = time.time()
                            counter = 0


                    except Exception as e:
                        print(f"Block Error: {e}")
                        break

                    block_number = block_number + 1
                    #time.sleep(1)  # Add a delay of 1 second

                print("Block data saved to CSV files in the 'blocks' folder")

                # Connection successful, break the loop
                break

            except Exception as e:
                print(f"Connection error: {e}")
                connected = False

        if not connected:
            print("Connection lost. Retrying with another endpoint...")

# Call the function to start downloading blocks
download_blocks(f'rpc_endpoints_{network}.csv')