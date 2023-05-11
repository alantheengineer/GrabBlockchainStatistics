#import generic libraries
import os
import pandas as pd
import time
#import database utilities
from sqlalchemy import create_engine
# import blockchain specific libraries
## solana
import asyncio
from solana.rpc.async_api import AsyncClient
# using the Infura node service to access the ethereum blockchain data
# this can be signed up for on their website, small projects are free
# https://infura.io/dashboard
ETH_KEY_FILE=os.path.join(os.getcwd(),'INFURA_PVT_KEY.txt')
def set_eth_env_key():
    with open(ETH_KEY_FILE,'r') as file:
        lines = [line.rstrip() for line in file]
    print('setting Infura project ID')
    os.environ['WEB3_INFURA_PROJECT_ID']=lines[0]
## ethereum
set_eth_env_key()
from web3.auto.infura import w3 as eth_chain
## fantom
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware

TIME_INTERVAL=20 #mins
SLEEP_TIME=TIME_INTERVAL*60
DB_FILE_NAME="sqlite:///blockchain_stats.db"
# for now grabing this data (and one more but using this list for ease of processing)
ETH_DATA_COLS=['gasUsed', 'number', 'size', 'hash', 'parentHash', 'timestamp']
# Fantom network setup data
FTM_NETWORK_NODE_KEY=os.path.join(os.getcwd(),'FTM_NODE_KEY.txt')
PLYGN_NETWORK_NODE_KEY=os.path.join(os.getcwd(),'PLYGN_NODE_KEY.txt')
ALGO_NETWORK_NODE_KEY=os.path.join(os.getcwd(),'ALGO_NODE_KEY.txt')

# You will need some blockchain nodes...
FTM_NODE_URL="your fantom node url here..."
PLYGN_NODE_URL="your polygon node url here..."
ALGO_NODE_URL="your algorand node url here..."

def Get_node_key(filename):
    with open(filename,'r') as file:
        lines = [line.rstrip() for line in file]
    print('node address: {}'.format(lines[0]))
    return lines[0]

def ftm_connection():
    print('Creating Fantom network connection...')
    return Web3(HTTPProvider(FTM_NODE_URL))

def polygon_connection():
    print('Creating Fantom network connection...')
    plygn = Web3(HTTPProvider(PLYGN_NODE_URL))
    plygn.middleware_onion.inject(geth_poa_middleware, layer=0)
    return plygn

def algorand_connection():
    print('Creating Fantom network connection...')
    return Web3(HTTPProvider(ALGO_NODE_URL))

def create_db_instance():
    print('creating database instance at: {}'.format(DB_FILE_NAME))
    return create_engine(DB_FILE_NAME, echo=False, future=False)

def add_data_to_table(engine, data, table_name):
    df=pd.DataFrame.from_dict([data])
    print('adding data to: {}'.format(table_name))
    df.to_sql(table_name,engine, index=False,if_exists='append')

# this is quite crude, I am just going to get the latest block and pull
# some basic data for now then grab the previous block and calculate a 
# rudimentary tps from that - at least it is an "actual" figure rather than 
# a theoretical value!
def create_eth_data_list(data):
    d = {var: data[var] for var in ETH_DATA_COLS}
    # add transactions from length of list 
    d['transactions'] = int(len(data['transactions']))
    return d

def get_eth_data(engine,client,name):
    print('getting {} data...'.format(name))
    print('connection established: {}'.format(client.isConnected()))
    latest_eth_block=client.eth.get_block('latest')
    eth=create_eth_data_list(latest_eth_block)
    # get the previous block and calculate the tps
    previous_block_num=int(eth['number'])-1
    previous_eth_block = client.eth.get_block(previous_block_num)
    latest_timestamp=eth['timestamp']
    previous_timestamp=previous_eth_block['timestamp']
    time_taken = int(latest_timestamp)-int(previous_timestamp)
    tps=0.0
    print('calculating tps...')
    if abs(time_taken)>0.0:
        tps=eth['transactions'] / time_taken
    eth['tps']=tps
    add_data_to_table(engine, eth, name)

# again using quite a crude method to get tps but it is more accurate 
# since it is an actual value and not a theoretical reported value from Solana
# declaring async so need to 'await' the actual call as well...
async def get_sol_data(engine,client,name):
    print('getting {} data...'.format(name))
    sol=dict()
    wait_count=5
    start_timer = time.time()
    data=await client.get_transaction_count()
    start=int(data['result'])
    for i in range(wait_count):
        time.sleep(wait_count)
    data=await client.get_transaction_count()
    end=int(data['result'])
    t_count=end-start
    end_timer = time.time()
    sol['tps']=t_count/(end_timer-start_timer)
    print('recorded {} tps'.format(sol['tps']))
    print('getting node, block and epoch info...')
    node_info=await client.get_cluster_nodes()
    epoch_info=await client.get_epoch_info()
    block_info=await client.get_block(epoch_info['result']['absoluteSlot'])
    sol['blockHeight']=block_info['result']['blockHeight']
    sol['blockTime']=block_info['result']['blockTime']
    sol['blockhash']=block_info['result']['blockhash']
    sol['previousBlockhash']=block_info['result']['previousBlockhash']
    sol['currentEpoch']=epoch_info['result']['epoch']
    sol['slotsInEpoch']=epoch_info['result']['slotsInEpoch']
    sol['networkNodes']=len(node_info['result'])
    sol['transactionsPerBlock']=len(block_info['result']['transactions'])
    add_data_to_table(engine, sol, name)

async def run_script():
    # initialise things for the run
    db=create_db_instance()
    # intialise containers for various connections... SOLANA
    sol_clients=dict()
    sol_clients['SOLMAIN'] = AsyncClient("https://api.mainnet-beta.solana.com")
    sol_clients['SOLDEV'] = AsyncClient("https://api.devnet.solana.com")
    # intialise containers for eth client connections ... FTM, Polygon, Algorand
    eth_clients=dict()
    eth_clients['ETH'] = eth_chain # imported directly from Infura
    eth_clients['FTM'] = ftm_connection() # created from Quicknode account
    eth_clients['POLY'] = polygon_connection() # created from Quicknode account
    counter=1
    while True:
        print('running: {}'.format(counter))
        for chain, client in eth_clients.items():
            get_eth_data(db,client,chain)
        for chain, client in sol_clients.items():
            await get_sol_data(db,client,chain)
        counter=counter+1
        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    #set_eth_env_key()
    asyncio.run(run_script())