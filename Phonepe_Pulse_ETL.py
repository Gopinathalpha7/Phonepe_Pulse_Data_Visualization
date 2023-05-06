# ==================================================     /    IMPORT LIBRARY    /    ========================================================= #

# [clone libraries]
import requests
import subprocess

# [pandas and file handling libraries]
import pandas as pd
import os
import json

# [SQL libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine

# ==============================================         /   /   E T L   /   /         ====================================================== #

# =====================================================    /   CLONING   /     ============================================================== #

#Specify the GitHub repository URL
response = requests.get('https://api.github.com/repos/PhonePe/pulse')
repo = response.json()
clone_url = repo['clone_url']

#Specify the local directory path
clone_dir = "C:/Phonepe Pulse data"

# Clone the repository to the specified local directory
subprocess.run(["git", "clone", clone_url, clone_dir], check=True)

# ===============================================    /    DATA PROCESSING     /   =========================================================== #

#==============================     DATA     /     AGGREGATED     /     TRANSACTION     ===================================#
# 1

path_1 = "C:/Phonepe Pulse data/data/aggregated/transaction/country/india/state/"
Agg_tran_state_list = os.listdir(path_1)

Agg_tra = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Agg_tran_state_list:
    p_i = path_1 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            A = json.load(Data)
            
            for l in A['data']['transactionData']:
                Name = l['name']
                count = l['paymentInstruments'][0]['count']
                amount = l['paymentInstruments'][0]['amount']
                Agg_tra['Transaction_type'].append(Name)
                Agg_tra['Transaction_count'].append(count)
                Agg_tra['Transaction_amount'].append(amount)
                Agg_tra['State'].append(i)
                Agg_tra['Year'].append(j)
                Agg_tra['Quarter'].append(int(k.strip('.json')))
                
df_aggregated_transaction = pd.DataFrame(Agg_tra)

#==============================     DATA     /     AGGREGATED     /     USER     ===================================#
# 2

path_2 = "C:/Phonepe Pulse data/data/aggregated/user/country/india/state/"
Agg_user_state_list = os.listdir(path_2)

Agg_user = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [], 'Percentage': []}

for i in Agg_user_state_list:
    p_i = path_2 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            B = json.load(Data)
            
            try:
                for l in B["data"]["usersByDevice"]:
                    brand_name = l["brand"]
                    count_ = l["count"]
                    ALL_percentage = l["percentage"]
                    Agg_user["Brands"].append(brand_name)
                    Agg_user["Count"].append(count_)
                    Agg_user["Percentage"].append(ALL_percentage*100)
                    Agg_user["State"].append(i)
                    Agg_user["Year"].append(j)
                    Agg_user["Quarter"].append(int(k.strip('.json')))
            except:
                pass

df_aggregated_user = pd.DataFrame(Agg_user)

#==============================     DATA     /     MAP     /     TRANSACTION     =========================================#
# 3

path_3 = "C:/Phonepe Pulse data/data/map/transaction/hover/country/india/state/"
map_tra_state_list = os.listdir(path_3)

map_tra = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [], 'Amount': []}

for i in map_tra_state_list:
    p_i = path_3 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            C = json.load(Data)
            
            for l in C["data"]["hoverDataList"]:
                District = l["name"]
                count = l["metric"][0]["count"]
                amount = l["metric"][0]["amount"]
                map_tra["District"].append(District)
                map_tra["Count"].append(count)
                map_tra["Amount"].append(amount)
                map_tra['State'].append(i)
                map_tra['Year'].append(j)
                map_tra['Quarter'].append(int(k.strip('.json')))
                
df_map_transaction = pd.DataFrame(map_tra)

#==============================         DATA     /     MAP     /     USER         ============================================#
# 4

path_4 = "C:/Phonepe Pulse data/data/map/user/hover/country/india/state/"
map_user_state_list = os.listdir(path_4)

map_user = {"State": [], "Year": [], "Quarter": [], "District": [], "RegisteredUser": []}

for i in map_user_state_list:
    p_i = path_4 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            D = json.load(Data)

            for l in D["data"]["hoverData"].items():
                district = l[0]
                registereduser = l[1]["registeredUsers"]
                map_user["District"].append(district)
                map_user["RegisteredUser"].append(registereduser)
                map_user['State'].append(i)
                map_user['Year'].append(j)
                map_user['Quarter'].append(int(k.strip('.json')))
                
df_map_user = pd.DataFrame(map_user)

#==============================     DATA     /     TOP     /     TRANSACTION     =========================================#
# 5

path_5 = "C:/Phonepe Pulse data/data/top/transaction/country/india/state/"
top_tra_state_list = os.listdir(path_5)

top_tra = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in top_tra_state_list:
    p_i = path_5 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            E = json.load(Data)
            
            for l in E['data']['pincodes']:
                Name = l['entityName']
                count = l['metric']['count']
                amount = l['metric']['amount']
                top_tra['District_Pincode'].append(Name)
                top_tra['Transaction_count'].append(count)
                top_tra['Transaction_amount'].append(amount)
                top_tra['State'].append(i)
                top_tra['Year'].append(j)
                top_tra['Quarter'].append(int(k.strip('.json')))
                
df_top_transaction = pd.DataFrame(top_tra)

#==============================     DATA     /     TOP     /     USER     ============================================#
# 6

path_6 = "C:/Phonepe Pulse data/data/top/user/country/india/state/"
top_user_state_list = os.listdir(path_6)

top_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Pincode': [], 'Registered_User': []}

for i in top_user_state_list:
    p_i = path_6 + i + "/"
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + j + "/"
        Agg_yr_list = os.listdir(p_j)

        for k in Agg_yr_list:
            p_k = p_j + k
            Data = open(p_k, 'r')
            F = json.load(Data)
            
            for l in F['data']['pincodes']:
                Name = l['name']
                registeredUser = l['registeredUsers']
                top_user['District_Pincode'].append(Name)
                top_user['Registered_User'].append(registeredUser)
                top_user['State'].append(i)
                top_user['Year'].append(j)
                top_user['Quarter'].append(int(k.strip('.json')))
                
df_top_user = pd.DataFrame(top_user)

#  =============     CONNECT SQL SERVER  /   CREAT DATA BASE    /  CREAT TABLE    /    STORE DATA    ========  #

# Connect to the MySQL server
mydb = mysql.connector.connect(
  host = "localhost",
  user = "root",
  password = "root",
  auth_plugin = "mysql_native_password"
)

# Create a new database and use
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")

# Connect to the new created database
engine = create_engine('mysql+mysqlconnector://root:root@localhost/phonepe_pulse', echo=False)

# Use pandas to insert the DataFrames datas to the SQL Database -> table
df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False)
df_aggregated_user.to_sql('aggregated_user', engine, if_exists = 'replace', index=False)
df_map_transaction.to_sql('map_transaction', engine, if_exists = 'replace', index=False)
df_map_user.to_sql('map_user', engine, if_exists = 'replace', index=False)
df_top_transaction.to_sql('top_transaction', engine, if_exists = 'replace', index=False)
df_top_user.to_sql('top_user', engine, if_exists = 'replace', index=False)

# =========================================================================================================================================== #