# %% Libs
'''
This is just a testing script for writing to PDW!
'''
import pandas as pd
from BmoScraper import BmoScraper
from keyring import get_password
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# %% Run process
bmo_urls = [
    'https://www.bmonotes.com/Note/JHN7482',
    'https://www.bmonotes.com/Note/JHN15954'
]
bmo = BmoScraper(bmo_urls)
bmo.run_all_rules()

# %% Cxn
user = "skimble"
password = get_password('docdb_preprod', user)
host = "dev-documentdb.cluster-cb6kajicuplh.us-east-1.docdb.amazonaws.com"
port = "27017"
options = ("tls=true&tlsAllowInvalidCertificates=true&replicaSet=rs0&"
           "readPreference=secondaryPreferred&retryWrites=false")
cxn_string = f"mongodb://{user}:{password}@{host}:{port}/?{options}"
client = MongoClient(cxn_string)

# %% View test collection for test write
db = client['test-masking-dev']
PdwProductCore = db.PdwProductCore
# collections = list(db.list_collections())
# collection_names = [c['name'] for c in collections]
# print(collection_names)

# %% Add all
pdw_df = bmo.pdw_df.copy()
pdw_df.drop(['PDW Name', 'Mark to Market Price'], inplace=True)
pdw_df.dropna(subset=bmo.pdw_df.columns, how='all', inplace=True)
pdw_df.reset_index(inplace=True)

# %% Process for JSON
pdw_df_dict = {}
for col in bmo.pdw_df.columns:
    pdw_df_dict[col] = pdw_df[['PDW Fields', col]].dropna()
    pdw_df_dict[col] = pd.concat(
        [
            pdw_df_dict[col]['PDW Fields'].str.split('.', expand=True),
            pdw_df_dict[col]
        ],
        axis=1,
    )
    pdw_df_dict[col].drop(columns='PDW Fields', inplace=True)

# %% Convert to JSON
'''
I have NOT added error handling for the VPN!  Make sure you're connected!
'''
result = []
for col in pdw_df_dict.keys():
    len_cols = list(range(len(pdw_df_dict[col].columns) - 1))
    pdw_pre_insert = pdw_df_dict[col].set_index(len_cols).groupby(
        level=0).apply(lambda x: x.xs(x.name)[col].to_dict()).to_dict()

    # Prepare final JSON
    pdw_insert = {}
    for key, val in pdw_pre_insert.items():
        pdw_insert[key] = {}
        for key2, val2 in val.items():
            if isinstance(key2, tuple):
                if isinstance(key2[1], str):
                    pdw_insert[key][key2[0]] = {key2[1]: val2}
                else:
                    pdw_insert[key][key2[0]] = val2
            else:
                pdw_insert[key][key2] = val2

    # Insert into DB
    try:
        result.append((col, PdwProductCore.insert_one(pdw_insert)))
    except DuplicateKeyError:
        print('Product exists')
