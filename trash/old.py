# def get_token_holders(token_id='1.3.1236', account_prefix=r'moneda-par', omit_accounts=[r'gobierno-par']):
#     url = 'http://185.208.208.184:5000/get_all_asset_holders?asset_id='+token_id
#     response = urllib.request.urlopen(url)
#     data = json.loads(response.read())
#     accounts = {'name':[i['name'] for i in data],
#                 'id': [i['account_id'] for i in data]}
#     df = pd.DataFrame(accounts['name'], index=accounts['id'], columns=['name'])
#     df['name'] = df['name'].str.replace(account_prefix+r'.','')
#     # data1 = [pd.Series(i)[['name','account_id']] for i in data_temp]
#     # df1 = pd.DataFrame(data1)
#     return df.loc[~df.name.isin(omit_accounts)]

# ### Para agregar usuarios no captados:
# # open transacciones.json de usarios catalogados como holders
# # with open('transacciones.json','r') as f:
# #     data = pd.read_json(f, orient='index')
# # data = data.drop(columns=['sender_name','recipient_name'])
# # agrega la cuenta del gobierno para identificar usuarios no captados
# all_accounts = get_token_holders(token_id=tk_id, account_prefix=tk_prefix,  omit_accounts=[''])
# gobierno_id = list(all_accounts[all_accounts.name == 'gobierno-par'].index)[0]
# historical_gob = get_user_data(user_id=gobierno_id, max_page_num=999999999, token_id='1.3.1236')
# # historico (inc. gobierno)
# data_2 = pd.concat([data] + [historical_gob]).drop_duplicates()
# # usuarios no incluidos como asset_holders que transaccionaron con gob o con token_holders
# strange_recip = np.array(data_2.loc[~data_2.recipient.isin(users_ids + [gobierno_id]),'recipient'])
# strange_sender = np.array(data_2.loc[~data_2.sender.isin(users_ids + [gobierno_id]),'sender'])
# strange_ids = list(np.union1d(strange_recip,strange_sender))
# # nombres de usarios strange
# strange_names = [get_user_name(i) for i in strange_ids]
# # datos de las cuentas 'strange' (corrido con max_page_num=9999999999)
# historical_strange = [get_user_data(user_id=i, max_page_num=999999999, token_id=tk_id) for i in strange_ids]
# data_strange = pd.concat(historical_strange).drop_duplicates()
# accounts_strange = pd.DataFrame(data=strange_names,index=strange_ids,columns=['name'])
# # datos de cuentas holders + cuentas strange
# accounts_full = pd.concat(accounts, accounts_strange)
# data_full = pd.concat(data, data_strange).drop_duplicates().sort_values('time', ascending=True)
# data_full = pd.merge(data_full, accounts_full, how='left', left_on='sender', right_index=True)
# data_full = data_full.rename(columns={'name':'sender_name'})
# data_full = pd.merge(data_full, accounts_full, how='left', left_on='recipient', right_index=True)
# data_full = data_full.rename(columns = {'name':'recipient_name'})
# # write to json file
# data_full.to_json('transacciones_full.json', orient='index')
