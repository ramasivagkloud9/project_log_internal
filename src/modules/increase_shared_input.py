import pandas as pd
from datetime import datetime, timedelta
import math

df = pd.read_csv('C:\\Users\\ramasiva.g\\Downloads\\Retail-Sales-Data-EDA-main\\Retail-Sales-Data-EDA-main\\Retail.csv')
# print(df)
print(df.columns)
print(df.dtypes)
"""
Index(['OrderNumber', 'ProductName', 'Color', 'Category', 'Subcategory',
       'ListPrice', 'Orderdate', 'Duedate', 'Shipdate', 'PromotionName',
       'SalesRegion', 'OrderQuantity', 'UnitPrice', 'SalesAmount',
       'DiscountAmount', 'TaxAmount', 'Freight'],
      dtype='object')
      """
fin_df = pd.DataFrame(columns=['OrderNumber', 'ProductName', 'Color', 'Category', 'Subcategory',
                               'ListPrice', 'Orderdate', 'Duedate', 'Shipdate', 'PromotionName',
                               'SalesRegion', 'OrderQuantity', 'UnitPrice', 'SalesAmount',
                               'DiscountAmount', 'TaxAmount', 'Freight'])
"""
{'OrderNumber': 'SO46959', 'ProductName': 'Road-650 Red, 48', 'Color': 'Red', 'Category': 'Bikes', 'Subcategory': 'Road Bikes', 'ListPrice': 782.99, 'Orderdate': '1/29/2012', 'Duedate': '2/10/2012', 'Shipdate': '2/5/2012', 'PromotionName': 'No Discount', 'SalesRegion': 'Canada', 'OrderQuantity': '6', 'UnitPrice': 469.794, 'SalesAmount': 2818.764, 'DiscountAmount': 0.0, 'TaxAmount': 225.5011, 'Freight': 70.4691}
"""


def converdate_str_to_datetime(str_: object):
    if str_ != '':
        return (datetime.strptime(str_, '%m/%d/%Y')+timedelta(days=1)).strftime("%m/%d/%Y")
    else:
        return ""
main_index = 0

for index, row in df.iterrows():
    main_index += 1
    fin_df.loc[len(fin_df.index)] = row
    row_list = {k: v for k, v in dict(row).items()}
    print("Created {} row".format(main_index))
    for second_index, i in enumerate(range(9)):
        main_index += 1
        row_list['OrderNumber'] = r"{}{}".format(row_list['OrderNumber'][0:2], int(row_list['OrderNumber'][2:]) + 1000)
        row_list['Orderdate'] = converdate_str_to_datetime(row_list['Orderdate'])
        row_list['Duedate'] = converdate_str_to_datetime(row_list['Duedate'])
        row_list['Shipdate'] = converdate_str_to_datetime(row_list['Shipdate'])
        row_list['UnitPrice'] = float(row_list['UnitPrice']) + 500
        row_list['SalesAmount'] = float(row_list['UnitPrice']) * float(row_list['OrderQuantity'])
        fin_df.loc[len(fin_df.index)] = row_list.values()
        print("Created {} row".format(main_index))
    # if index == 10:
    #     break

fin_df.to_csv('incremented_file_{}.csv'.format(fin_df.shape[0]))
