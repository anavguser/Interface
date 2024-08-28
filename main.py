from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from postgrest.exceptions import APIError
import streamlit as st


mtr = pd.read_csv('mtr.csv')
payment = pd.read_csv('payment.csv')

# print(mtr.head())
# print(payment.head())

# -------------------------------------------------------------------------------------------------

# Print the shape of the DataFrame before removal
# print("Shape before MTR Manipulation:", mtr.shape)

# Step 0: Rename the columns
mtr = mtr.rename(columns={'Order Id': 'Order ID'})

# Step 1: Remove rows where 'Transaction Type' is 'Cancel'
mtr = mtr[mtr['Transaction Type'] != 'Cancel']

# Step 2: Rename 'Refund' to 'Return'
mtr['Transaction Type'] = mtr['Transaction Type'].replace('Refund', 'Return')

# Step 3: Rename 'FreeReplacement' to 'Return'
mtr['Transaction Type'] = mtr['Transaction Type'].replace('FreeReplacement', 'Return')

# Step 4: Drop unnecessary columns
mtr = mtr.drop(columns=['Invoice Date'])
mtr = mtr.drop(columns=['Shipment Date'])
mtr = mtr.drop(columns=['Shipment Item Id'])
mtr = mtr.drop(columns=['Item Description'])

# print(mtr.head())
# print("Shape after MTR Manipulation:", mtr.shape)

# -------------------------------------------------------------------------------------------------

# print("Shape before Payment Manipulation:", payment.shape)

# Step 1: Remove rows where 'Type' is 'Transfer' in the Payment DataFrame
payment = payment[payment['type'] != 'Transfer']

# Step 2: Rename the columns
payment = payment.rename(columns={'type': 'Payment Type'})
payment = payment.rename(columns={'total': 'Net Amount'})
payment = payment.rename(columns={'description': 'P_Description'})
payment = payment.rename(columns={'date/time': 'Payment Date'})
payment = payment.rename(columns={'order id': 'Order ID'})

# Step 3: Rename specified values to 'Order'
values_to_rename = ['Adjustment', 'FBA Inventory Fee', 'Fulfilment Fee Refund', 'Service Fee']
payment['Payment Type'] = payment['Payment Type'].replace(values_to_rename, 'Order')

# Step 4: Rename 'Refund' to 'Return'
payment['Payment Type'] = payment['Payment Type'].replace('Refund', 'Return')

# Step 5: Add a new column 'Transaction Type' and assign 'Payment' to all rows
payment['Transaction Type'] = 'Payment'

# Print the shape of the Payment DataFrame after removal
# print(payment.head())
# print("Shape after Payment Manipulation:", payment.shape)

# -------------------------------------------------------------------------------------------------


def merge_sheets():
    # Merge the dataframes on 'Order ID' and 'Transaction Type'
    merged_df = pd.merge(payment, mtr, on=['Order ID', 'Transaction Type'], how='outer')

    # Reorder the columns to match the desired output
    merged_df = merged_df[['Order ID', 'Transaction Type', 'Payment Type', 'Invoice Amount', 
                           'Net Amount', 'P_Description', 'Order Date', 'Payment Date']]

    return merged_df

# Call the function and store the result in a variable
result_df = merge_sheets()

# Display the first few rows of the DataFrame
# print(result_df.head())
# -------------------------------------------------------------------------------------------------

# # Merge the dataframes on 'Order ID' and 'Transaction Type'
# merged_df = pd.merge(payment_df, mtr_df, on=['Order ID', 'Transaction Type'], how='outer')

# # Reorder the columns to match the desired output
# merged_df = merged_df[['Order ID', 'Transaction Type', 'Payment Type', 'Invoice Amount', 
#                        'Net Amount', 'P_Description', 'Order Date', 'Payment Date']]

# # Save the merged dataframe to a new CSV file
# merged_df.to_csv('Merged_Output.CSV', index=False)

# -------------------------------------------------------------------------------------------------
# this worksssss
def clean_amount(value):
    if isinstance(value, str):
        return pd.to_numeric(''.join(c for c in value if c.isdigit() or c in '.-'), errors='coerce')
    return value

# Clean and convert the Net Amount column
result_df['Net Amount'] = result_df['Net Amount'].apply(clean_amount)

# Group by P_Description to get the sum of Net Amount and count of Order IDs
summary_df = result_df.groupby('P_Description').agg(
    Net_Amount_Sum=('Net Amount', 'sum'),
    Order_ID_Count=('Order ID', 'count')
).sort_values(by='Net_Amount_Sum', ascending=False).reset_index()

# summary_df now holds the summary data
print(summary_df)

P_description_value = 'Product A'  # Example: 'Product XYZ'
wow_df = result_df[result_df['P_Description'] == P_description_value]
print(wow_df.shape)

p_descriptions = [
    'Fulfillment Fee Refund',
    'Customer Return',
    'Customer Service',
    'Damaged:Warehouse',
    'Return Fee',
    'Pickup Service',
    'Cost of Advertising'
]

# Creating separate DataFrames for each P_Description value
fulfillment_fee_refund_df = result_df[result_df['P_Description'] == 'Fulfillment Fee Refund']
customer_return_df = result_df[result_df['P_Description'] == 'FBA Inventory Reimbursement - Customer Return']
customer_service_df = result_df[result_df['P_Description'] == 'FBA Inventory Reimbursement - Customer Service Issue']
damaged_warehouse_df = result_df[result_df['P_Description'] == 'FBA Inventory Reimbursement - Damaged:Warehouse']
return_fee_df = result_df[result_df['P_Description'] == 'FBA Removal Order: Return Fee']
pickup_service_df = result_df[result_df['P_Description'] == 'FBA Inbound Pickup Service']
cost_of_advertising_df = result_df[result_df['P_Description'] == 'Cost of Advertising']

dataframes = {
    'Fulfillment Fee Refund': fulfillment_fee_refund_df,
    'Customer Return': customer_return_df,
    'Customer Service': customer_service_df,
    'Damaged Warehouse': damaged_warehouse_df,
    'Return Fee': return_fee_df,
    'Pickup Service': pickup_service_df,
    'Cost of Advertising': cost_of_advertising_df,
}

# Streamlit app setup
st.title('Dataframe Selector')

# Selectbox for choosing a dataframe
selected_df_name = st.selectbox('Select a dataframe to view:', list(dataframes.keys()))

# Display the selected dataframe
st.write(f"Displaying: {selected_df_name}")
st.dataframe(dataframes[selected_df_name])


# -------------------------------------------------------------------------------------------------

# Assuming result_df is your original DataFrame
# Group by Order ID and filter for Order IDs with length 10
removal_df = result_df[result_df['Order ID'].str.len() == 10]

# Count the number of unique Order IDs with length 10
order_id_count = len(removal_df)

# Print the count of Order IDs
print(f"Removal Order IDs: {order_id_count}")
print()

# Display the new DataFrame
print(removal_df)
print()

print(f"Shape of result_df before removal: {result_df.shape}")

order_ids_to_remove = removal_df['Order ID'].unique()

# Remove rows from result_df where Order ID is in removal_df
result_df = result_df[~result_df['Order ID'].isin(order_ids_to_remove)]

# Print the shape of the DataFrame before and after removal
print(f"Shape of result_df after removal: {result_df.shape}")

# Optional: Reset the index of the resulting DataFrame
result_df = result_df.reset_index(drop=True)


# -------------------------------------------------------------------------------------------------

# Filter for Returns with non-blank Invoice Amount
return_df = result_df[(result_df['Transaction Type'] == 'Return') & (result_df['Invoice Amount'].notna())]

# Count the number of unique Order IDs in return_df
return_order_id_count = return_df['Order ID'].nunique()

# Print the count of Order IDs
print(f"Return Order IDs: {return_order_id_count}")
print()

# Display the first few rows of the new DataFrame
print(return_df.head())

# Print the shape of the return_df to show total number of rows and columns
print(f"Shape of return_df: {return_df.shape}")

print(f"Shape of result_df before removal: {result_df.shape}")

order_ids_to_remove = return_df['Order ID'].unique()

# Remove rows from result_df where Order ID is in removal_df
result_df = result_df[~result_df['Order ID'].isin(order_ids_to_remove)]

# Print the shape of the DataFrame before and after removal
print(f"Shape of result_df after removal: {result_df.shape}")


## Optional: If you want to save the new DataFrame to a CSV file
# return_df.to_csv('return_orders.csv', index=False)

# -------------------------------------------------------------------------------------------------

print()

result_df['Net Amount'] = result_df['Net Amount'].replace({',': ''}, regex=True).astype(float)

# Filtering the DataFrame based on conditions
neg_df = result_df[(result_df['Transaction Type'] == 'Payment') & (result_df['Net Amount'] < 0)]

# Counting the number of distinct Order IDs
order_id_count = neg_df['Order ID'].nunique()

# Display the count and the filtered DataFrame
print(f"Number of distinct Order IDs: {order_id_count}")
print(neg_df)

neg_df.to_csv('negative_payments.csv', index=False)