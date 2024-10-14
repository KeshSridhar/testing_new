"""
TABLE NOTES

Table updates will have to rewrite data for that given year each time
The "last_updated" field is the last date the latest year data was pushed
"""

import pandas as pd
from stac_utils import google
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from dotenv import load_dotenv
import re
import datetime
import tempfile

# get client
client = google.auth_bq()

# for logging
capabilities = DesiredCapabilities().CHROME
capabilities["goog:loggingPrefs"] = {"performance": "ALL"}  # chromedriver 75+

# custom options
options = Options()

# # headless browser
options.add_argument("--headless=new")

with tempfile.TemporaryDirectory() as tempdir:
    options.add_experimental_option("prefs", {
        "download.default_directory": f"{tempdir}"
    })

    # chromium -- no need to specify path
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)

    driver.get("https://www.mec.mo.gov/MEC/Campaign_Finance/CF_ContrCSV.aspx")

    # path for file directory
    relative_path = f"{tempdir}/"

    # set current year
    year = datetime.datetime.now().year

    # download report data
    driver.find_element(By.XPATH, f"//*[@value='summary']").click()
    time.sleep(4)
    driver.find_element(By.XPATH, f"//*[@value='{year}']").click()
    time.sleep(5)
    driver.find_element(By.XPATH, "//*[@value='Export to CSV']").click()
    time.sleep(6)

    # get name of downloaded file
    filename = os.listdir(f"{relative_path}")[0]



    # get dataframe of the latest file
    df_current_file = pd.read_csv(f'{relative_path}{filename}',sep=',', dtype=str, on_bad_lines='warn')

    # make MECID uppercase
    df_current_file["MECID"] = df_current_file["MECID"].str.upper()

    # add current year
    df_current_file.insert(1, 'year', str(year))

    # add updated date as today's date
    df_current_file['last_updated'] = str(datetime.date.today())

    # delete all latest year rows from previous file
    sql = f"""
    DELETE
    FROM
      stac-labs.mo_sharing.MO_MEC_Data_Report_Summary_Data_Combined
    WHERE
      year = '{year}'
    """
    # run delete
    pd.DataFrame(google.run_query(sql=sql, client=client))

    #### PREVIOUS FILE ####
    # get previous file
    sql = """
    SELECT
      *
    FROM
      stac-labs.mo_sharing.MO_MEC_Data_Report_Summary_Data_Combined
    """

    # create dataframe for previous file, from existing bq table
    df_previous_file = pd.DataFrame(google.run_query(sql=sql, client=client))

    # find new starting primary key in previous file, for any potential new rows
    starting_pk_for_new_rows = df_previous_file["ID"].astype(int).max() + 1

    # get total rows of current file
    total_new_rows = len(df_current_file)

    # generate unique id array
    id_column = list(range(starting_pk_for_new_rows, starting_pk_for_new_rows + total_new_rows))

    # create ID column in current file, and move to first column
    df_current_file = df_current_file.assign(ID=id_column)
    id_column = df_current_file.pop("ID")
    df_current_file.insert(0, 'ID', id_column)

    # format column names to bq specifications
    df_current_file.rename(columns=lambda y: re.sub("[^A-Za-z0-9_]", "_", y.strip()), inplace=True)
    df_current_file.rename(columns=lambda y: re.sub("^[0-9]", "_", y.strip()), inplace=True)

    # change cols to float datatype
    df_current_file["Previous_Receipts"] = df_current_file["Previous_Receipts"].astype(float)
    df_current_file["Contributions_Received"] = df_current_file["Contributions_Received"].astype(float)
    df_current_file["Loans_Received"] = df_current_file["Loans_Received"].astype(float)
    df_current_file["Misc__Receipts"] = df_current_file["Misc__Receipts"].astype(float)
    df_current_file["Receipts_Subtotal"] = df_current_file["Receipts_Subtotal"].astype(float)
    df_current_file["In_Kind_Contributions"] = df_current_file["In_Kind_Contributions"].astype(float)
    df_current_file["Total_Receipts_This_Election"] = df_current_file["Total_Receipts_This_Election"].astype(float)
    df_current_file["Previous_Expenditures"] = df_current_file["Previous_Expenditures"].astype(float)
    df_current_file["Cash_or_Check_Expenditures"] = df_current_file["Cash_or_Check_Expenditures"].astype(float)
    df_current_file["In_Kind_Expenditures"] = df_current_file["In_Kind_Expenditures"].astype(float)
    df_current_file["Credit_Expenditures"] = df_current_file["Credit_Expenditures"].astype(float)
    df_current_file["Expenditure_Subtotal"] = df_current_file["Expenditure_Subtotal"].astype(float)
    df_current_file["Total_Expenditures"] = df_current_file["Total_Expenditures"].astype(float)
    df_current_file["Previous_Contributions"] = df_current_file["Previous_Contributions"].astype(float)
    df_current_file["Cash_Check_Contributions"] = df_current_file["Cash_Check_Contributions"].astype(float)
    df_current_file["Credit_Contributions"] = df_current_file["Credit_Contributions"].astype(float)
    df_current_file["In_Kind_Contributions_1"] = df_current_file["In_Kind_Contributions_1"].astype(float)
    df_current_file["Contribution_Subtotal"] = df_current_file["Contribution_Subtotal"].astype(float)
    df_current_file["Total_Contributions"] = df_current_file["Total_Contributions"].astype(float)
    df_current_file["Loan_Disbursements"] = df_current_file["Loan_Disbursements"].astype(float)
    df_current_file["Disbursements_Payments"] = df_current_file["Disbursements_Payments"].astype(float)
    df_current_file["Misc__Disbursements"] = df_current_file["Misc__Disbursements"].astype(float)
    df_current_file["Total_Disbursements"] = df_current_file["Total_Disbursements"].astype(float)
    df_current_file["Starting_Money_on_Hand"] = df_current_file["Starting_Money_on_Hand"].astype(float)
    df_current_file["Monetary_Receipts"] = df_current_file["Monetary_Receipts"].astype(float)
    df_current_file["Check_Disbursements"] = df_current_file["Check_Disbursements"].astype(float)
    df_current_file["Cash_Disbursements"] = df_current_file["Cash_Disbursements"].astype(float)
    df_current_file["Total_Monetary_Disbursements"] = df_current_file["Total_Monetary_Disbursements"].astype(float)
    df_current_file["Ending_Money_on_Hand"] = df_current_file["Ending_Money_on_Hand"].astype(float)
    df_current_file["Outstanding_Indebtedness"] = df_current_file["Outstanding_Indebtedness"].astype(float)
    df_current_file["Loans_Recieved"] = df_current_file["Loans_Recieved"].astype(float)
    df_current_file["New_Expenditures"] = df_current_file["New_Expenditures"].astype(float)
    df_current_file["New_Contributions"] = df_current_file["New_Contributions"].astype(float)
    df_current_file["Payments_Made_on_Loan"] = df_current_file["Payments_Made_on_Loan"].astype(float)
    df_current_file["Debt_Forgiven_on_Loans"] = df_current_file["Debt_Forgiven_on_Loans"].astype(float)
    df_current_file["Total_Indebtendness"] = df_current_file["Total_Indebtendness"].astype(float)

    ## Upload current file to BQ TO BQ ##
    #  desired table name in BQ
    table_id = f"stac-labs.mo_sharing.MO_MEC_Data_Report_Summary_Data_Combined"

    # Run table creation job
    job = client.load_table_from_dataframe(df_current_file, table_id)
    job.result()

    # Confirmation of table update
    table = client.get_table(table_id)  # Make an API request.
    print(
        "Total {} rows and {} columns in table {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

    print(df_current_file)
