import os
import pandas as pd
from tabulate import tabulate,  _table_formats
import re
import pyparsing
import ModuleADF

directory = "/home/user/python-project/File/SP/ETL1"
files = os.listdir(directory)
dict_list = []
#hardcoded_file_name = ["FOND_ID.USP_LOAD_ABSTR_IFRS17_BALANCING_SP.sql"] #get desired file name

commentFilter = pyparsing.cppStyleComment.suppress()

def process_sql(sql_code):
  """Processes SQL code, skipping green highlighted comments."""

  # Define a regex pattern to match comments (adjust if needed)
  comment_pattern = r"--.*$"

  # Split the SQL code into lines
  lines = sql_code.splitlines()

  processed_code = ""
  for line in lines:
    # Remove comments from the current line
    stripped_line = re.sub(comment_pattern, "", line)
    processed_code += stripped_line + "\n"

  return processed_code
for subdir, dirs, files in os.walk(directory):
    for file in files: #filter an array or objects using files[:1]
     #if file in hardcoded_file_name: # remove if if not used anymore
            with open(os.path.join(subdir, file), "r") as f:
                contents = process_sql(f.read()) # remove green highlighted -- comments
                contents = commentFilter.transformString(contents) # remove green regex pattern /**/ comments
                #print(contents)
                table_name = []
                file_dict = {"SpName": file, "TableName":""}
                dict_list.append(file_dict)
                for line in contents.replace("[","").replace("]","").splitlines():
                    if ("FOND_ID." in line.replace(" ", "") or 
                            "ABST_ID." in line.replace(" ", "") and "-" not in line
                        or "STAG_ID." in line.replace(" ","")):
                            FirstClean = line.replace("TRUNCATE TABLE","").replace("INSERT INTO","").replace("FROM","").replace("\t","").replace(";","")
                            #print(FirstClean)
                            for line2 in FirstClean.split(sep="."):
                                if "ABST_ID" not in line2.replace(" ","") and "FOND_ID" not in line2.replace(" ","") and "STAG_ID" not in line2.replace(" ",""):
                                    if " " not in line2.lstrip():
                                        p = line2+";"[:line2.find(";")]
                                        p1 = p.replace(" ","")
                                        #print(p1)
                                        if p1.startswith("STAG_") or p1.startswith("FOND_") or p1.startswith("ABST_") or p1.startswith("USP_") or p1.startswith("TMP_"):
                                            if "(" in p1:
                                                table_name.append(p1[:p1.find("(")])
                                                #print(p1[:p1.find("(")])
                                                file_dict["TableName"] = table_name
                                            else:
                                                #print("step2 = "+p1)
                                                table_name.append(p1)
                                                file_dict["TableName"] = table_name
                                    else:
                                        line_fix = line2.lstrip()[:line2.lstrip().find(" ")]
                                        if line_fix.startswith("STAG_") or line_fix.startswith("FOND_") or line_fix.startswith("ABST_") or line_fix.startswith("USP_") or line_fix.startswith("TMP_"):
                                                if "(" in line_fix:
                                                    table_name.append(line_fix[:line_fix.find("(")])
                                                    file_dict["TableName"] = table_name
                                                else:
                                                    table_name.append(line_fix)
                                                    file_dict["TableName"] = table_name
                                                    #print(line_fix)

df = pd.DataFrame(dict_list).explode('TableName')
df = df.map(lambda x: x.replace(")","").replace("_'","").replace("'',","").replace("'","").replace(":","").replace("@ENTITY_FLG",""))
df = df.map(lambda x: x.replace("+@V_DRIVER_PERIOD","").replace("+_HIS","")) #ETL5
df = df.map(lambda x: x.replace("+@BATCH_NUMBER+","").replace(",U","").replace(",@BATCH_NUMBER","").replace("+","").replace(",@BATCHDATESTR,_HIS","")) #ETL4
df = df.drop_duplicates()
df_adf = ModuleADF.adf_function()
df_all = pd.merge(df, df_adf, left_on='TableName', right_on='FinalTable', how='left')
df_all = df_all[['ADF Name', 'SpName','TableName']]

df_all.to_csv("/home/user/python-project/File/Output/OutputMerge.csv", index=False)
#print(tabulate(df, headers='keys', tablefmt='psql'))
#print(df_all)
print(tabulate(df_all, headers='keys', tablefmt='psql'))