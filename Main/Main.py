import os
import pandas as pd
from tabulate import tabulate

directory = "/home/user/python-project/File/SP"
files = os.listdir(directory)
dict_list = []
hardcoded_file_name = ["USP_LOAD_ABST_ACTUAL_CF_TRAN.sql","USP_LOAD_ABST_ALLOC_DRIVER_CONFIG.sql"] #get desired file name

for file in files: #filter an array or objects using files[:1]
    if file in hardcoded_file_name: # remove if if not used anymore
        with open(os.path.join(directory, file), "r") as f:
            contents = f.read()
            #print(file)
            table_name = []
            file_dict = {"SpName": file, "TableName":""}
            dict_list.append(file_dict)
            for line in contents.splitlines():
                if ("FOND_ID." in line.replace(" ", "") or 
                        "ABST_ID." in line.replace(" ", "") and "-" not in line
                    or "STAG_ID." in line.replace(" ","")):
                        FirstClean = line.replace("TRUNCATE TABLE","").replace("INSERT INTO","").replace("FROM","").replace("\t","").replace(";","")
                        for line2 in FirstClean.split(sep="."):
                            if "ABST_ID" not in line2.replace(" ","") and "FOND_ID" not in line2.replace(" ","") and "STAG_ID" not in line2.replace(" ",""):
                                if " " not in line2.lstrip():
                                    p = line2+";"[:line2.find(";")]
                                    p1 = p.replace(" ","")
                                    if p1.startswith("STAG_") or p1.startswith("FOND_") or p1.startswith("ABST_") or p1.startswith("USP_"):
                                        if "(" in p1:
                                            table_name.append(p1[:p1.find("(")])
                                            file_dict["TableName"] = table_name
                                        else:
                                            print("step2 = "+p1)
                                            table_name.append(p1)
                                            file_dict["TableName"] = table_name
                                else:
                                    line_fix = line2.lstrip()[:line2.lstrip().find(" ")]
                                    if line_fix.startswith("STAG_") or line_fix.startswith("FOND_") or line_fix.startswith("ABST_") or line_fix.startswith("USP_"):
                                            if "(" in line_fix:
                                                table_name.append(line_fix[:line_fix.find("(")])
                                                file_dict["TableName"] = table_name
                                            else:
                                                table_name.append(line_fix)
                                                file_dict["TableName"] = table_name
df = pd.DataFrame(dict_list).explode('TableName')
print(tabulate(df, headers='keys', tablefmt='psql'))
