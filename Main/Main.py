import os

directory = "/home/user/python-project/File/SP"
files = os.listdir(directory)

for file in files: #filter an array or objects using files[:1]
    with open(os.path.join(directory, file), "r") as f:
        contents = f.read()
        print(file)
        for line in contents.splitlines():
            if ("FOND_ID." in line.replace(" ", "") or 
                    "ABST_ID." in line.replace(" ", "") and "-" not in line
                or "STAG_ID." in line.replace(" ","")):
                    FirstClean = line.replace("TRUNCATE TABLE","").replace("INSERT INTO","").replace("FROM","").replace("\t","").replace(";","")
                    for line2 in FirstClean.split(sep="."):
                        if "ABST_ID" not in line2.replace(" ","") and "FOND_ID" not in line2.replace(" ",""):
                            if " " not in line2:
                                print(line2+";"[:line2.find(";")])
                            else:
                                print(line2[:line2.find(" ")])

