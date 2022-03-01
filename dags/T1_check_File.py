import os
import datetime


# List file in Source


def check_statuts(Store_Date):
    # Store_Date = kwargs['Date']

    def list_File_Path(path):
        list_File = os.listdir(path)
        return list_File

    def check_new_file(entity, StoreDate):
        # dir_path = os.path.dirname(file)
        list_File = list_File_Path("dags/data/source/"
                                   + entity)

        if Store_Date not in list_File:
            print("No new " + entity+" in " + Store_Date)
            return 0
        print("Find new "+entity)
        return 1

    status = []
    entity_list = ["users", "transactions", "promotions"]
    for entity in entity_list:
        status.append(check_new_file(entity, Store_Date))

    return status