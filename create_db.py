import sqlite3
from sqlite3 import Error
import numpy as np
import pandas as pd
import os
import xml.etree.ElementTree as ET

os.chdir(r'C:\Users\marti\OneDrive\Plocha\research_projects\proverky')


name_space = {'struct_type': 'https://proverki.gov.ru/opendata/3.0/20180523'}

another_tree = ET.parse('xml_files\data-20190218-structure-20180523.xml')
len(another_tree.getroot().getchildren())
another_tree.getroot().attrib

tree_small = ET.parse('xml_files\data-20200403-structure-20180523.xml')
root_small = tree.getroot()

root_small[5][4]
root_small.tag
root_small.attrib

len(root_small.getchildren())


tree = ET.parse('xml_files\data-20201018-structure-20180523.xml')

root = tree.getroot()

len(root.getchildren())

root.tag
root.attrib
root[128].attrib
[x for x in root[125]]

root[12].find(r'struct_type:I_SUBJECT', name_space).attrib

root[125][6].attrib

root[125]
root[125].findall(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR')[0].get('POSITION')
root[125].find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR').attrib
list_of_children = []
for child in root:
    list_of_children.append(child)

insp_position = []
insp_name = []
insp_type = []
for inspection in list_of_children:
    try:
        position = inspection.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR').get('POSITION')
        full_name = inspection.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR').get('FULL_NAME')
        type_name = inspection.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR').get('INSPECTOR_TYPE_NAME')
        insp_position.append(position)
        insp_name.append(full_name)
        insp_type.append(type_name)
    except AttributeError:
        insp_position.append(None)
        insp_name.append(None)
        insp_type.append(None)

org_name_list = []
subject_name_list = []
subject_inn_list = []
for inspection in list_of_children:
    try:
        subject_name = inspection.find(r'struct_type:I_SUBJECT', name_space).get('ORG_NAME')
        subject_inn = inspection.find(r'struct_type:I_SUBJECT', name_space).get('INN')
        #type_name = inspection.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_AUTHORITY').get('INSPECTOR_TYPE_NAME')
        subject_name_list.append(subject_name)
        subject_inn_list.append(subject_inn)
    except AttributeError:
        subject_name_list.append(None)
        subject_inn_list.append(None)

for inspection in list_of_children:
    try:
        org_name = inspection.find(r'struct_type:I_AUTHORITY', name_space).get('FRGU_ORG_NAME')
        #org_ = inspection.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_AUTHORITY').get('FULL_NAME')
        #type_name = inspection.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_AUTHORITY').get('INSPECTOR_TYPE_NAME')
        org_name_list.append(org_name)
    except AttributeError:
        org_name_list.append(None)


list_of_children[2]
pd.Series(subject_inn_list).value_counts()
subject_inn_list[5]
pd.Series(insp_name).value_counts()
pd.Series(insp_type).value_counts()


root.find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR').get('POSITION')


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_connection(r"db\pythonsqlite.db")
