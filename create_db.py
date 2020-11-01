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

tree_small = ET.parse('xml_files\data-20190218-structure-20180523.xml')
root_small = tree_small.getroot()

root_small[5][4]
root_small.tag
root_small.attrib

len(root_small.getchildren())


tree = ET.parse(r'xml_files\2019_04_data-20201021-structure-20180523.xml')

root = tree.getroot()

len(root.getchildren())

root.tag
root.attrib
root[128].getchildren()
inspection.find(r'struct_type:I_SUBJECT', name_space).get('INN')
[child.tag for child in root[510]]
root[508].find(r'struct_type:I_CLASSIFICATION_LB', name_space).attrib
i_class_list = []
for inspection in list(root):
    try:
        i_class = inspection.find(r'struct_type:I_CLASSIFICATION', name_space).get('ISUPERVISION_NAME')
        i_class_list.append(i_class)
    except AttributeError:
        i_class_list.append(None)
pd.Series(i_class_list).value_counts()[:50]

test_purchses = root.findall(".//*[@ITYPE_NAME='Контрольная закупка']")
test_purchses[0]
[(child.tag, child.attrib) for child in test_purchses[5].iter()]

list(root[100])
[child.attrib for child in root[600].iter()]
[child.tag for child in root[508]]
list(root[508][1].iter())

pd.Series([child.attrib.get('ITYPE_NAME') for child in list(root)]).value_counts()
# Number 9000 is inspection of municipal adimistration
[(child.tag, child.attrib) for child in root[13000]]
[(child.tag, child.attrib) for child in root[80].iter()]
root[80].find(r'struct_type:I_OBJECT', name_space)[1].attrib
list(root[80].iter())
root[80]

[x for x in root[125]]
[child.attrib for child in root[508][4].iter()]

root[12].find(r'struct_type:I_SUBJECT', name_space).attrib

root[125][6].attrib

root[125]
root[125].findall(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR')[0].get('POSITION')
root[125].find(r'{https://proverki.gov.ru/opendata/3.0/20180523}I_INSPECTOR').attrib
list_of_children = []
for child in root:
    list_of_children.append(child)

def parse_it(tag, subtag, root=root, name_space=name_space):
    output = []
    for inspection in list(root):
        try:
            extr_element = inspection.find(r'struct_type:'+tag, name_space).get(subtag)
            output.append(extr_element)
        except AttributeError:
            output.append(None)
    return output

pd.Series(parse_it('I_APPROVE', 'IS_APPROVED')).value_counts()
pd.Series(parse_it('INSPECTION', 'ITYPE_NAME')).value_counts()
pd.Series(parse_it('I_AUTHORITY', 'FRGU_ORG_NAME')).value_counts()[100:150]


is_aproved_list = []
for inspection in list(root):
    try:
        is_aproved = inspection.find(r'struct_type:I_APPROVE', name_space).get('IS_APPROVED')
        is_aproved_list.append(is_aproved)
    except AttributeError:
        is_aproved_list.append(None)

pd.Series(is_aproved_list).value_counts()

is_aproved_list

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
pd.Series(org_name_list).value_counts()[50:100]
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
