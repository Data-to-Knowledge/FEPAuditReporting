# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 11:34:51 2019

@author: KatieSi

Monthly FEP Audit Submissions
Due Thursday 10am until April 30th.

Run at 8:30 am Thursdays
Output tables e-mailed to Carly Waddleton
"""

# Import packages
import pandas as pd
import pdsql
import numpy as np
from datetime import datetime, timedelta
from calendar import  month_name

# Set Parameters
ReportName= 'Monthly FEP Audit Submissions0'
RunDate = datetime.now()


AuditCol = [
        'gsID',
        'inspID',
        'ConsentNumber',
        'OverallGrade',
        'EffectiveFromDate',
        'PrimaryFarmType',
        'AuditDate'
        ]

AuditColNames = {
        'EffectiveFromDate' : 'Audit Submission Date',
         'OverallGrade': 'Grade',
         'inspID' : 'Inspection ID'
         }

AlertFilter = {
        'RecordDeleted': ['0']
         }

RecordDeleted = 0

ConsentCol = [
        'B1_ALT_ID',
        'MonOfficerDepartment'
        ]

ConsentColNames = {
        'B1_ALT_ID' : 'ConsentNumber',
        'MonOfficerDepartment' : 'Zone'
        }


InspCol = [
        'InspectionID',
        'InspectionCompleteDate',
        'InspectionStatus',
        'Subtype',
        'R3_DEPTNAME',
        ]

InspColNames = {
        'InspectionCompleteDate' : 'Date Processed',
        'R3_DEPTNAME' : 'Processing Department'
        }

# Query SQL tables : FEP Individual
Audit = pdsql.mssql.rd_sql(
                   'SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'D_ACC_FEP_IndividualSummary',
                   col_names = AuditCol,
                   where_op = 'AND',
                   where_in = AlertFilter,
                   date_col = 'AuditDate',
                   from_date = '2018-07-01'
                   )

#Format Table
Audit.rename(columns=AuditColNames, inplace=True)
Audit['ConsentNumber'] = Audit['ConsentNumber'].str.strip()
Audit['ConsentNumber'] = Audit['ConsentNumber'].str.upper()
Audit['Audit Submission Month'] = Audit['Audit Submission Date'].dt.month_name()
Audit['Audit Submission Year'] = Audit['Audit Submission Date'].dt.year


# Create Filter
Audit_List = Audit['ConsentNumber'].values.tolist()


# Query SQL tables : FEP Individual
Consent = pdsql.mssql.rd_sql('SQL2012PROD03',
                   'DataWarehouse', 
                   table = 'F_ACC_Permit',
                   col_names = ConsentCol,
                   where_in = {'B1_ALT_ID': Audit_List})

# Format Table
Consent.rename(columns=ConsentColNames, inplace=True)

Audit = pd.merge(Audit, Consent,
             on='ConsentNumber',
             how='left')

# Filter for last Month
Month = month_name[RunDate.month - 1]
Year = RunDate.year
AuditFilter = Audit[Audit['Audit Submission Month'] == Month]
AuditFilter = AuditFilter[AuditFilter['Audit Submission Year'] == Year]


AuditGrade = AuditFilter[['Zone','Grade']]

AuditFarm = AuditFilter[['PrimaryFarmType','Grade']]


# Create Pivot
#x = Audit.pivot_table(Audit,
#                  index=['Zone','Audit Submission Month'],
#                #  values=['gsID'],
#                  columns=['Grade'],
#                  aggfunc=[len],
#                  fill_value=0,
#                  dropna = False
#                  )
#
#x.query('Zone == ["Zone Ashburton "]')
#'Manager == ["Debra Henley"]'
#x= AuditGrade.pivot_table(AuditGrade,
#                  index=['Zone'],
#                  columns=['Grade'],
#                  aggfunc=[len],
#                  fill_value=0,
#                  dropna = False
#                  )
#
#y= AuditFarm.pivot_table(AuditFarm,
#                  index=['PrimaryFarmType'],
#                  columns=['Grade'],
#                  aggfunc=[len],
#                  fill_value=0
#                  )

ZoneGrades = Audit.groupby(['Audit Submission Month','Zone', 'Grade'])['Grade'].aggregate('count').unstack()
ZoneGrades.fillna(0, inplace= True)
ZoneGrades[list("ABCD")] = ZoneGrades[list("ABCD")].astype(int)
print(ZoneGrades)


FarmTypeGrades = Audit.groupby(['PrimaryFarmType','Audit Submission Month', 'Grade'])['Grade'].aggregate('count').unstack()
FarmTypeGrades.fillna(0, inplace= True)
FarmTypeGrades[list("ABCD")] = FarmTypeGrades[list("ABCD")].astype(int)
print(FarmTypeGrades)
x= FarmTypeGrades.Index(['April'])
x = FarmTypeGrades.reset_index()
x.reset_index(level='Audit Submission Month')


# https://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
x.to_csv('test.csv')

Audit.groupby(['Zone'])['Grade'].count()

## Create aggragate info
#WUA_Inspections_count = pd.DataFrame(
#        WUA_Inspections.groupby(['OfficerAssigned'])['InspectionID'].count())
##### Missing the (nospecific officer)
#
#WUA_Inspections_count.columns = ['Inspection Count']



#
#
## Print email
#print('Hi Carly,\n\nBelow are the WUA inspections still in process as of ',
#      datetime.strftime(datetime.now() - timedelta(days =1), '%d-%m-%Y'),
#      '\n\nThere are',len(WUAInspections.index),'inspections this week.',
#      '\n\n\n',WUAInspectionsCount,
#      '\n\n\nWater Use - Alert inspections still in process\n\n',
#      WUAInspections.to_string(index=False),
#      '\n\n* Note: This report was created at ',
#      RunDate.strftime("%H:%M %Y-%m-%d"), ' using ', ReportName,
#      '\n\nCheers,\nKatie\n'
#      )
#
#
#
