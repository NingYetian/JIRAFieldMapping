import query_builder as qb
import pyodbc
from query_builder.project import Project
from query_builder.fields.jira import JiraCustomField
from query_builder.fields.devtest import DevTestCustomField


HUB_10_CONN_STR = ("DRIVER={SQL Server Native Client 11.0};"
                   "SERVER=cds-hub-10.ad.ea.com;"
                   "DATABASE=MDS;"
                   "Trusted_Connection=yes;")

def jira_mapping_feasibility(sourceid, projectid, fieldid, bughubfieldname):
    """
    determine the Jira mapping feasibility by checking two conditions:
    comparing the data type and the length between the source Jira field and the BugHub
    field

    Parameters
    ----------
    sourceid : int
        The source server ID
    projectid : int
        The project ID
    fieldid : int
        The jira custom field ID
    bughubfieldname : str
        The field in BugHub as target field

    Returns
    -------
    boolean
        if yes, mapping is feasible, otherwise not feasible
    """
   # initiate a boolean variable to determine Jira mapping feasibility
    feasibility = False

    # get Jira field type and length info through extracting info from the dataframe
    engine = qb.create_source_engine(sourceid)

    project = Project(sourceid, projectid)

    # get Jira custom field with associated detials
    jiraCustomFieldlDetails = [JiraCustomField.fromid(sourceid,
                                                    fieldid,
                                                    bughubfieldname,
                                                    engine)]

    # instantiate a query object from the Jira class
    query = qb.JiraQuery(engine, project, custom_fields=jiraCustomFieldlDetails)

    df = query.to_pandas()

    # create a variable to store jira field's data type
    jira_type = df[bughubfieldname].dtype.kind

    # to execute bughub field:
    conn = pyodbc.connect(HUB_10_CONN_STR)
    qry = '''
        SELECT DataType, DataLength
        FROM [MDS].[mdm].[BugHub_TargetFields] AS [target]
		WHERE [target].Entity = 'Bugs'
			AND [target].IsDeprecated = 0
            AND [target].[Database] = 'BugHub'
			AND [target].[Schema] = 'staging'
            AND FieldName = ?
        '''
    cursor = conn.cursor()
    row = cursor.execute(qry, bughubfieldname).fetchone()

    # store bughub data type and length as a tuple in the row variable
    bughub_type = row.DataType
    bughub_length = int(row.DataLength)

    # to check data type feasibility between bughub and Jira
    type_feasible = False

    if ((bughub_type == 'varchar') or (bughub_type == 'nvarchar')):
        type_feasible = True
    elif (
            (bughub_type == 'bit')
            and (jira_type in ('b', 'i', 'U'))
        ):
        type_feasible = True
    elif (
            (bughub_type in ('datetime', 'datetime2'))
            and (jira_type in ('m', 'M'))
        ):
        type_feasible = True
    elif ((bughub_type == 'decimal')
          and (jira_type == 'f')):
        type_feasible = True
    else:
        type_feasible = False

    # determine mapping feasibility with taking data length into consideration
    if type_feasible:
        if jira_type in ('O', 'S', 'U'):
            jira_max_length = df[bughubfieldname].str.len().max()
            if bughub_length >= jira_max_length:
                feasibility = True
            else:
                feasibility = False
        else:
            feasibility = True

    return feasibility


def devtest_mapping_feasibility(sourceid, projectid,
                                datafieldid, bughubfieldname):
    """
    determine the DevTest mapping feasibility by checking two conditions:
    comparing the data type and the length between the DevTest field and the BugHub
    field

    Parameters
    ----------
    sourceid : int
        The source server ID in Bughub
    projectid : int
        The project ID
    entity : str : {'devtest_templates', 'devtest_tasks'}
        The corresponding type of DevTest entity
    datafieldid: int
        The Devtest field ID
    bughubfieldname : str
        The field in BugHub as target field

    Returns
    -------
    boolean
        if yes, mapping is feasible, otherwise not feasible
    """

    # initiate a bollean variable to determine DevTest mapping feasibility
    feasibility = False

    # get DevTest field type and length info through extracting info from the dataframe
    engine = qb.create_source_engine(sourceid)

    project = Project(sourceid, projectid)

    devtestCustomFieldDetails = [DevTestCustomField.fromid(datafieldid,
                                                            project,
                                                            bughubfieldname,
                                                            engine)]

    # instantiate a query object form the Devtest class
    query = qb.DevTestQuery(engine, project,
                            custom_fields=devtestCustomFieldDetails)

    df = query.to_pandas()

    # Store DevTest data type as variables
    devtest_type = df[bughubfieldname].dtype.kind

    # to excute bughub field:
    conn = pyodbc.connect(HUB_10_CONN_STR)
    qry = '''
        SELECT DataType, DataLength
        FROM [MDS].[mdm].[BugHub_TargetFields] AS [target]
		WHERE [target].Entity in ('Testtask', 'Testcase')
			AND [target].IsDeprecated = 0
            AND [target].[Database] = 'BugHub'
			AND [target].[Schema] = 'staging'
            AND FieldName = ?
        '''
    cursor = conn.cursor()
    row = cursor.execute(qry, bughubfieldname).fetchone()

    # store the bughub type and legnth variable
    bughub_type = row.DataType
    bughub_length = int(row.DataLength)

    # initiate a boolean to check if data type matches between BugHub and DevTest fields
    type_feasible = False

    if bughub_type in ('varchar', 'nvarchar'):
        type_feasible = True
    elif (
            (bughub_type == 'bit')
            and (devtest_type in ('b', 'i', 'U'))
        ):
        type_feasible = True
    elif (
            (bughub_type in ('datetime', 'datetime2'))
            and (devtest_type in ('m', 'M'))
        ):
        type_feasible = True
    elif ((bughub_type == 'decimal')
          and (devtest_type == 'f')
         ):
        type_feasible = True
    else:
        type_feasible = False

    # determine mapping feasibility with taking data length into consideration
    if type_feasible:
        if devtest_type in ('O', 'S', 'U'):
            devtest_max_length = int(df[bughubfieldname].str.len().max())
            if bughub_length >= devtest_max_length:
                feasibility = True
            else:
                feasibility = False
        else:
            feasibility = True

    return feasibility
