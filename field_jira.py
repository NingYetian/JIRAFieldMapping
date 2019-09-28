import ...


CONN_STR = (
    "DRIVER={SQL Server Native Client 11.0};"
    "SERVER=cds-hub-10.ad.ea.com;"
    "DATABASE=MDS;"
    "Trusted_Connection=yes;"
)

STD_FIELDS = {
    "bugtitle": (475, "JiraIssue", "SUMMARY"),
    "timeestimate": (476, "JiraIssue", "TIMEESTIMATE"),
    "timeoriginalestimate": (477, "JiraIssue", "TIMEORIGINALESTIMATE"),
    "timespent": (478, "JiraIssue", "TIMESPENT"),
    "currentowner": (1277, "cwd_user_assignee", "display_name"),
    "creator": (1296, "cwd_user_creator", "display_name"),
    "submitter": (1315, "cwd_user_reporter", "display_name"),
    "status": (1332, "Issuestatus", "pname"),
    "bugtype": (1340, "IssueType", "pname"),
    "priority": (1360, "Priority", "pname"),
    "resolution": (1369, "Resolution", "pname"),
    "affectedversion": (1876, "AffectedVersions", "AffectedVersions"),
    "affectedversions": (1876, "AffectedVersions", "AffectedVersions"),
    "component": (1882, "Component", "Component"),
    "components": (1882, "Component", "Component"),
    "fixversion": (1885, "IssueFixVersion", "FixVersion"),
    "fixversions": (1885, "IssueFixVersion", "FixVersion"),
    "label": (1891, "Label", "LABEL"),
    "labels": (1891, "Label", "LABEL"),
}


@dataclass
class JiraStandardField:
    """
    Jira standard field with associated details.

    Parameters
    ----------
    staging_entity: str
        Entity in the BugHub_StagingFields table in MDS to which the field belongs to
    staging_name : str
        Name of the field in the BugHub_StagingFields table in MDS
    bughub_name : str, optional
        Name of the field in BugHub
    """

    staging_entity: str
    staging_name: str
    bughub_name: Optional[str] = None

    @classmethod
    def frombughub(cls, bughub_name: str):
        """
        Creates a JiraStandardField from its name in BugHub

        Parameters
        ----------
        bughub_name : str
            Name of the field in BugHub
        """
        try:
            match = STD_FIELDS[bughub_name.lower()]
        except KeyError:
            raise ValueError(f"Standard field not found: {bughub_name}")

        return cls(
            staging_entity=match[1], staging_name=match[2], bughub_name=bughub_name
        )

    @classmethod
    def fromstaging(cls, scode: int):
        """
        Creates a StandardField from its staging field code in staging field

        Parameters
        ----------
        scode: int
            staging code in BugHub_StagingFields table in MDS to which the field belongs to
        """
        conn = pyodbc.connect(CONN_STR)
        qry = """
            SELECT
                Staging.entity as [staging_entity],
	            Staging.Name as [staging_name]
            FROM [mds].[mdm].[BugHub_StagingFields] as Staging
            WHERE ID = ?
        """
        cursor = conn.cursor()
        row = cursor.excute(qry, scode).fetchone()

        return cls(
            staging_entity=row.staging_entity,
            staging_name=row.staging_name
        )


@dataclass
class JiraCustomField:
    """
    Jira custom field with associated details.

    Parameters
    ----------
    id : int
        Custom field ID
    type : str
        Custom field type key suffix, e.g. 'select'.
    source_name : str
        Name of the field in the source
    bughub_name : str, optional
        Name of the field in BugHub. None if not mapped.
    """

    id: int
    type: str
    source_name: str
    bughub_name: Optional[str] = None

    @classmethod
    def fromid(
        cls,
        sourceid: int,
        customfieldid: int,
        bughub_name: Optional[str] = None,
        engine: Optional[Engine] = None,
    ):
        """
        Creates a JiraCustomField from a custom field ID and source ID

        Parameters
        ----------
        sourceid : int
            The source ID in BugHub
        customfieldid : int
            The custom field ID in the source.
        bughub_name : str, optional
            Name of the field in BugHub
        engine
            An Engine connected to the source server. If None, a new
            engine will be created.
        """
        if not engine:
            engine = create_source_engine(sourceid)
        qry = text(
            "SELECT id, customfieldtypekey, cfname FROM customfield WHERE id = :id"
        )
        conn = engine.connect()
        result = conn.execute(qry, id=customfieldid).first()
        if result is None:
            raise ValueError(f"Field {customfieldid} not found in server {sourceid}")
        return cls(
            id=int(result.id),
            type=result.customfieldtypekey.rsplit(":", 1)[-1],
            source_name=result.cfname,
            bughub_name=bughub_name,
        )

