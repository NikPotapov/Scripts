from atlassian import Jira
import pandas as pd
import os, getpass, dotenv
import logging


# init module logger wich inherits the config
logger = logging.getLogger(__name__)
dotenv.load_dotenv()

# ------------------------------------------------------------
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
epic_customfield = os.getenv("epic_customfield")
repository_customfield = os.getenv("repository_customfield")
start_number = os.getenv("start_number")
limit_number = os.getenv("limit_number")
username = os.getenv("username")
password = os.getenv("password")
# ----------------------------------------------------------

if not username or not password:
    username = input("Please enter username: ")
    password = getpass.getpass("Password: ")


def parse_test(test, epic_customfield, repository_customfield):
    """
    Parse one test project for filtering special parameters like as name, status...

    Parameters
    ----------
    test : list
        List with dictionaries inside has special params for fields grabbing
    epic_customfield : string
        Variable is customized from .env file based on environment
    repository_customfield :string
        Variable is customized from .env file based on environment
    """

    remove = "[']"
    key = test["key"]
    status = test["fields"]["status"]["name"]
    priority = test["fields"]["priority"]["name"]
    name = test["fields"]["summary"]

    try:
        reporter_id = test["fields"]["reporter"]["name"]
    except:
        reporter_id = None
    try:
        test_issue_links = search_issue_links(test["fields"]["issuelinks"])
    except:
        test_issue_links = None
    try:
        assignee_id = test["fields"]["assignee"]["name"]
    except:
        assignee_id = None
    try:
        fix_version = test["fields"]["fixVersions"][0]["name"]
    except:
        fix_version = None
    try:
        component = test["fields"]["components"][0]["name"]
    except:
        component = None
    try:
        label = str(test["fields"]["labels"])
        for char in remove:
            label = label.replace(char, "")
    except:
        label = None
    try:
        epic = test["fields"]["customfield_" + epic_customfield]
    except:
        epic = None
    try:
        repository = test["fields"]["customfield_" + repository_customfield]
    except:
        repository = None

    return (
        key,
        name,
        status,
        priority,
        fix_version,
        component,
        label,
        epic,
        assignee_id,
        reporter_id,
        repository,
        test_issue_links,
    )


def insert_into_df(row_items, df):
    """
    Inserting into dataFrame test-data project by one row

    Parameters
    ----------
    row_items : list
        List has filtered data and prepared items for inserting into
    df : DataFrame
        DataFrame is processing test-data to insert
    """

    df = pd.concat(
        [
            df,
            pd.DataFrame.from_records(
                [
                    {
                        "Key": row_items[0],
                        "Name": row_items[1],
                        "Status": row_items[2],
                        "Priority": row_items[3],
                        "Fix_Version": row_items[4],
                        "Component": row_items[5],
                        "Labels": row_items[6],
                        "Epic_Link": row_items[7],
                        "Assignee_ID": row_items[8],
                        "Reporter_ID": row_items[9],
                        "Test_Repository_Path": row_items[10],
                        "Test_Issue_Links": row_items[11],
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    return df


def search_issue_links(issues_link_list):
    """Search issue link tests if exist in the list
    with nested dictionaries inisde

    Parameters
    ----------
    issues_link_list: list
        List contains values regarding all issue links
    """

    issue_links = ""
    for dictionary in issues_link_list:
        for value in dictionary.values():
            if isinstance(value, dict):
                for key, val in value.items():
                    if key == "outward" and val == "tests":
                        issue_key = dictionary["outwardIssue"]["key"]
                        issue_links = str(issue_key + " " + issue_links)
    return issue_links


def filter_df(
    df, status, priority, fix_version, component, label, epic_link, test_repository_path
):
    """Filter dataFrame based on argument variables, droping rows if don't contain arg value

    Parameters
    ----------
    df: dataFrame
        dataFrame represents data filled from tests
    str: status
        Status input argument defined by user
    str: priority
            Priority input argument defined by user
    str: fix_version
            Fix Version input argument defined by user
    str: component
            Component input argument defined by user
    str: label
            Label input argument defined by user
    str: epic_link
            Epic Link input argument defined by user
    str: test_repository_path
            Test Repository Path input argumnet defined by user
    """

    if status:
        df.drop(df[df["Status"] != status].index, inplace=True)
        logger.info(f"Status is filtred by {status} value.")
    if priority:
        df.drop(df[df["Priority"] != priority].index, inplace=True)
        logger.info(f"Priority is filtred by {priority} value.")
    if fix_version:
        df.drop(df[df["Fix_Version"] != fix_version].index, inplace=True)
        logger.info(f"Fix Version is filtred by {fix_version} value.")
    if component:
        df.drop(df[df["Component"] != component].index, inplace=True)
        logger.info(f"Component is filtred by {component} value.")
    if label:
        df.drop(df[df["Labels"] != label].index, inplace=True)
        logger.info(f"Label is filtred by {label} value.")
    if epic_link:
        df.drop(df[df["Epic_Link"] != epic_link].index, inplace=True)
        logger.info(f"Epic Link is filtred by {epic_link} value.")
    if test_repository_path:
        df.drop(
            df[df["Test_Repository_Path"] != test_repository_path].index, inplace=True
        )
        logger.info(f"Test repository Path is filtred by {test_repository_path} value.")
    return df


def save_excel(df, path):
    """Save file as Excel format to mentioned directory

    Parameters
    ----------
    df : DataFrame
        DataFrame is representing prepared all test-data.
    path : String
        Directory for deliverying file
    """

    writer = pd.ExcelWriter(path)
    df.to_excel(writer, "TESTS", index=False)
    writer.save()
    logger.info(
        "DataFrame is written to Excel file with "
        + str(len(df))
        + " rows successfully."
    )


def jira_test_download(
    path,
    status,
    priority,
    fix_version,
    component,
    label,
    epic_link,
    test_repository_path,
):
    """Export jira tests to a file

    Parameters
    ----------
    path: str
        String representation of the output file path
    """

    logger.info("Loggging into Jira")

    jira = Jira(
        url=JIRA_BASE_URL,
        username=username,
        password=password,
        verify_ssl=False,
    )

    logger.info(f"Downloading tests from JIRA {JIRA_BASE_URL}")
  
    projects = jira.get_all_project_issues(
        project="LDT",
        fields="*all",
        start=start_number,
        limit=limit_number
        )

    column_names = [
        "Key",
        "Name",
        "Status",
        "Priority",
        "Fix_Version",
        "Component",
        "Labels",
        "Epic_Link",
        "Assignee_ID",
        "Reporter_ID",
        "Test_Repository_Path",
        "Test_Issue_Links",
    ]

    df = pd.DataFrame(columns=column_names)

    for test in projects:
        
        type_project = str(test["fields"]["issuetype"]["name"]).strip()
        
        if type_project == "Test":

            row_items = parse_test(test, epic_customfield, repository_customfield)

            df = insert_into_df(row_items, df)

    filter_df(
        df,
        status,
        priority,
        fix_version,
        component,
        label,
        epic_link,
        test_repository_path,
    )

    save_excel(df, path) if len(df) > 0 else logger.info(
        "Excel is not saved, dataframe is empty!"
    )
