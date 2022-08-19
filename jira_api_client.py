"""Docstring for the jira_api_client.py module.

This module serves as an API client to manage the 
lifecycle of different jira resources.

"""
import sys, os
from argparse import Namespace
from pathlib import Path

# allow import of sibling packages
sys.path.append(os.path.dirname(sys.path[0]))

from common_utils.standard_logger import create_argparser, configure_root_logger
from common_utils.cli_subcommands import *
from api.test_download import *
from subparser import add_jira_api_args

# create the argparser with standard arguments
cli = create_argparser("default description")
subparsers = cli.add_subparsers(dest="subcommand")

test_arguments = [
    argument(
        "-o",
        "--output",
        help="Path to the output file",
        type=str,
        required=False,
        default="jira_tests_export.xlsx",
    ),
    argument(
        "-s",
        "--status",
        help="Input argument for filter status",
        type=str,
        required=False,
    ),
    argument(
        "-p",
        "--priority",
        help="Input argument for filter priority",
        type=str,
        required=False,
    ),
    argument(
        "-f",
        "--fix_version",
        help="Input argument for filter fix version",
        type=str,
        required=False,
    ),
    argument(
        "-c",
        "--component",
        help="Input argument for filter component",
        type=str,
        required=False,
    ),
    argument(
        "-l",
        "--label",
        help="Input argument for filter label",
        type=str,
        required=False,
    ),
    argument(
        "-e",
        "--epic_link",
        help="Input argument for filter epic link",
        type=str,
        required=False,
    ),
    argument(
        "-t",
        "--test_repository_path",
        help="Input argument for filter test repository path",
        type=str,
        required=False,
    ),
]


@subcommand(subparsers, test_arguments)
def test_download_wrapper(args: Namespace) -> None:
    """Download the tests from Jira

    Parameters
    ----------
    args : Namespace
        Argparse Namespace object containing the arguments
    """

    # define args parameters
    status = args.status
    priority = args.priority
    fix_version = args.fix_version
    component = args.component
    label = args.label
    epic_link = args.epic_link
    test_reposipory_path = args.test_repository_path

    # validate input path
    try:
        in_path = Path(args.output).resolve()
    except (OSError, RuntimeError) as e:
        logger.exception(f"Invalid path provided: {args.output}")
        sys.exit(1)

    # call the wrapped func
    jira_test_download(
        in_path,
        status,
        priority,
        fix_version,
        component,
        label,
        epic_link,
        test_reposipory_path,
    )


if __name__ == "__main__":
    cli.description = "Jira API Client"
    # parse args
    args = cli.parse_args()
    # configure the root logger based on args
    configure_root_logger(args)
    # init module logger wich inherits the config
    logger = logging.getLogger(__name__)

    if args.subcommand is None:
        cli.print_help()
    else:
        args.func(args)
        sys.exit(0)
