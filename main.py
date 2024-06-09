import sys
import argparse
from workflows import FirstWorkflow, SecondWorkflow, ThirdWorkflow, FourthWorkflow

def main(workflow_name):
    workflows = {
        "FirstWorkflow": FirstWorkflow,
        "SecondWorkflow": SecondWorkflow,
        "ThirdWorkflow": ThirdWorkflow,
        "FourthWorkflow": FourthWorkflow
    }

    if workflow_name in workflows:
        workflow = workflows[workflow_name]()
        workflow.runner()
    else:
        print(f"Unknown workflow: {workflow_name}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark ETL workflows.")
    parser.add_argument('workflow', type=str, help="Name of the workflow to run")

    args = parser.parse_args()
    main(args.workflow)
