import os
import time
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

load_dotenv()


class GoldLayer:

    def __init__(self):
        pass

    def trigger_databricks_job(self, job_id: int, load_date: str):

        databricks_pat = os.getenv("Databricks_PAT")
        databricks_host = os.getenv("Databricks_HOST")

        client = WorkspaceClient(
            host=databricks_host,
            token=databricks_pat
        )

        # Trigger the Databricks Job
        run = client.jobs.run_now(
            job_id=job_id,
            notebook_params={
                "load_date": load_date
            }
        )

        run_id = run.run_id

        print(f"Databricks Job Triggered Successfully")
        print(f"Run ID : {run_id}")

        # Poll until the job finishes
        while True:

            run_details = client.jobs.get_run(run_id)

            life_cycle_state = run_details.state.life_cycle_state
            result_state = run_details.state.result_state

            print(f"Life Cycle State : {life_cycle_state}")
            print(f"Result State     : {result_state}")

            # Job completed
            if life_cycle_state == RunLifeCycleState.TERMINATED:

                if result_state == RunResultState.SUCCESS:
                    print("Databricks Job completed successfully.")
                    break

                raise Exception(
                    f"Databricks Job failed. Result State: {result_state}"
                )

            # Optional: Handle internal errors
            elif life_cycle_state == RunLifeCycleState.INTERNAL_ERROR:
                raise Exception("Databricks Job ended with an INTERNAL ERROR.")

            # Wait before checking again
            time.sleep(30)

        return f"Databricks Job completed successfully. Run ID: {run_id}"


if __name__ == "__main__":

    obj = GoldLayer()

    response = obj.trigger_databricks_job(
        job_id=401738252534212,
        load_date="2026-06-29"
    )

    print(response)