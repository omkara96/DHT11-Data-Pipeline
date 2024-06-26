
import datetime
import json
import time
from typing import Dict, List, Optional, Union
 
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.exceptions import AirflowException, DagNotFound, DagRunAlreadyExists
from airflow.models import BaseOperator, BaseOperatorLink, DagBag, DagModel, DagRun
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.utils.state import State
from airflow.utils.types import DagRunType
 
 
class TriggerDagRunLink(BaseOperatorLink):
    """
    Operator link for TriggerDagRunOperator. It allows users to access
    DAG triggered by task using TriggerDagRunOperator.
    """
 
    name = 'Triggered DAG'
 
 
    def get_link(self, operator, dttm):
        query = {"dag_id": operator.trigger_dag_id, "execution_date": dttm.isoformat()}
        return build_airflow_url_with_query(query)
 
 
class DagRunOrder(object):
    def __init__(self, run_id=None, dag_id=None, payload=None):
        self.run_id = run_id
        self.dag_id = dag_id
        self.payload = payload
 
 
class CustomTriggerDagRunOperator(BaseOperator):
    """
    Triggers a DAG run for a specified ``dag_id``
 
    :param trigger_dag_id: the dag_id to trigger (templated)
    :type trigger_dag_id: str
    :param conf: Configuration for the DAG run
    :type conf: dict
    :param execution_date: Execution date for the dag (templated)
    :type execution_date: str or datetime.datetime
    :param reset_dag_run: Whether or not clear existing dag run if already exists.
        This is useful when backfill or rerun an existing dag run.
        When reset_dag_run=False and dag run exists, DagRunAlreadyExists will be raised.
        When reset_dag_run=True and dag run exists, existing dag run will be cleared to rerun.
    :type reset_dag_run: bool
    :param wait_for_completion: Whether or not wait for dag run completion. (default: False)
    :type wait_for_completion: bool
    :param poke_interval: Poke interval to check dag run status when wait_for_completion=True.
        (default: 60)
    :type poke_interval: int
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param failed_states: list of failed or dis-allowed states, default is ``None``
    :type failed_states: list
    :type python_callable: python callable
    """
 
    template_fields = ("trigger_dag_id", "execution_date", "conf", "params")
    template_fields_renderers = {
        "params": "json",
        "params.prev_start_date_success": "py",
        "params.curr_start_date": "py",
    }
 
    ui_color = "#ffefeb"
 
 
#    @property
#   def operator_extra_links(self):
#        """Return operator extra links"""
#       return [TriggerDagRunLink()]
 
 
    @apply_defaults
    def __init__(
        self,
        *,
        trigger_dag_id: str,
        conf: Optional[Dict] = None,
        execution_date: Optional[Union[str, datetime.datetime]] = None,
        reset_dag_run: bool = False,
        wait_for_completion: bool = False,
        poke_interval: int = 60,
        allowed_states: Optional[List] = None,
        failed_states: Optional[List] = None,
        python_callable=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.conf = conf
        self.reset_dag_run = reset_dag_run
        self.wait_for_completion = wait_for_completion
        self.poke_interval = poke_interval
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.failed_states = failed_states or [State.FAILED]
        self.python_callable = python_callable
 
        if not isinstance(execution_date, (str, datetime.datetime, type(None))):
            raise TypeError(
                "Expected str or datetime.datetime type for execution_date."
                "Got {}".format(type(execution_date))
            )
 
        self.execution_date: Optional[datetime.datetime] = execution_date  # type: ignore
 
        try:
            json.dumps(self.conf)
        except TypeError:
            raise AirflowException("conf parameter should be JSON Serializable")
 
    def execute(self, context: Dict):
        if isinstance(self.execution_date, datetime.datetime):
            execution_date = self.execution_date
        elif isinstance(self.execution_date, str):
            execution_date = timezone.parse(self.execution_date)
            self.execution_date = execution_date
        else:
            execution_date = timezone.utcnow()
 
        #run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)
        run_id = 'trig__{}'.format(execution_date)
        dag_id = self.trigger_dag_id
        dro = DagRunOrder(run_id=run_id, dag_id=dag_id)
        context['params'] = self.params
        self.log.info(f"dro.payload: {dro.payload}")
        if self.python_callable is not None:
            dro = self.python_callable(context, dro)
        try:
            # Ignore MyPy type for self.execution_date
            # because it doesn't pick up the timezone.parse() for strings
            dag_run = trigger_dag(
                dag_id=self.trigger_dag_id,
                run_id=run_id,
                conf=json.dumps(dro.payload),
                execution_date=self.execution_date,
                replace_microseconds=False,
            )
 
        except DagRunAlreadyExists as e:
            if self.reset_dag_run:
                self.log.info("Clearing %s on %s", self.trigger_dag_id, self.execution_date)
 
                # Get target dag object and call clear()
 
                dag_model = DagModel.get_current(self.trigger_dag_id)
                if dag_model is None:
                    raise DagNotFound(f"Dag id {self.trigger_dag_id} not found in DagModel")
 
                dag_bag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
 
                dag = dag_bag.get_dag(self.trigger_dag_id)
 
                dag.clear(start_date=self.execution_date, end_date=self.execution_date)
 
                dag_run = DagRun.find(dag_id=dag.dag_id, run_id=run_id)[0]
            else:
                raise e
 
        if self.wait_for_completion:
            # wait for dag to complete
            while True:
                self.log.info(
                    'Waiting for %s on %s to become allowed state %s ...',
                    self.trigger_dag_id,
                    dag_run.execution_date,
                    self.allowed_states,
                )
                time.sleep(self.poke_interval)
 
                dag_run.refresh_from_db()
                state = dag_run.state
                self.log.info("********state is %s", state)
                
                if state in self.allowed_states:
                    self.log.info("%s finished with allowed state %s", self.trigger_dag_id, state)
                    return
                if self.failed_states is not None:
                    if state in self.failed_states:
                        self.log.info("%s finished with state %s", self.trigger_dag_id, state)
                        raise AirflowException(f"{self.trigger_dag_id} failed with failed states {state}")