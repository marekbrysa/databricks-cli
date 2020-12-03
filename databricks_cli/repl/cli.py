import click
from click import ParamType

import databricks_cli
from databricks_cli.click_types import PipelineSpecClickType, PipelineIdClickType, JsonClickType, OutputClickType
from databricks_cli.version import print_version_callback, version
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.utils import pipelines_exception_eater, CONTEXT_SETTINGS, pretty_format, \
    error_and_quit
from prompt_toolkit import PromptSession
from tabulate import tabulate

class ReplContext(object):
    def __init__(self):
        self.state = {}
    def prompt(self):
        """items = []
        if 'object' in self.state:
            items.append(self.state['object'])
        if 'id' in self.state:
            items.append(self.state['id'])
        print(self.state)
        print(items)
        return ' '.join(items)"""
        str(self.state)

    def get_object(self):
        return self.state.get('object')

    def get_id(self):
        return self.state.get('id')

    def set_object(self, obj):
        self.state = {
            'object': obj
        }

    def set_id(self, obj_id):
        self.state['id'] = obj_id

@click.command(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with the Databricks Delta Pipelines.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@provide_api_client
def repl_group(api_client):  # pragma: no cover
    """
    Utility to interact with the Databricks pipelines.
    """
    ctx = {}
    session = PromptSession()

    while True:
        try:
            cmd = session.prompt('{} > '.format(str(ctx)))
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        words = cmd.split(' ')
        first = words[0]
        second = None
        if len(words) > 1:
            second = words[1]

        if first == 'jobs':
            databricks_cli.jobs.cli.list_cli.callback(output='table')
        elif first == 'job':
            if second == 'get':
                databricks_cli.jobs.cli.get_cli.callback(job_id=ctx.get('job_id'))
            else:
                ctx['job_id'] = second
        elif first == 'runs':
            databricks_cli.runs.cli.list_cli()
            databricks_cli.runs.cli.list_cli.callback(job_id=ctx.get('job_id'), output='table')
        elif first == 'run':
            if second == 'get':
                databricks_cli.runs.cli.get_cli.callback(run_id=ctx.get('run_id'))
            else:
                ctx['run_id'] = second
        else:
            ctx.pop('run_id', None)


