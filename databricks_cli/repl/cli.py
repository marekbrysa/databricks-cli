import click
from click.exceptions import Exit, ClickException
from prompt_toolkit import PromptSession

import databricks_cli
from databricks_cli.configure.config import debug_option
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, short_help='Interactive CLI REPL')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@click.option('--profile', required=False, default=None,
              help='CLI connection profile to use. The default profile is "DEFAULT".')
@debug_option
def repl_group(profile):  # pragma: no cover
    """
    Interactive CLI REPL.
    """
    ctx = {}
    if profile is not None:
        ctx['profile'] = profile

    ctx_options = ['job-id', 'profile', 'cluster-id', 'run-id']

    session = PromptSession()

    click.echo(
        'Welcome to the Databricks CLI REPL!\n'
        'Set context by doing e.g. `--job-id 123`, unset with `--job-id`.\n'
        'Use Databricks CLI commands as usual and parameters for {} will be '
        'provided from the current context.'.format(", ".join(ctx_options)))
    click.echo('Press Ctrl-D to exit.')

    while True:
        click.echo()
        try:
            cmd = session.prompt('{} > '.format(str(ctx)))
        except KeyboardInterrupt:
            continue
        except EOFError:
            break

        cmdline = cmd.split()
        if len(cmdline) == 0:
            continue
        first = cmdline[0]
        second = None
        if len(cmdline) > 1:
            second = cmdline[1]

        ctx_option = first[2:]
        if ctx_option in ctx_options:
            if second is None:
                ctx.pop(ctx_option, None)
            else:
                ctx[ctx_option] = second
        else:
            try:
                click_ctx = databricks_cli.cli.cli.make_context("databricks-cli", cmdline)
                cmd_name, group1, args = databricks_cli.cli.cli.resolve_command(click_ctx, cmdline)
                if len(args) == 0 or group1 is None:
                    click.echo("Command not found.", err=True)
                    continue
                cmd_name, cmd, args = group1.resolve_command(click_ctx, args)
                if cmd is None:
                    click.echo("Command not found.", err=True)
                    continue
                for param in cmd.params:
                    param_name = param.name.replace('_', '-')
                    if param_name in ctx:
                        cmdline.append('--{}'.format(param_name))
                        cmdline.append(ctx[param_name])
                click_ctx = databricks_cli.cli.cli.make_context("databricks-cli", cmdline)
                databricks_cli.cli.cli.invoke(click_ctx)
            except Exit:
                pass
            except ClickException as e:
                click.echo(e, err=True)
