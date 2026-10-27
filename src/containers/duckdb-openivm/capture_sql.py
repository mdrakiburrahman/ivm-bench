#!/usr/bin/env python3
"""Diagnostic CLI wrapper: capture full SQL across native helper connections."""

import json
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid


def logging_sql(path):
    literal = "'" + str(path).replace("'", "''") + "'"
    # COPY suppresses the enable_logging table's result without changing CLI
    # output mode. Selecting QueryLog first also suppresses its setup warning.
    return (
        "SET logging_mode='ENABLE_SELECTED';\n"
        "SET enabled_log_types='QueryLog';\n"
        "COPY (SELECT * FROM enable_logging('QueryLog', storage='file', "
        f"storage_path={literal})) TO '/dev/null';\n"
    )


def main():
    binary = Path(__file__).with_name('duckdb.real')
    args = sys.argv[1:]
    if (os.environ.get('OPENIVM_QUERY_LOG') != '1' or sys.stdin.isatty() or
            any(arg in ['--version', '-version', '-v', '--help', '-help', '-h'] for arg in args)):
        os.execv(str(binary), [str(binary), *args])

    invocation = uuid.uuid4().hex
    work = Path(os.environ.get('DUCKDB_OPENIVM_WORK_DIR', '/data/processed/duckdb-openivm'))
    directory = work/'_sql_trace'/invocation
    directory.mkdir(parents=True)
    prefix = logging_sql(directory/'queries.csv')
    commands = [i+1 for i,arg in enumerate(args[:-1]) if arg in ['-c', '--command']]
    submitted = '' if commands else sys.stdin.buffer.read().decode('utf-8')
    (directory/'request.json').write_text(json.dumps({
        'invocation_id': invocation, 'created_at': time.time(),
        'argv': args, 'stdin_sql': submitted,
        'command_sql': [args[i] for i in commands],
    }, indent=2)+'\n')

    if commands:
        args[commands[0]] = prefix + args[commands[0]]
    else:
        # exec preserves PID, signals, return codes and inherited output streams.
        # A file-backed stdin avoids any producer/consumer pipe deadlock.
        source = tempfile.TemporaryFile()
        source.write((prefix+submitted).encode('utf-8'))
        source.seek(0)
        os.dup2(source.fileno(), sys.stdin.fileno())
    os.execv(str(binary), [str(binary), *args])


if __name__ == '__main__':
    main()
