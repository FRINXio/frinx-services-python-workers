import json
import resource
import sys
from typing import Any

import pyseccomp as seccomp

MEMORY_LIMIT = 64 * 1024 * 1024  # 64kb
CPU_TIME_LIMIT = 1  # 1sec
WRITE_LIMIT = 512  # 512bytes


def set_mem_limit() -> None:
    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_LIMIT, MEMORY_LIMIT))
    resource.setrlimit(resource.RLIMIT_CPU, (CPU_TIME_LIMIT, CPU_TIME_LIMIT))
    resource.setrlimit(resource.RLIMIT_FSIZE, (WRITE_LIMIT, WRITE_LIMIT))


def drop_perms() -> None:
    sec_filter = seccomp.SyscallFilter(seccomp.ERRNO(seccomp.errno.EPERM))
    sec_filter.add_rule(seccomp.ALLOW, 'write', seccomp.Arg(0, seccomp.EQ, sys.stdout.fileno()))
    sec_filter.add_rule(seccomp.ALLOW, 'write', seccomp.Arg(0, seccomp.EQ, sys.stderr.fileno()))
    sec_filter.load()


def execute(code: str) -> None:
    ex_locals: dict[str, Any] = {}
    exec(code, None, ex_locals)
    print(json.dumps(ex_locals.get('result', '')))


if __name__ == '__main__':
    set_mem_limit()
    drop_perms()
    execute(sys.argv[1])
