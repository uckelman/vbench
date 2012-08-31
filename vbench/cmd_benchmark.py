import hashlib
import subprocess
import timeit
import traceback

from cStringIO import StringIO
from numpy import empty, mean, median, std

from vbench.benchmark import Benchmark

class CmdBenchmark(Benchmark):
  """Base Benchmark class for commands to be exec'd. 

  setup: string or list of strings,
    The command to run before attempting the benchmark.
  code: string or list of strings,
    The command to benchmark.
  cleanup: string or list of strings,
    The command to run after the benchmark is complete.

  setup_env: dict with string keys and values,
    The environment with which to run the setup command.
  code_env: dict with string keys and values,
    The environment with which to run the benchmark command.
  cleanup_env: dict with string keys and values,
    The environment with which to run the cleanup command.

  repeat: nonnegative integer
    The number of times to run the benchmark.

  See also Benchmark for other keyword arguments.
  """
  def __init__(
    self,
    code, code_env=None, repeat=3,
    setup=None, setup_env=None,
    cleanup=None, cleanup_env=None,
    **kwargs
  ):
    super(CmdBenchmark, self).__init__(**kwargs)
    self.code = code.split() if isinstance(code, str) else code
    self.code_env = code_env
    self.repeat = repeat
    self.setup = setup.split() if isinstance(setup, str) else setup
    self.setup_env = setup_env
    self.cleanup = cleanup.split() if isinstance(cleanup, str) else cleanup
    self.cleanup_env = cleanup_env

  def _setup(self):
    if self.setup:
      subprocess.check_output(self.setup, env=self.setup_env)

  def _exec(self, ctx):
    pass

  def _cleanup(self, ctx):
    if self.cleanup:
      subprocess.check_output(self.cleanup, env=self.cleanup_env)

  def _error(self, ctx):
    buf = StringIO()
    traceback.print_exc(file=buf)
    return buf.getvalue()

  def run(self):
    try:
      ctx = self._setup()
      timings = empty(self.repeat) 
  
      try:
        for k in xrange(self.repeat):
          timings[k] = self._exec(ctx)

        return [{
          'succeeded': True,
          'timing_min': min(timings),
          'timing_max': max(timings),
          'timing_mean': mean(timings),
          'timing_median': median(timings),
          'timing_std': std(timings)
        }]

      except:
        return [{
          'succeeded': False,
          'traceback': self._error(ctx)
        }]
  
    finally:    
      self._cleanup(ctx)

  @property
  def checksum(self):
    return hashlib.md5(
      ' '.join(
        (
          self.name,
          (' '.join(self.setup) if self.setup else str(self.setup)),
          str(self.setup_env),
          ' '.join(self.code),
          str(self.code_env),
          (' '.join(self.cleanup) if self.cleanup else str(self.cleanup)),
          str(self.cleanup_env)
        )
      )
    ).hexdigest()


class CmdTimingBenchmark(CmdBenchmark):
  """Benchmark class for commands to be timed. 

  See CmdBenchmark for keyword arguments.
  """
  def __init__(self, *args, **kwargs):
    super(CmdTimingBenchmark, self).__init__(*args, **kwargs)

  def _exec(self, ctx):
    start = timeit.default_timer()
    subprocess.check_output(self.code, env=self.code_env)
    return timeit.default_timer() - start


class CmdGrepBenchmark(CmdBenchmark):
  """Benchmark class for commands to be timed. 

  pattern: re.RegexObject
    A pattern with which to grep the command output. The value of the
    first capturing group in the pattern will be the result of the
    benchmark run.

  See CmdBenchmark for other arguments.
  """
  def __init__(self, *args, **kwargs):
    self.pat = kwargs.pop('pattern')
    super(CmdGrepBenchmark, self).__init__(*args, **kwargs)

  def _exec(self, ctx):
    output = subprocess.check_output(
      self.code, env=self.code_env, stderr=subprocess.STDOUT
    )
    match = self.pat.search(output)
    if match:
      return float(match.group(1))
    else:
      raise Exception('no match!')


class CmdPerfBenchmark(CmdBenchmark):
  """Benchmark class for commands to be run with perf. 

  perf: string or list of strings,
    perf command and its arguments 

  See CmdBenchmark for other arguments.
  """
  def __init__(self, *args, **kwargs):
    self.perf = args[0].split() if isinstance(args[0], str) else args[0]
    args = args[1:]
    super(CmdPerfBenchmark, self).__init__(*args, **kwargs)

  def _exec(self, ctx):
    output = subprocess.check_output(self.perf + self.code, env=self.code_env)

    # lines from perf look like: value,key
    event = {}
    for line in output.split('\n'):
      col = line.split(',')
      if len(col) >= 2:
        event[col[1]] = col[0]
    return event

  def run(self):
    try:
      ctx = self._setup()
      events = []
  
      try:
        for k in xrange(self.repeat):
          event = self._exec(ctx)

          for key, val in event.iteritems():
            events.append({
              'succeeded': True,
              'series_name': key,
              'series_value': val
            })

        return events

      except:
        return [{
          'succeeded': False,
          'traceback': self._error(ctx)
        }]
  
    finally:    
      self._cleanup(ctx)

  def get_results(self, db_path, series_name):
    from vbench.db import BenchmarkDB
    db = BenchmarkDB.get_instance(db_path)
    return db.get_series(self.checksum, series_name)
