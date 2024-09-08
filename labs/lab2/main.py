import glob
import os
import signal
import time
import argparse
from enum import Enum

from constants import STREAMS, NUM_MAPPERS, MAPPER_PORTS, REDUCER_PORTS
from _coordinator import Coordinator
from mrds import MyRedis
from mylog import Logger

logging = Logger().get_logger()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "recovery_test_mode",
      type=Coordinator.RecoveryTestModes,
      choices=list(Coordinator.RecoveryTestModes),
  )
  opts = parser.parse_args()

  Logger()
  rds = MyRedis()
  pattern = "csv_files/*.csv"

  logging.info("adding files to streams")
  j: int = 1
  for file in glob.glob(pattern):
    #   logging.info(f"adding {file} to stream {STREAMS[j%NUM_MAPPERS]}" )
      rds.add_file(STREAMS[j % NUM_MAPPERS], file, j)
      j += 1
  logging.info("done adding files to streams")
  C = Coordinator(opts.recovery_test_mode)
  C.start()
  C.join()
