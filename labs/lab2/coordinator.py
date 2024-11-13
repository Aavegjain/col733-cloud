import datetime
from abc import ABC, abstractmethod
from enum import Enum, IntEnum
from multiprocessing import Process
import os
import signal
import socket
from typing import Final, Optional
import time
import threading
import logging
import queue

from mapper import Mapper
from constants import HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, CHECKPOINT_INTERVAL, NUM_MAPPERS, NUM_REDUCERS, STREAMS, \
  MAPPER_PORTS, REDUCER_PORTS, COORDINATOR_PORT
from reducer import Reducer
from message import Message, MT

from mylog import Logger
logging = Logger().get_logger()

class WorkerState:
  def __init__(self, idx: int, is_mapper: bool, addr: tuple[str, int]):
    self.idx: Final[int] = idx
    self.id: Final[str] = f"{'Mapper' if is_mapper else 'Reducer'}_{idx}"
    self.is_mapper: Final[bool] = is_mapper
    self.addr: Final[tuple[str, int]] = addr  # for udp connection ("localhost", port)

    self.last_hb_recvd: int = 0  # when did we receive last heartbeat (in seconds)
    self.last_cp_id: int = 0  # (0 initially) the id of last checkpoint a worker made
    self.recovery_id: int = 0
    self.is_done: bool = False  # only for mappers (have mappers finished reading their stream)
    self.last_checkpoint_done: bool = False # has a worker finished done its last checkpoint
    self.process: Process


  def reset(self):
  
    self.is_done = False
    self.last_checkpoint_done = False

  def start_worker(self, restart: bool = False) -> None:
    # restart flag: when we are trying to restart a worker. killing the process first.
    if restart:
      assert self.process is not None
      self.process.kill()

    if self.is_mapper:
      self.process = Mapper(self.idx, REDUCER_PORTS, MAPPER_PORTS[self.idx])
    else:
      self.process = Reducer(self.idx, REDUCER_PORTS[self.idx], NUM_MAPPERS)
    self.process.start()

class PHASE(IntEnum):
  CP = 1
  RECOVER_REDUCER = 2
  RECOVER_MAPPER = 3
  LAST_CP = 4
  EXITING = 5

class CoordinatorState():
  def __init__(self) -> None:
    self.phase: PHASE = PHASE.CP
    self.next_recovery_id: int = 0
    self.next_cp_id: int = 1
    self.workers: dict[str, WorkerState] = {}
    self.sock: Optional[socket.socket] = None
  
  # function to calculate min(last_checkpoint_id) for all workers
  # during recovery, all workers should recover from this checkpoint!
  def last_completed_checkpoint_id(self):
    checkpoint_id = 1_000_000  # random large number
    for _, ws in self.workers.items():
      if checkpoint_id > ws.last_cp_id:
        checkpoint_id = ws.last_cp_id
    if checkpoint_id == 0: # this means there exist some worker has not checkpointed yet
      checkpoint_id = -1 # for this, we use -1 as id, which means recover from beginning 
      # for workers, you would need to handle this appropriately when they receive a recover message with
      # last checkpoint id as -1. (HINT: Recover from beginning state)
    return checkpoint_id

class RcvMsg(ABC):
  @abstractmethod
  def update(self, state: CoordinatorState, source: str) -> Optional[PHASE]:
    """
      Do not implement this, this is an abstract method. Implement this in child classes.
      Input: state -> Current Coordinator state,
             source -> id of the worker from where this message was received from.
      Output: phase -> New phase to transition to after receiving the message.

      Semantics of the function is as follows:-
        1. Change the coordinator state (if required).
        2. Return the new PHASE if PHASE transition is necessary.
    """
    raise NotImplementedError

class HBRecvMsg(RcvMsg):
  def update(self, state: CoordinatorState, source: str) -> Optional[PHASE]:
    logging.info(f"Coordinator received heartbeat from {source}")
    state.workers[source].last_hb_recvd = int(time.time())
    return None

class CkptAckRecvMsg(RcvMsg):
  def __init__(self, checkpoint_id: str, recovery_id: str):
    self.checkpoint_id: Final[int] = int(checkpoint_id)
    self.recovery_id: Final[int] = int(recovery_id) 

  def all_acked(self, state):
    for _,ws in state.workers.items():
      if (ws.last_cp_id == state.next_cp_id):
        continue
      else:
        return False 
    return True 

  def update(self, state: CoordinatorState, source: str) -> Optional[PHASE]:
    # TODO: Take appropriate action when coordinator receives checkpoint_ack message from a worker\
    # update state of worker. update last_ckpt_id 
    # if all workers last_cp_id = next_cp_id, then increment next_cp_id and go to cp phase 
    # logging.info(f"Coord: received cp ack from {source}, ckpt_id = {self.checkpoint_id}, recv_id = {self.recovery_id}")
    if (state.next_recovery_id != self.recovery_id):
      # logging.warning(f"Coord: not handling in-flight messages, msg recv_id = {self.recovery_id}, coord recv_id = {state.next_recovery_id}")
      return None 
    state.workers[source].last_cp_id = self.checkpoint_id
    ws = state.workers[source] 
    # if (ws.is_mapper):
    #   if (ws.is_done):
    #     ws.done_is_checkpointed = True 
    #   else:
    #     ws.done_is_checkpointed = False 
    if (self.all_acked(state)):
      state.next_cp_id += 1 
      # logging.info(f"Coord: updating next cp id to {state.next_cp_id}") 
      return PHASE.CP 
    else:
      return None



class LastCkptAckRecvMsg(RcvMsg):
  def __init__(self, checkpoint_id: str, recovery_id: str):
    self.recovery_id = int(recovery_id)
    assert int(checkpoint_id) == 0

  def update(self, state: CoordinatorState, source: str) -> Optional[PHASE]:
    # TODO: Take appropriate action when coordinator receives last_checkpoint_ack message from a worker
    # logging.info(f"Received LAST_CKPT_ACK message from {source}")
    assert(state.next_cp_id == 0) 
    # logging.info(f"Coord: received LAST_CKPT_ACK from {source},  recv_id = {self.recovery_id}")
    if (state.next_recovery_id != self.recovery_id):
      # logging.warning(f"Coord: not handling in-flight messages, msg recv_id = {self.recovery_id}, coord recv_id = {state.next_recovery_id}")
      return None 

    state.workers[source].last_checkpoint_done = True
    # dont update last_cp_id, instead update last_checkpoint_Done bool
    # coz last_cp_id can make min cp id 0 :( 
    are_all_workers_done = True
    for _, ws in state.workers.items():
      if ws.is_mapper:
        assert(ws.is_done) 
        if ws.last_checkpoint_done == False:
          are_all_workers_done = False
      else:
        if (ws.last_checkpoint_done == False):
          are_all_workers_done = False 
    if are_all_workers_done:
      # logging.info(f"Coord: moving to exit phase")    
      return PHASE.EXITING
    else:
      # logging.info("Coord: not moving to exit phase, some workers pending")
      return None

class RecoveryAckRecvMsg(RcvMsg):
  def __init__(self, recovery_id: int):
    self.recovery_id = recovery_id

  def is_mapper(self, source):
    return source.startswith("Mapper")

  def all_recovered(self, state, check_mapper):
    for _,ws in state.workers.items():
      if (check_mapper):
        if (not ws.is_mapper):
          continue
      else:
        if (ws.is_mapper):
          continue
      if (ws.recovery_id == state.next_recovery_id):
        continue
      else:
        return False 
    return True

  def update(self, state: CoordinatorState, source: str) -> Optional[PHASE]:
    # TODO: Take appropriate action when coordinator receives recovery_ack message from worker
    # logging.info(f"Coord: recovery ack received from {source} with recv id {self.recovery_id}")
    if (self.recovery_id != state.next_recovery_id):
      # logging.info(f"Coord: received cp ack from {source}, ckpt_id = {self.checkpoint_id}, recv_id = {self.recovery_id}"
      # logging.warning(f"Coord: not handling in-flight messages, msg recv_id = {self.recovery_id}, coord recv_id = {state.next_recovery_id}")
      return None 
    if (self.is_mapper(source)):
      assert(state.phase == PHASE.RECOVER_MAPPER)
    else:
      assert(state.phase == PHASE.RECOVER_REDUCER)


    state.workers[source].recovery_id = self.recovery_id
    if (state.phase == PHASE.RECOVER_REDUCER):
      if (self.all_recovered(state, check_mapper = False)):
        # logging.info(f"Coord: all reducers returned recovery ack, going to RECOVER_MAPPER")
        return PHASE.RECOVER_MAPPER
      else:
        return None
    elif(state.phase == PHASE.RECOVER_MAPPER):
      if (self.all_recovered(state, check_mapper = True)):
        # logging.info(f"Coord: all mappers returned recovery ack, going to CP")
        return PHASE.CP
      else:
        return None
    else:
      logging.error(f"Coord: Error: received recovery ack msg in {state.phase} state")


class DoneRecvMsg(RcvMsg):
  def __init__(self, recovery_id: int):
    self.recovery_id = recovery_id


  def update(self, state: CoordinatorState, source: str) -> Optional[PHASE]:
    # logging.info(f"Received DONE message from {source}")
    if (self.recovery_id != state.next_recovery_id):
      # logging.info(f"Coord: received cp ack from {source}, ckpt_id = {self.checkpoint_id}, recv_id = {self.recovery_id}"
      # logging.warning(f"Coord: not handling in-flight messages, msg recv_id = {self.recovery_id}, coord recv_id = {state.next_recovery_id}")
      return None 

    assert state.workers[source].is_mapper == True, "Only mappers should send DONE message"
    state.workers[source].is_done = True
    are_all_mappers_done = True
    for _, ws in state.workers.items():
      if ws.is_mapper:
        if ws.is_done == False:
          are_all_mappers_done = False

    if are_all_mappers_done:
      # return PHASE.EXITING
      # TODO: Move to LAST_CP instead
      # logging.info(f"Coord: moving to last_Cp phase")
      state.next_cp_id = 0 
      return PHASE.LAST_CP
    return None

# converting received message, into appropriate message type
def msg_factory(message: Message) -> RcvMsg:
  if message.msg_type == MT.HEARTBEAT:
    return HBRecvMsg()
  elif message.msg_type == MT.CHECKPOINT_ACK:
    return CkptAckRecvMsg(message.kwargs["checkpoint_id"], message.kwargs["recovery_id"])
  elif message.msg_type == MT.DONE:
    return DoneRecvMsg( message.kwargs["recovery_id"])
  elif message.msg_type == MT.LAST_CHECKPOINT_ACK:
    return LastCkptAckRecvMsg(message.kwargs["checkpoint_id"],  message.kwargs["recovery_id"])
  elif message.msg_type == MT.RECOVERY_ACK:
    return RecoveryAckRecvMsg(message.kwargs["recovery_id"])
  else:
    logging.error(f"Unknown message type {message.msg_type}! Bad situtation.")
    raise Exception(f"Unknown message type {message.msg_type}! Bad situtation.")
    

class RecvThread(threading.Thread):
  def __init__(self, state: CoordinatorState, phase_queue: queue.Queue[PHASE]):
    super().__init__()
    self.state = state
    self.phase_queue = phase_queue
    signal.signal(signal.SIGINT, self.shutdown_handler)
    signal.signal(signal.SIGALRM, self.monitor_health)
    signal.setitimer(signal.ITIMER_REAL, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)

  def shutdown_handler(self, signum, frame):
    print("Shutting down...")
    signal.alarm(0)  # Disable the alarm
    exit(0)

  # monitoring health of all the workers. This handler will be invoked every HEARTBEAT_INTERVAL seconds.
  # Here we are checking that for all workers, (current_time - last_heartbeat_time <= HEARTBEAT_TIMEOUT)
  # If this is not the case, this means that the worker is down: We need to take appropriate actions.
  def monitor_health(self, signum, frame):
    logging.info("-- monitoring heartbeats --")
    recover = False
    for _, ws in self.state.workers.items():
      last = ws.last_hb_recvd
      cur = int(time.time())
      diff = cur - last
      logging.info(f"{_} sent last heartbeat {diff} seconds ago")
      if diff > HEARTBEAT_TIMEOUT:
        logging.critical(f"{_} is facing heartbeat timeouts")
        ws.start_worker(restart=True)
        recover = True
      
    if recover:
      # TODO: Take appropriate actions (the worker is down)
      
      self.state.next_recovery_id += 1

      time.sleep(0.2)
      # logging.info(f"Coord: starting recovery, putting in queue")
      # logging.info(f"Coord: moving from {self.state.phase.name} to RECOVERY_REDUCER") 

      # logging.info(f"Coord: new recovery id is {self.state.next_recovery_id}")
      self.state.phase = PHASE.RECOVER_REDUCER
      self.phase_queue.put(self.state.phase)
      
      


  def run(self):
    logging.info("RECV thread of coordinator started!")
    while True:
      response, _ = self.state.sock.recvfrom(1024)
      message = Message.deserialize(response)  # type = Message(msg_type, source, **kwargs)
      logging.info(f"Received message of type '{message.msg_type.name}' from '{message.source}'")
      msg = msg_factory(message)
      new_phase = msg.update(self.state, message.source)  # it will return new phase, in case there is some phase change
      if new_phase is not None: # adding new phase into queue, so that send thread can read and take appropriate actions
        # while(self.phase_queue.qsize() >= 1):
        #   pass 
        logging.info(f"Moving from {self.state.phase.name} to {new_phase.name}")
        self.state.phase = new_phase
        self.phase_queue.put(new_phase)

class SendMsg(ABC):
  def send(self, sock: Optional[socket.socket], addr: tuple[str, int]) -> None:
    assert sock is not None
    try:
      b_msg = self.encode()
      sock.sendto(b_msg, addr)
    except socket.error as e:
      logging.error(f"Error sending data: {e}")
    except Exception as e:
      logging.error(f"Unexpected error: {e}")

  @abstractmethod
  def encode(self) -> bytes:
    raise NotImplementedError

class CPMsg(SendMsg):
  def __init__(self, checkpoint_id: int, recovery_id: int):
    self.checkpoint_id = checkpoint_id
    self.recovery_id = recovery_id
  
  def encode(self) -> bytes:
    return Message(msg_type=MT.CHECKPOINT, source="Coordinator", checkpoint_id=self.checkpoint_id, recovery_id= self.recovery_id).serialize()


class RecoveryMsg(SendMsg):
  def __init__(self, checkpoint_id: int, recovery_id: int):
    self.checkpoint_id = checkpoint_id
    self.recovery_id = recovery_id

  def encode(self) -> bytes:
    return Message(msg_type=MT.RECOVER, source="Coordinator",
                   recovery_id= self.recovery_id, checkpoint_id=self.checkpoint_id).serialize()

class ExitMsg(SendMsg):
  def encode(self) -> bytes:
    return Message(msg_type=MT.EXIT, source="Coordinator").serialize()


class SendThread(threading.Thread):
  def __init__(self, id: str, pid: int, state: CoordinatorState, phase_queue: queue.Queue[PHASE]):
    super().__init__()
    self.id: Final[str] = id
    self.pid: Final[int] = pid
    self.state: Final[CoordinatorState] = state
    self.phase_queue: queue.Queue[PHASE] = phase_queue


  def send_marker_to_mappers(self, phase):
    if (not self.state.sock ):
      logging.critical("self.sock still None")
      return 
    msg_to_send = CPMsg(self.state.next_cp_id, self.state.next_recovery_id)
    msg_bytes = msg_to_send.encode() 
    for _,ws in self.state.workers.items():
      if (ws.is_mapper):
        # logging.info(f"Coord({phase}): sending marker to {ws.id}")
        self.state.sock.sendto(msg_bytes, ws.addr)
    # logging.info(f"Coord({phase}): sent all markers")

  def send_recovery(self, to_mappers):
    if (to_mappers):
      logging.info("Coord: recovery called for mappers")
    else:
      logging.info("Coord: recovery called for reducers")
    msg_to_send = RecoveryMsg(self.state.next_cp_id, self.state.next_recovery_id)
    msg_bytes = msg_to_send.encode() 
    for _,ws in self.state.workers.items():
      if (to_mappers):
        if (not ws.is_mapper):
          continue
      else:
        if (ws.is_mapper):
          continue
  
      # logging.info(f"Coord: sending recovery to {ws.id}, ckpt id: {self.state.next_cp_id}, recv_id: {self.state.next_recovery_id}")
      self.state.sock.sendto(msg_bytes, ws.addr)
    # logging.info(f"Coord: sent all recovery msg")

  """
    *_phase methods define what to do in a certain PHASE.
  """
  def cp_phase(self):
    if self.state.phase != PHASE.CP:
      logging.error(f"Coord: phase mismatch for cp, phase is {self.state.phase}")
      return
    # TODO: complete the behavior when system is in CP phase
    # have to send markers to mappers with ckpt id  next_cp_id and recovery_id next_recovery_id 
    self.send_marker_to_mappers("CP") 


  def recover_phase(self, is_mapper: bool) -> None:
    if is_mapper:
      assert self.state.phase == PHASE.RECOVER_MAPPER
    else:
      assert self.state.phase == PHASE.RECOVER_REDUCER

    # TODO: complete the behavior when system is in RECOVERY phase

    if (self.state.phase == PHASE.RECOVER_REDUCER):
      # self.state.next_recovery_id += 1
      rollback_checkpoint_id = self.state.last_completed_checkpoint_id()
      # logging.info(f"Coord: rollback id is {rollback_checkpoint_id}") 
      # logging.info(f"Coord: recovery id updated to {self.state.next_recovery_id}")
      # reset all workers 
      for _, ws in self.state.workers.items():
        ws.reset() 
        if (rollback_checkpoint_id != -1):
           ws.last_cp_id = rollback_checkpoint_id  
        else:
          ws.last_cp_id = 0  # no checkpoints made 
      # global state 
      if (rollback_checkpoint_id != -1):
        self.state.next_cp_id = rollback_checkpoint_id
      else:
        self.state.next_cp_id = 0 #  will be set to 1 on complete recovery
      # send message to all reducers
      self.send_recovery(to_mappers=False)
    else:
      self.send_recovery(to_mappers=True) 
      self.state.next_cp_id += 1 # need to ckpt from next ckpt of rollback ckpt 
      

  def last_cp_phase(self):
    assert self.state.phase == PHASE.LAST_CP
    # TODO: complete the behavior when system is in Last_CP phase
    self.send_marker_to_mappers("LAST_CP") 


  def exit_phase(self, start_time):
    assert self.state.phase == PHASE.EXITING

    logging.info(f"{self.id} sending exit command to workers")
    for _, ws in self.state.workers.items():
      ExitMsg().send(self.state.sock, ws.addr)
    logging.critical(f"{self.id} exiting!")
    # self.state.send_socket.close()
    end_time = datetime.datetime.now()
    logging.info(f"Job Finished at {end_time}")
    logging.info(f"Total Time Taken = {end_time - start_time}")
    os.kill(self.pid, signal.SIGKILL)

  def run(self):
    start_time = datetime.datetime.now()
    logging.info(f"Starting Job at {start_time}")
    logging.info("SENDING thread of coordinator started!")
    while True:
      current_phase = None
      if not self.phase_queue.empty():
        current_phase = self.phase_queue.get()

      # if (current_phase is not None):
      #   self.state.phase = current_phase

      if current_phase is None:
        continue

      elif current_phase == PHASE.CP:
        logging.info("The current phase is Checkpointing phase")
        time.sleep(CHECKPOINT_INTERVAL)
        self.cp_phase()
      
      elif current_phase == PHASE.RECOVER_REDUCER:
        logging.info("The current phase is Reducer Recovery phase")
        self.recover_phase(is_mapper=False)

      elif current_phase == PHASE.RECOVER_MAPPER:
        logging.info("The current phase is Mapper Recovery phase")
        self.recover_phase(is_mapper=True)

      elif current_phase == PHASE.LAST_CP:
        logging.info("The current phase is Last Checkpoint phase")
        self.last_cp_phase()

      elif current_phase == PHASE.EXITING:
        logging.info("The current phase is Exiting phase")
        self.exit_phase(start_time)

      else:
        logging.error("Unknown global phase. Exiting!")
        break

