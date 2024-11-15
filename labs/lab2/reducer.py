from _socket import SHUT_RDWR
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from collections import defaultdict
import logging
import json
import os
import signal
import queue
import socket
import threading
from multiprocessing import Process
import time
from typing import Final

from constants import HEARTBEAT_INTERVAL, COORDINATOR_PORT
from message import Message, MT
from mylog import Logger

logging = Logger().get_logger()


def recvall(sock: socket.socket, length: int) -> bytes:
    data = b''
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise Exception("Connection closed")
        data += packet
    return data


@dataclass
class ReducerState:
  pid: int
  id: str
  c_socket: socket.socket
  listen_port: int
  server_socket: socket.socket
  barrier: threading.Barrier
  wc: dict[str, int] = field(default_factory=lambda: defaultdict(int))
  last_recovery_id: int = 0
  last_cp_id: int = 0
  client_sockets: list[socket.socket] = field(default_factory=list)
  q_has_ckpt = False 

  def __post_init__(self):
    # creating connections with coordinator
    self.c_socket.bind(("localhost", self.listen_port))

    # creating connections with mappers and threads to handle data incoming from mapper
    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server_socket.bind(("localhost", self.listen_port))
    self.server_socket.listen(5)
    logging.info(f"{self.id} listening on localhost:{self.listen_port}")

  # use this function to send any message to the coordinator
  def to_coordinator(self, msg: Message) -> None:
    self.c_socket.sendto(msg.serialize(), ("localhost", COORDINATOR_PORT))

  # use this function to create a reducer's checkpoint
  # wc is checkpointed
  def checkpoint(self, checkpoint_id: int):
    os.makedirs('checkpoints', exist_ok=True)
    filename = f"checkpoints/{self.id}_{checkpoint_id}.txt"
    with open(filename, 'w') as file:
      json.dump(self.wc, file)
    self.last_cp_id = checkpoint_id

  # reducer recovering
  def recover(self, recovery_id: int, checkpoint_id: int):
    # TODO: Recover from checkpoint
    logging.info(f"{self.id}: recovering from {recovery_id}")
    if (checkpoint_id < 0):
      # clear wc 
      logging.info(f"{self.id}: resetting wc since ckpt_id={checkpoint_id}")
      self.wc = defaultdict(int)
    else:
      filename = f"checkpoints/{self.id}_{checkpoint_id}.txt"
      try:
        read_wc = dict()
        with open(filename, 'r') as file:
          read_wc = json.load(file)
        # reset wc 
        logging.info(f"{self.id}: resetting wc, read wc is {read_wc}")
        self.wc = read_wc 
      except Exception as e:
        logging.error(f"{self.id}: recovery failed: {e}") 
        return 
    self.last_recovery_id = recovery_id
    if (checkpoint_id != -1):
      self.last_cp_id = checkpoint_id 
    else:
      self.last_cp_id = 0 # back to initial
      
  def exit(self):
    # Break existing map threads by re-opening sockets. while loop will create new ones
    logging.info(f"{self.id} is exiting! The local word count is: {self.wc}")
    for s in self.client_sockets:
      try:
        s.shutdown(SHUT_RDWR)
      except Exception as e:
        logging.warning(f"Mapper is probably dead. Ignoring {e}")
    self.client_sockets = []
    self.barrier.reset()

    try:
      self.server_socket.shutdown(SHUT_RDWR)
    except Exception as e:
      logging.warning(f"Mapper is probably dead. Ignoring {e}")

    os.kill(self.pid, signal.SIGKILL)


class Cmd(ABC):
  @abstractmethod
  def handle(self, state: ReducerState):
    """Same as `class Cmd` of `mapper.py`"""
    raise NotImplementedError


class CPMarker(Cmd):
  def __init__(self, checkpoint_id: int, recovery_id: int):
    self.checkpoint_id = checkpoint_id
    self.recovery_id = recovery_id

  def handle(self, state: ReducerState):
    # TODO: this reducer has to checkpoint. Take appropriate actions.
    # assuming marker receiver received from all mappers
    # 1. checkpoint and update state  2. send ack  
    if (self.recovery_id == state.last_recovery_id):
      logging.info(f"{state.id}: checkpointing for ckpt id: {self.checkpoint_id} and red_id: {self.recovery_id}")
      state.q_has_ckpt = False 
      state.checkpoint(self.checkpoint_id) 
      state.last_cp_id = self.checkpoint_id # done also in state.checkpoint

      # logging.info(f"{state.id}: sending checkpoint ack to coord")
      # send ack to coordinator
      if (self.checkpoint_id != 0):
        ckpt_ack_msg = Message(msg_type = MT.CHECKPOINT_ACK, source = state.id, checkpoint_id = self.checkpoint_id, recovery_id = self.recovery_id)  
      else:
        ckpt_ack_msg = Message(msg_type = MT.LAST_CHECKPOINT_ACK , source = state.id, checkpoint_id = self.checkpoint_id, recovery_id = self.recovery_id)   
      state.to_coordinator(ckpt_ack_msg) 
      # logging.info(f"{state.id}: checkpoint complete for id {self.checkpoint_id}" )

    else:
      logging.warning(
        f"{state.id}: Ignoring in-flight messages with recovery_id = {self.recovery_id}.My recovery_id = {state.last_recovery_id}")




class Recover(Cmd):
  def __init__(self, recovery_id: int, checkpoint_id: int):
    self.checkpoint_id = checkpoint_id
    self.recovery_id = recovery_id

  def handle(self, state: ReducerState):
    # TODO: This reducer need to recover from failure.
    # logging.info(f"{state.id}: recovering state")
    state.recover(self.recovery_id, self.checkpoint_id) 
    # logging.info(f"{state.id}: resetting barrier")
    state.barrier.reset() 
    # logging.info(f"{state.id}: sending recover_ack")
    recv_ack_msg = Message(msg_type = MT.RECOVERY_ACK, source = state.id, recovery_id = self.recovery_id)   
    state.to_coordinator(recv_ack_msg) 

    # logging.info(f"{state.id}: recovery complete for recv id {self.recovery_id}" )

    


class WC(Cmd):
  def __init__(self, word: str, count: int, recovery_id: int):
    self.recovery_id = recovery_id
    self.word = word
    self.count = count

  def handle(self, state: ReducerState):
    if self.recovery_id == state.last_recovery_id:
      state.wc[self.word] += self.count
      # logging.info(f"Adding word count {self.word}={self.count}")
    else:
      pass
      # logging.warning(
        # f"Ignoring in-flight messages from with recovery_id = {self.recovery_id}.My recovery_id = {state.last_recovery_id}, word is {self.word}, count is {self.count}")


class Exit(Cmd):
  # This is how EXIT request will be handled.
  def handle(self, state: ReducerState):
    logging.critical(f"{state.id} exiting!")
    state.exit()


class CmdHandler(threading.Thread):
  def __init__(self, state: ReducerState, cmd_q: queue.Queue[Cmd]):
    super(CmdHandler, self).__init__()
    self.cmd_q = cmd_q
    self.state = state

  # in every iteration, this thread will read the queue to process any request
  def run(self) -> None:
    while True:
      cmd: Cmd = self.cmd_q.get() # note, the default mode is blocking
      try:
        cmd.handle(self.state)

      except Exception as e:
        logging.exception(e)
        logging.critical("Reducer should not have gotten exceptions! Killing self.")
        os.kill(self.state.pid, signal.SIGKILL)


class Reducer(Process):
  def __init__(self, idx: int, listen_port: int, num_mappers: int):
    super().__init__()
    self.id: Final[str] = f"Reducer_{idx}"
    self.cmd_q: queue.Queue[Cmd]

    self.listen_port = listen_port
    self.num_mappers: Final[int] = num_mappers

    # to track the checkpoint_id received from different mappers
    self.cp_marker: dict[int, int] = {}
    for i in range(num_mappers):
      self.cp_marker[i] = -1
    self.update_mapper_idx = 0 # the mapper which puts the ckpt cmd in queue 
  

  def send_heartbeat(self, heartbeat_socket: socket.socket):
    coordinator_addr = ("localhost", COORDINATOR_PORT)
    while True:
      try:
        pass
        heartbeat_socket.sendto(Message(msg_type=MT.HEARTBEAT, source=self.id).serialize(), coordinator_addr)
        time.sleep(HEARTBEAT_INTERVAL)
      except Exception as e:
        logging.error(f"Heartbeat error: {e}")

  def handle_coordinator(self, coordinator_conn: socket.socket, cmd_q: queue.Queue[Cmd]):
    while True:
      try:
        response, _ = coordinator_conn.recvfrom(1024)
        message: Message = Message.deserialize(response)
        logging.info(f"{self.id} received message of type {message.msg_type.name} from {message.source}")

        if message.msg_type == MT.EXIT:
          cmd_q.put(Exit())
        elif message.msg_type == MT.RECOVER:
          cmd_q.put(Recover(checkpoint_id=message.kwargs["checkpoint_id"],
                            recovery_id=message.kwargs["recovery_id"]))

      except Exception as e:
        logging.error(f"Error: {e}")

  def handle_mappers(self, client_socket: socket.socket, cmd_q: queue.Queue[Cmd], barrier: threading.Barrier, state: ReducerState):
    try:
      while True:
        data = recvall(client_socket, 1024)
        if not data:
          break
        message = Message.deserialize(data)
        # logging.info(f"{self.id} received message of type {message.msg_type.name} from {message.source}")

        # TODO: Here reducer has received a message from mapper.
        # Take appropriate actions. 
        # HINT: Process the message, and put the work to do in cmd queue

        # if (message.msg_type == MT.FWD_CHECKPOINT):
        #   mapper_name = message.source 
        #   assert(mapper_name.startswith("Mapper")) 
        #   mapper_id = int(mapper_name[7:])  
        #   logging.info(f"{self.id}: checkpoint marker received from {mapper_name}")
        #   if (mapper_id != self.update_mapper_idx):
        #     self.cp_marker[mapper_id] = int(message.kwargs.get("checkpoint_id"))
        #     logging.info(f"{self.id}: set cp_marker for mapper_{mapper_id} to {self.cp_marker[mapper_id]}")
          
        #   if (mapper_id == self.update_mapper_idx):
        #     recovery_id = message.kwargs.get("recovery_id")
        #     ckpt_id = int(message.kwargs.get("checkpoint_id"))
        #     # logging.info(f"{self.id}: {self.update_mapper_idx} putting marker {ckpt_id} and recovery id: {recovery_id}")
        #     cmd_q.put(CPMarker(ckpt_id, recovery_id))
        #     self.cp_marker[mapper_id] = ckpt_id
        #     logging.info(f"{self.id}: set cp_marker for (update)mapper_{mapper_id} to {self.cp_marker[mapper_id]}")
        #   else:
        #     while(True):
        #       if (self.cp_marker[self.update_mapper_idx] == self.cp_marker[mapper_id]):
        #         break
        #   try:
        #     barrier.wait() 
        #     logging.info(f"{self.id}: barrier exited for {mapper_id}")
        #   except threading.BrokenBarrierError:
        #     logging.info(f"{self.id}: barrier exited coz of recovery for {mapper_id}")
        #   logging.info(f"{self.id}: exited barrier for mapper_{mapper_id}")
        #   self.cp_marker[mapper_id] = -1 


        if message.msg_type == MT.FWD_CHECKPOINT:  # received checkpoint marker
          # TODO
          mapper_name = message.source 
          assert(mapper_name.startswith("Mapper")) 
          mapper_id = int(mapper_name[7:])  
          ckpt_id = int(message.kwargs.get("checkpoint_id"))
          recv_id = int(message.kwargs.get("recovery_id")) 
          logging.info(f"{self.id}: checkpoint marker received from mapper {mapper_name}, mapper_id {mapper_id}, ckpt_id: {ckpt_id}, recv_id: {recv_id}")
          
          if (recv_id != state.last_recovery_id):
            logging.info(f"{self.id}: ignoring msg since recv_id:{recv_id} doesnt match with mine {state.last_recovery_id}")
            continue

          if (mapper_id != self.update_mapper_idx):
            self.cp_marker[mapper_id] = int(message.kwargs.get("checkpoint_id"))
          else:
            self.cp_marker[mapper_id] = -2 
          try:
            barrier.wait() 
            logging.info(f"{self.id}: barrier exited")
          except threading.BrokenBarrierError:
            logging.info(f"{self.id}: barrier exited coz of recovery ")
          
          if (mapper_id == self.update_mapper_idx):
            recovery_id = message.kwargs.get("recovery_id")
            
            ckpt = int(message.kwargs.get("checkpoint_id"))
            # logging.info(f"{self.id}: {self.update_mapper_idx} putting marker {ckpt} and recovery id: {recovery_id}")
            cmd_q.put(CPMarker(ckpt, recovery_id))
            self.cp_marker[mapper_id] = int(message.kwargs.get("checkpoint_id"))
            # for i in range(self.num_mappers):
              # self.cp_marker[i] = -2
          else:
            # logging.info(f"{self.id}: sleeping since {self.update_mapper_idx} putting marker")
            # time.sleep(0.1)
            while(True):
              # logging.info(f"{self.id}: sleeping since {self.update_mapper_idx} putting marker")
              # time.sleep(0.05)
              pass 
              if (self.cp_marker[mapper_id] == self.cp_marker[self.update_mapper_idx]):
                break 
          # self.cp_marker[mapper_id] = -2   

          logging.info(f"{self.id}: mapper {mapper_name} thread will now process next msg") 
        else:
          assert message.msg_type == MT.WORD_COUNT
          key = message.kwargs.get('key')
          value = message.kwargs.get('value')
          recovery_id = message.kwargs.get('last_recovery_id')
          cmd_q.put(WC(key, value, recovery_id))
    except Exception as e:
      logging.error(f"Error: {e}")

    client_socket.close()
    logging.info(f"{self.id} handle_mappers thread exiting")

  def run(self):
    cmd_q = queue.Queue()

    # state of this reducer
    state = ReducerState(pid=self.pid, id=self.id, listen_port=self.listen_port,
                         barrier=threading.Barrier(self.num_mappers),
                         c_socket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
                         server_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    
    # thread to process the command queue
    cmd_thread = CmdHandler(state, cmd_q)
    cmd_thread.start()

    # thread to handle messages from the coordinator
    coordinator_handler = threading.Thread(target=self.handle_coordinator, args=(state.c_socket, cmd_q,))
    coordinator_handler.start()

    # thread to send heartbeats to the coordinator
    heartbeat_thread = threading.Thread(target=self.send_heartbeat, args=(state.c_socket,))
    heartbeat_thread.start()

    try:
      while True:
        # accepting TCP connections from the mappers
        client_socket, _ = state.server_socket.accept()
        state.client_sockets.append(client_socket)
        logging.info(f"{self.id} Accepted connection from mapper")
        client_handler = threading.Thread(target=self.handle_mappers, args=(client_socket, cmd_q, state.barrier, state))
        client_handler.start()
    except Exception as e:
      logging.error(f"Error: {e}")

    # no joining  ? (not required i think, since state.exit() will kill this process)
