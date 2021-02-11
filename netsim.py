"""
    The basic implementation of the network simulator: 
    https://www.grotto-networking.com/DiscreteEventPython.html
    Extended by: Mania Abdi, 
    Author: Greg M. Bernstein
    Released under the MIT license
"""
import simpy
import random
import copy
from simpy.core import BoundClass
from simpy.resources import base
from heapq import heappush, heappop
from colorama import Fore, Style
from sys import getsizeof

class Request(object):
    """ A very simple class that represents a request.
        This packet will run through a queue at a switch output port.
        We use a float to represent the size of the packet in bytes so that
        we can compare to ideal M/M/1 queues.

        Parameters
        ----------
        time : float
            the time the packet arrives at the output queue.
        size : float
            the size of the packet in bytes
        id : int
            an identifier for the packet
        src, dst : int
            identifiers for source and destination
        flow_id : int
            small integer that can be used to identify a flow
    """
    def __init__(self, time, req_id, src, sport, dst, dport, rpc, data=None):
        self.time = time
        self.src = src
        self.sport = sport
        self.dst = dst
        self.dport = dport
        self.reqid = req_id
        self.rpc = rpc
        self.data = data
        self.size = 0
        self.size = getsizeof(self) 

    def __sizeof__(self):
        return getsizeof(self.time) + getsizeof(self.size) + getsizeof(self.reqid) + getsizeof(self.src) + getsizeof(self.sport) +\
               getsizeof(self.dst) + getsizeof(self.dport) + getsizeof(self.data) + getsizeof(self.rpc) + (self.data['size'] if 'size' in self.data else 0) 

    def __repr__(self):
        return "rpc: {}, data: {}, src: {}:{}, dst: {}:{}, req_id: {}, time: {} with size: {}".\
            format(self.rpc, self.data, self.src, self.sport, self.dst, self.dport, self.reqid, self.time, self.size)




class Packet(object):
    """ A very simple class that represents a packet.
        This packet will run through a queue at a switch output port.
        We use a float to represent the size of the packet in bytes so that
        we can compare to ideal M/M/1 queues.

        Parameters
        ----------
        time : float
            the time the packet arrives at the output queue.
        size : float
            the size of the packet in bytes
        id : int
            an identifier for the packet
        src, dst : int
            identifiers for source and destination
        flow_id : int
            small integer that can be used to identify a flow
    """
    def __init__(self, time, size, id, src="a", dst="z", flow_id=0):
        self.time = time
        self.size = size
        self.id = id
        self.src = src
        self.dst = dst
        self.flow_id = flow_id

    def __repr__(self):
        return "id: {}, src: {}, time: {}, size: {}".\
            format(self.id, self.src, self.time, self.size)




class NetworkInterface(object):
    """ Models a switch output port with a given rate and buffer size limit in bytes.
        Set the "out" member variable to the entity to receive the packet.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        rate : float
            the bit rate of the port
        qlimit : integer (or None)
            a buffer size limit in bytes or packets for the queue (including items
            in service).
        limit_bytes : If true, the queue limit will be based on bytes if false the
            queue limit will be based on packets.
        packet_size:
            size of packet in byte, default is 1024 bytes

    """
    # FIXME this should split according to the flow id
    def __init__(self, env, name, ip, rate, gateway, packet_size=1024, debug=False):
        self.out_store = simpy.Store(env)
        self.in_store = simpy.Store(env)
        self.rate = rate
        self.env = env
        self.name = name
        self.ip = ip
        self.gateway = gateway
        
        self.recipients = {}
        
        self.packets_rec = 0
        self.packet_rcv = 0
        self.byte_size = 0
        self.packets_drop = 0
        self.packet_size = packet_size
        self.debug = debug
        self.busy = 0  # Used to track if a packet is currently being sent
        self.send_action = env.process(self.send())  # starts the run() method as a SimPy process
        self.recv_action = env.process(self.receive())  # starts the run() method as a SimPy process


    def add_flow(self, port, recipient):
        self.recipients[port] = recipient


    def receive(self):
        while True:
            msg = (yield self.in_store.get()) 
            if self.debug:
                print(f'NIC {self.ip} recieved message {msg} at {self.env.now}')
            if msg.dport not in self.recipients:
                raise NameError(f'There is no recipient for this message at {self.ip}:{msg.dport}')
            yield self.recipients[msg.dport].put(msg)


    def send(self):
        while True:
            msg = (yield self.out_store.get()) 
            self.busy = 1
            self.byte_size -= msg.size
            transmit_time = msg.size*8.0/self.rate
            yield self.env.timeout(transmit_time)
            self.out.put(msg)
            self.busy = 0
            if self.debug:
                print(msg)


    def put(self, req):
        if req.dst == self.ip:
            self.packet_rcv += 1
            return self.in_store.put(req)
        req.src = self.ip
        self.packets_rec += 1

        tmp_byte_count = self.byte_size + req.size
        return self.out_store.put(req)
    



class SwitchPort(object):
    """ Models a switch output port with a given rate and buffer size limit in bytes.
        Set the "out" member variable to the entity to receive the packet.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        rate : float
            the bit rate of the port
        qlimit : integer (or None)
            a buffer size limit in bytes or packets for the queue (including items
            in service).
        limit_bytes : If true, the queue limit will be based on bytes if false the
            queue limit will be based on packets.

    """
    def __init__(self, env, rate, qlimit=None, limit_bytes=True, debug=False):
        self.store = simpy.Store(env)
        self.rate = rate
        self.env = env
        self.connected_ip = '0.0.0.0' 
        self.out = None
        self.packets_rec = 0
        self.packets_drop = 0
        self.qlimit = qlimit
        self.limit_bytes = limit_bytes
        self.byte_size = 0  # Current size of the queue in bytes
        self.debug = debug
        self.busy = 0  # Used to track if a packet is currently being sent
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        while True:
            msg = (yield self.store.get())
            self.busy = 1
            self.byte_size -= msg.size
            transmit_time = msg.size*8.0/self.rate
            if self.debug:
                print(f'{Fore.BLUE} {self.connected_ip} ip will recieve the {msg} response after {round(transmit_time, 2)}, rate is {self.rate} {Style.RESET_ALL}')
            yield self.env.timeout(msg.size*8.0/self.rate)
            self.out.put(msg)
            self.busy = 0
            if self.debug:
                print(msg)

    def put(self, pkt):
        self.packets_rec += 1
        tmp_byte_count = self.byte_size + pkt.size

        if self.qlimit is None:
            self.byte_size = tmp_byte_count
            return self.store.put(pkt)
        if self.limit_bytes and tmp_byte_count >= self.qlimit:
            self.packets_drop += 1
            return
        elif not self.limit_bytes and len(self.store.items) >= self.qlimit-1:
            self.packets_drop += 1
        else:
            self.byte_size = tmp_byte_count
            return self.store.put(pkt)




class Router(object):
    """ A demultiplexing element that chooses the output port in destination.

        Contains a list of output ports of the same length as the probability list
        in the constructor.  Use these to connect to other network elements.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        probs : List
            list of probabilities for the corresponding output ports
        rate : int
            the defaul rate for the output ports default is 1 kbps
    """
    def __init__(self, env, name, ports, rate, ip='127.0.0.1', gateway='0.0.0.0', debug=False): 
        self.env = env
        self.ip = ip
        self.gateway = gateway
        self.name = name
        self.rack = int(ip.split('.')[1])
        self.n_ports = ports
        self.port_rate = rate
        self.free_ports = [SwitchPort(env, self.port_rate) for i in range(self.n_ports)]  # Create and initialize output ports
        self.packets_rec = 0
        self.route_table = {}
        self.debug = debug


    def connect(self, sink, gateway=False):
        if not self.free_ports:
            raise NameError('The switch is fully connect. Redesign your topology')
        port = self.free_ports.pop()
        port.connected_ip = sink.ip
        port.out = sink
        self.route_table[sink.ip] = port
        if gateway:
            self.route_table['gateway'] = port
        return

    def add_route(self, ip):
        ''' Currently I wan to set up a static route '''
        domain = ip.rsplit('.', 1)[0]
        self.route_table[domain] = self.route_table[ip]
        pass

    def route(self, pkt):
        if self.debug:
           print(f'{Fore.GREEN} currently at {self.ip} {Style.RESET_ALL}')
           for rt in self.route_table:
               print(f'{Fore.RED} {rt} : {self.route_table[rt].connected_ip} {Style.RESET_ALL}')
        if pkt.dst in self.route_table:
            return self.route_table[pkt.dst]
        domain = pkt.dst.rsplit('.', 1)[0]
        if domain in self.route_table:
            return self.route_table[domain]
        return self.route_table['gateway']


    def put(self, pkt):
        self.packets_rec += 1
        dest = self.route(pkt)
        if self.debug:
            print(f'router {self.name}, packet source {pkt.src} destination {pkt.dst}, next hop {dest.connected_ip}')
        return dest.put(pkt)



class FlowDemux(object):
        """ A demultiplexing element that splits packet streams by flow_id.

        Contains a list of output ports of the same length as the probability list
        in the constructor.  Use these to connect to other network elements.

        Parameters
        ----------
        outs : List
            list of probabilities for the corresponding output ports
    """
        def __init__(self, outs=None, default=None):
            self.outs = outs
            self.default = default
            self.packets_rec = 0

        def put(self, pkt):
            self.packets_rec += 1
            flow_id = pkt.flow_id
            if flow_id < len(self.outs):
                self.outs[flow_id].put(pkt)
            else:
                if self.default:
                    self.default.put(pkt)



"""
    Trying to implement a stamped/ordered version of the Simpy Store class.
    The "stamp" is used to sort the elements for removal ordering. This
    can be used in the implementation of sophisticated queueing disciplines, but
    would be overkill for fixed priority schemes.

    Copyright 2014 Greg M. Bernstein
    Released under the MIT license
"""


class StampedStorePut(base.Put):
    """ Put *item* into the store if possible or wait until it is.
        The item must be a tuple (stamp, contents) where the stamp is used to sort
        the content in the StampedStore.
    """
    def __init__(self, resource, item):
        self.item = item
        """The item to put into the store."""
        super(StampedStorePut, self).__init__(resource)


class StampedStoreGet(base.Get):
    """Get an item from the store or wait until one is available."""
    pass


class StampedStore(base.BaseResource):
    """Models the production and consumption of concrete Python objects.

    Items put into the store can be of any type.  By default, they are put and
    retrieved from the store in a first-in first-out order.

    The *env* parameter is the :class:`~simpy.core.Environment` instance the
    container is bound to.

    The *capacity* defines the size of the Store and must be a positive number
    (> 0). By default, a Store is of unlimited size. A :exc:`ValueError` is
    raised if the value is negative.

    """
    def __init__(self, env, capacity=float('inf')):
        super(StampedStore, self).__init__(env, capacity=float('inf'))
        if capacity <= 0:
            raise ValueError('"capacity" must be > 0.')
        self._capacity = capacity
        self.items = []  # we are keeping items sorted by stamp
        self.event_count = 0 # Used to break ties with python heap implementation
        # See: https://docs.python.org/3/library/heapq.html?highlight=heappush#priority-queue-implementation-notes
        """List of the items within the store."""

    @property
    def capacity(self):
        """The maximum capacity of the store."""
        return self._capacity

    put = BoundClass(StampedStorePut)
    """Create a new :class:`StorePut` event."""

    get = BoundClass(StampedStoreGet)
    """Create a new :class:`StoreGet` event."""

    # We assume the item is a tuple: (stamp, packet). The stamp is used to
    # sort the packet in the heap.
    def _do_put(self, event):
        self.event_count += 1 # Needed this to break heap ties
        if len(self.items) < self._capacity:
            heappush(self.items, [event.item[0], self.event_count, event.item[1]])
            event.succeed()

    # When we return an item from the stamped store we do not
    # return the stamp but only the content portion.
    def _do_get(self, event):
        if self.items:
            event.succeed(heappop(self.items)[2])

"""
    A Set of components to enable simulation of various networking QoS scenarios.

    Copyright 2014 Dr. Greg Bernstein
    Released under the MIT license
"""


class ShaperTokenBucket(object):
    """ Models an ideal token bucket shaper. Note the token bucket size should be greater than the
        size of the largest packet that can occur on input. If this is not the case we always accumulate
        enough tokens to let the current packet pass based on the average rate. This may not be
        the behavior you desire.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        rate : float
            the token arrival rate in bits
        b_size : Number
            a token bucket size in bytes
        peak : Number or None for infinite peak
            the peak sending rate of the buffer (quickest time two packets could be sent)

    """
    def __init__(self, env, rate, b_size, peak=None, debug=False):
        self.store = simpy.Store(env)
        self.rate = rate
        self.env = env
        self.out = None
        self.packets_rec = 0
        self.packets_sent = 0
        self.b_size = b_size
        self.peak = peak

        self.current_bucket = b_size  # Current size of the bucket in bytes
        self.update_time = 0.0  # Last time the bucket was updated
        self.debug = debug
        self.busy = 0  # Used to track if a packet is currently being sent ?
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        while True:
            msg = (yield self.store.get())
            now = self.env.now
            #  Add tokens to bucket based on current time
            self.current_bucket = min(self.b_size, self.current_bucket + self.rate*(now-self.update_time)/8.0)
            self.update_time = now
            #  Check if there are enough tokens to allow packet to be sent
            #  If not we will wait to accumulate enough tokens to let this packet pass
            #  regardless of the bucket size.
            if msg.size > self.current_bucket:  # Need to wait for bucket to fill before sending
                yield self.env.timeout((msg.size - self.current_bucket)*8.0/self.rate)
                self.current_bucket = 0.0
                self.update_time = self.env.now
            else:
                self.current_bucket -= msg.size
                self.update_time = self.env.now
            # Send packet
            if not self.peak:  # Infinite peak rate
                self.out.put(msg)
            else:
                yield self.env.timeout(msg.size*8.0/self.peak)
                self.out.put(msg)
            self.packets_sent += 1
            if self.debug:
                print(msg)

    def put(self, pkt):
        self.packets_rec += 1
        return self.store.put(pkt)


class VirtualClockServer(object):
    """ Models a virtual clock server. For theory and implementation see:
        L. Zhang, Virtual clock: A new traffic control algorithm for packet switching networks,
        in ACM SIGCOMM Computer Communication Review, 1990, vol. 20, pp. 19.


        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        rate : float
            the bit rate of the port
        vticks : A list
            list of the vtick parameters (for each possible packet flow_id). We assume a simple assignment of
            flow id to vticks, i.e., flow_id = 0 corresponds to vticks[0], etc... We assume that the vticks are
            the inverse of the desired rates for the flows in bits per second.
    """
    def __init__(self, env, rate, vticks, debug=False):
        self.env = env
        self.rate = rate
        self.vticks = vticks
        self.auxVCs = [0.0 for i in range(len(vticks))]  # Initialize all the auxVC variables
        self.out = None
        self.packets_rec = 0
        self.packets_drop = 0
        self.debug = debug
        self.store = StampedStore(env)
        self.action = env.process(self.run())  # starts the run() method as a SimPy process

    def run(self):
        while True:
            msg = (yield self.store.get())
            # Send message
            yield self.env.timeout(msg.size*8.0/self.rate)
            self.out.put(msg)

    def put(self, pkt):
        self.packets_rec += 1
        now = self.env.now
        flow_id = pkt.flow_id
        # Update of auxVC for the flow. We assume that vticks is the desired bit time
        # i.e., the inverse of the desired bits per second data rate.
        # Hence we then multiply this value by the size of the packet in bits.
        self.auxVCs[flow_id] = max(now, self.auxVCs[flow_id]) + self.vticks[flow_id]*pkt.size*8.0
        # Lots of work to do here to implement the queueing discipline
        return self.store.put((self.auxVCs[flow_id], pkt))


class WFQServer(object):
    """ Models a WFQ/PGPS server. For theory and implementation see:

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        rate : float
            the bit rate of the port
        phis : A list
            list of the phis parameters (for each possible packet flow_id). We assume a simple assignment of
            flow id to phis, i.e., flow_id = 0 corresponds to phis[0], etc...
    """
    def __init__(self, env, rate, phis, debug=False):
        self.env = env
        self.rate = rate
        self.phis = phis
        self.F_times = [0.0 for i in range(len(phis))]  # Initialize all the finish time variables
        # We keep track of the number of packets from each flow in the queue
        self.flow_queue_count = [0 for i in range(len(phis))]
        self.active_set = set()
        self.vtime = 0.0
        self.out = None
        self.packets_rec = 0
        self.packets_drop = 0
        self.debug = debug
        self.store = StampedStore(env)
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.last_update = 0.0

    def run(self):
        while True:
            msg = (yield self.store.get())
            self.last_update = self.env.now
            flow_id = msg.flow_id
            # update information about flow items in queue
            self.flow_queue_count[flow_id] -= 1
            if self.flow_queue_count[flow_id] == 0:
                self.active_set.remove(flow_id)
            # If end of busy period, reset virtual time and reinitialize finish times.
            if len(self.active_set) == 0:
                self.vtime = 0.0
                for i in range(len(self.F_times)):
                    self.F_times[i] = 0.0
            # Send message
            yield self.env.timeout(msg.size*8.0/self.rate)
            self.out.put(msg)

    def put(self, pkt):
        self.packets_rec += 1
        now = self.env.now
        flow_id = pkt.flow_id
        self.flow_queue_count[flow_id] += 1
        self.active_set.add(flow_id)
        phi_sum = 0.0
        for i in self.active_set:
            phi_sum += self.phis[i]
        self.vtime += (now-self.last_update)/phi_sum
        self.F_times[flow_id] = max(self.F_times[flow_id], self.vtime) + pkt.size*8.0/self.phis[flow_id]
        self.last_update = now
        return self.store.put((self.F_times[flow_id], pkt))


