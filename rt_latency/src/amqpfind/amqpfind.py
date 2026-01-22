#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
ampqfind.py
~~~~~~~~~~~

PURPOSE
Use a AMQP exchange in simple ways.
Documentation at https://docs.google.com/document/d/1OQV0ewHOyupsFO_MNyu0qBowS3InsprvWmiMunbXbMg

Based on himawari8_delta.py

REFERENCES
http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/
https://kombu.readthedocs.org/en/latest/userguide/introduction.html
http://kombu.readthedocs.org/en/latest/userguide/examples.html
http://oriolrius.cat/blog/2013/09/30/hello-world-using-kombu-library-and-python/
https://gist.github.com/thanos/2933599
https://gist.github.com/markmc/5685616

REQUIRES
pika, python 2.6 or newer

USAGE

# receive (consume)
amqpfind -H hwing.ssec.wisc.edu -X himawari -u himawari -p guest -C 'himawari8.delta' -j '{path}' |xargs -n1 -P4 mycode.sh

# send (produce)
amqpfind -X satellite -C 'test.*' -j '{hello}'
echo '{"hello": "world"}' |./amqpfind -X satellite -P test.hello

:authors: K.Hallock <kevin.hallock@ssec.wisc.edu>, R.K.Garcia <rayg@ssec.wisc.edu>
:copyright: 2015 by University of Wisconsin Regents
:license: GPLv3, see LICENSE for more details
"""
__author__ = 'rayg'
__docformat__ = 'reStructuredText'

import os, sys, re, socket, time
import logging, unittest, optparse, json
import traceback
from functools import reduce
from datetime import datetime, timedelta
from signal import alarm, SIGALRM, signal, SIGTERM
from random import shuffle
from functools import partial
from collections import defaultdict, namedtuple
import multiprocessing as mp

PY_MAJOR_VERSION = sys.version_info[0]
if PY_MAJOR_VERSION == 2:
    import Queue
else:
    import queue as Queue  # py3

try:
    import pika
except ImportError:  # bring our own wheels
    DEVERSIONITIS = {
        2: "pika-1.1.0-py2.py3-none-any.whl",
        3: "pika-1.1.0-py2.py3-none-any.whl"
    }
    home,_ = os.path.split(__file__)
    sys.path.append(os.path.join(home, DEVERSIONITIS[PY_MAJOR_VERSION]))
    import pika
try:
    PIKA_MAJOR_VERSION, = re.findall(r'^(\d+).*$', pika.__version__)
except AttributeError:
    PIKA_MAJOR_VERSION = -1
PIKA_MAJOR_VERSION = int(PIKA_MAJOR_VERSION)

LOG = logging.getLogger(__name__)

OPTS = None  # main() replaces this with global application options

DEFAULT_SERVER='mq1.ssec.wisc.edu'
DEFAULT_USER='himawari' # FUTURE: this should be guest
DEFAULT_PASSWORD='guest'
DEFAULT_EXCHANGE='satellite'
DEFAULT_KEY='geo.himawari.8.ahi.file.delta.hsf.image.complete'
DEFAULT_MESSAGE_TTL = 72 * 60 * 60 * 1000   # 72h time-to-live as milliseconds, used for durable queues named after host
DEFAULT_RECONNECT_TIME = 30
DEFAULT_HORIZON_SEC = 5.0


class TimeoutException(Exception):
    pass


def handle_timeout(*args):
    LOG.error('timeout')
    raise TimeoutException("no messages emitted")


def _deprecated_default_callback(routing_key, body):
    # dumpmode on single-server
    if isinstance(body, dict):
        dump = json.dumps(body)
    else:
        dump = body.decode("utf-8")
    print("{0:s}: '{1:s}'".format(routing_key, dump))
    sys.stdout.flush()
    LOG.debug(repr(body))


def _missing_(*args, **kwargs):
    """pseudo-constructor used to make sure all keys resolve"""
    return '?UNKNOWN?'


def json_emit(topic, content, format_str):
    try:
        if format_str is not None and format_str not in ('?', '*'):
            discontent = defaultdict(_missing_)
            try:
                discontent.update(content)
                s = format_str.format_map(discontent)
            except AttributeError as welcome_to_the_antique_shoppe:
                from string import Formatter
                parts = Formatter().parse(format_str)
                upheaval = dict((part[1], discontent[part[1]]) for part in parts)
                s = format_str.format(**upheaval)
            print(s)
        elif format_str == '?':
            print('v'*25)
            keys = list(sorted(content.keys()))
            for key in keys:
                print('%24s: %s' % (key, content[key]))
            print('^'*25)
        elif format_str == '*':  # null-terminated json
            toco = (topic, content)
            txt = json.dumps(toco) + '\0'
            sys.stdout.write(txt)
        else:
            # Dumpmode default is just line-output content with topic prefix:
            loco = json.dumps(content)
            txt = "{}: {}\n".format(topic, repr(loco))
            sys.stdout.write(txt)
    except KeyError as missing:
        LOG.error('skipping message, missing key %s in %s' % (str(missing), repr(content)))
    sys.stdout.flush()


class test_adde_abi_callback(object):
    D = None
    all = set(range(1,17))

    def __init__(self):
        from collections import defaultdict
        self.D = defaultdict(set)  # (adde_dataset, start_time): {band, band, ...}

    def __call__(self, routing_key, json_dict):
        D = self.D
        # try:
        #     json_dict = json.loads(json_txt.decode('utf-8'))
        # except ValueError:
        #     LOG.error('invalid JSON: ' + repr(json_txt))
        #     raise
        key = json_dict['adde_dataset'], json_dict['start_time']
        D[key].add(json_dict['band'])
        if D[key]==self.all:
            del D[key]
        from datetime import datetime
        print(str(datetime.utcnow()) + ': ' + ' // '.join('%r: %r' % (k,v) for k,v in D.items()))
        sys.stdout.flush()


def acknowledge_after_callback_wrapper(ch, method, properties, body, callback, timeout=None):
    try:
        content = json.loads(body.decode('utf-8'))
    except ValueError as invalid_format:
        LOG.error("ignoring message: unable to deserialize JSON dictionary %s" % repr(body))
        content = None
    if content is not None:
        callback(method.routing_key, content)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if timeout:
        alarm(timeout)


class AmqpExchange(object):
    """
    Shorthand wrapper for an AMQP exchange.
    Includes optional durable queue:
        durable=True ==> use hostname as durable queue name
        durable="queuename" ==> use that queue name
    Default message TTL on durable queues is 72h to avoid disk/mem DoS.
    """

    def __init__(self, host=DEFAULT_SERVER,
                 user=DEFAULT_USER, password=DEFAULT_PASSWORD,
                 exchange=DEFAULT_EXCHANGE, key=DEFAULT_KEY,
                 durable=None, timeout=None, **ignored):

        credentials = pika.PlainCredentials(user, password)
        conn_params = pika.ConnectionParameters(host=host,
                                                credentials=credentials)
        self.host = host
        self.exchange = exchange
        self.key = key
        self.timeout = timeout
        self._durable = durable

        self.connection = pika.BlockingConnection(conn_params)
        self.channel = self.connection.channel()

    def consume(self, callback=None, exchange=None, key=None, timeout=None):
        exchange = exchange or self.exchange
        key = key or self.key
        timeout = timeout or self.timeout

        # declare temporary queue
        if not self._durable:
            result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            queue_name = result.method.queue
            self.channel.queue_bind(exchange=exchange,
                                    queue=queue_name,
                                    routing_key=key)
        else:
            queue_name = socket.gethostname() if self._durable is True else self._durable
            try:  # binding to pre-existing queue we presumably made in a prior run
                self.channel.queue_bind(exchange=exchange,
                                        queue=queue_name,
                                        routing_key=key)
            except:  # otherwise create and bind to a new durable queue - TBD how these get purged
                # https://www.rabbitmq.com/ttl.html
                self.channel.queue_declare(queue=queue_name,
                                           durable=True,
                                           arguments={'x-message-ttl': DEFAULT_MESSAGE_TTL})
                self.channel.queue_bind(exchange=exchange,
                                        queue=queue_name,
                                        routing_key=key)

        self.channel.basic_qos(prefetch_count=1)
        wrapped_callback = partial(acknowledge_after_callback_wrapper, callback=callback or default_callback, timeout=timeout)
        if PIKA_MAJOR_VERSION < 1:
            self.channel.basic_consume(wrapped_callback, queue=queue_name)
        else:
            self.channel.basic_consume(queue_name, wrapped_callback)
        if timeout:
            alarm(timeout)
        try:
            LOG.info("about to consume {0}/{1}/{2}".format(self.host, exchange, key))
            self.channel.start_consuming()
        except KeyboardInterrupt:
            LOG.warning('keyboard interrupt')
            raise

    def produce(self, content_dict, key=None, exchange=None):
        content_json = json.dumps(content_dict)
        exchange = exchange or self.exchange
        key = key or self.key
        self.channel.basic_publish(exchange,
                                   key,
                                   content_json,
                                   pika.BasicProperties(content_type='text/json',
                                                        delivery_mode=1))

    def close(self):
        if not self.connection:
            return
        if self.channel is not None:
            self.channel.close()
            self.channel = None
        self.connection.close()
        self.connection = None


def _debug(type, value, tb):
    "enable with sys.excepthook = debug"
    if not sys.stdin.isatty():
        sys.__excepthook__(type, value, tb)
    else:
        import traceback, pdb
        traceback.print_exception(type, value, tb)
        # …then start the debugger in post-mortem mode.
        pdb.post_mortem(tb) # more “modern”


def worker_main(server_info, queue, reconnect_delay=DEFAULT_RECONNECT_TIME, reconnect_tries=None):
    """yield message dictionaries to a Queue
    """
    host = server_info['host']
    def queue_callback(routing_key, json_dict, queue=queue, host=host):
        now = datetime.utcnow()
        try:
            # provide host and preferred reception time upstream from multi-host
            content = {'__reception_time__': now.isoformat(),
                       '__reception_host__': host}
            content.update(json_dict)
            # content.update(json.loads(json_txt.decode('utf-8')))
        except ValueError:
            LOG.error('invalid JSON from %s: %s' % (host, repr(json_dict)))
            raise
        queue.put((now, host, routing_key, content))

    while (reconnect_tries is None) or (reconnect_tries > 0):
        LOG.info("connecting to %s" % repr(server_info))
        amqp = AmqpExchange(**server_info)
        try:
            LOG.debug("consuming messages from %s" % repr(server_info))
            amqp.consume(queue_callback)
        except KeyboardInterrupt as weredone:
            LOG.warning("KeyboardInterrupt inducing worker shutdown")
            break
        except:
            exc_txt = traceback.format_exc()
            LOG.error('exception in server connection %s:\n%s' % (host, exc_txt))
        amqp.close()

        LOG.warning("sleeping %ss before reconnecting to %s" % (str(reconnect_delay), host))
        if reconnect_tries is not None:
            reconnect_tries -= 1
            LOG.info("%d retries remaining for %s" % (reconnect_tries, host))
        time.sleep(reconnect_delay)


class Transforms(object):
    """transform dictionary contents with eval expressions"""
    _trans = None
    _ns = None

    def __init__(self, transforms=None, extra_eval_namespace=None):
        self._trans = []
        self._ns = dict(extra_eval_namespace or {})
        if transforms:
            for t in transforms:
                self.add(t)

    def namespace(self):
        d = globals()
        if self._ns:
            d.update(self._ns)
        return d

    def add(self, key, transform=None):
        if transform is None:
            key, transform = key.split('=')
            key = key.strip()
            transform = transform.strip()
        code = compile(transform, '<string>', 'eval')
        self._trans.append((key, code))

    def __call__(self, msg):
        old_msg = dict(msg)
        new_msg = dict(msg)
        LOG.debug("transforming message using %d rules" % len(self._trans))
        for key, code in self._trans:
            new_val = eval(code, self.namespace(), old_msg)
            LOG.debug("transformed %s: %s => %s" % (key, old_msg.get(key, "<empty>"), new_val))
            new_msg[key] = new_val
        return new_msg


class NoneDict(dict):
    "dictionary that returns None instead of throwing KeyError"
    def __getitem__(self, item):
        if item not in self:
            LOG.warning("key {0} not present in dictionary; using None".format(item))
            return None
        return dict.__getitem__(self, item)


class Dispatcher(object):

    QUIT = "object to return when program should exit"
    transforms = None  # callable that munges a message before emitting
    key_code = None  # expression to evaluate to key a message
    score_code = None  # lambda function comparing two messages and returning one, or False if user provided a comparator
    active_keys = None  # dict of recent keys being deduplicated: {key: when-first-seen}
    msg_buffer = None  # defaultdict of {key: [(when, source, message), ... ]}; or None, if messages are not being buffered and scored before forwarding
    horizon = None  # timedelta, how long to hold onto keys before discard
    shuffle = True  # if true, shuffle messages having the same keys before choosing one
    json_format_str = None  # format string for JSON content
    callback = None  # None, or callable(topic:str, content:dict)
    timeout = None  # seconds to set alarm() for after each emit()
    extra_eval_namespace = None  # extra symbols to use with eval()
    extra_default_payload = None  # dictionary of additional keys to provide defaults for if not already present

    def __init__(self, transforms, extra_eval_namespace, opts, extra_default_payload=None):
        super(Dispatcher, self).__init__()
        self.transforms = transforms
        self.extra_eval_namespace = dict(extra_eval_namespace)
        if opts.timeout:
            self.timeout = opts.timeout
        if opts.key is not None:
            self.key_code = compile(opts.key, '<string>', 'eval')
            self.active_keys = {}
        if opts.score is not None:
            if opts.score.strip().startswith('lambda'):  # then it's a full comparator
                score_code = opts.score
                LOG.debug("parsing scoring lambda a,b function '%s'" % score_code)
                self.compare = eval(compile(score_code, '<string>', 'eval'), self.namespace())
                self.score_code = False  # not truthy but not None, implies go straight to compare()
            else:
                self.score_code = compile(opts.score, '<string>', 'eval')
            self.msg_buffer = defaultdict(list)
        if opts.json is not None:
            self.json_format_str = opts.json
            # LOG.info("json format str is %r" % opts.json)
        if opts.window is not None:
            self.horizon = timedelta(seconds=opts.window)
        elif opts.key is not None:
            self.horizon = timedelta(seconds=DEFAULT_HORIZON_SEC)
        if opts.callback is not None:
            self.callback = eval(opts.callback, self.namespace())
        self.extra_default_payload = {} if not extra_default_payload else dict(extra_default_payload)

    def namespace(self):
        d = globals()
        d.update(self.extra_eval_namespace)
        return d

    def add_default_metadata(self, topic, msg):
        if '__topic__' not in msg:
            msg['__topic__'] = topic
        if '__reception_time__' not in msg:
            msg['__reception_time__'] =  datetime.utcnow().isoformat()
        for k, v in self.extra_default_payload.items():
            if k not in msg:
                msg[k] = v

    def emit(self, topic, msg):
        """send a message to downstream, based on user options
        """
        if self.timeout is not None:
            alarm(self.timeout)
        self.add_default_metadata(topic, msg)
        if self.transforms is not None:
            msg = self.transforms(msg)
        if self.callback is not None:
            self.callback(topic, msg)
        else:
            json_emit(topic, msg, self.json_format_str)

    def key_for_msg(self, msg):
        """given key_code is a compiled expression to evaluate on message content
        """
        try:
            return eval(self.key_code, self.namespace(), NoneDict(msg)) if self.key_code else None
        except:
            LOG.error("could not evaluate key for %s: %s" % (repr(msg), traceback.format_exc()))
            return None

    def score_for_msg(self, msg):
        try:
            score = eval(self.score_code, self.namespace(), dict(msg)) if self.score_code else None
            LOG.debug("score of %s for %s" % (score, repr(msg)))
            return score
        except:
            LOG.error("could not extract score value for %s: %s" % (repr(msg), traceback.format_exc()))
            return None

    def compare(self, msg1, msg2):
        """generic score comparator, replaced if an advanced user provides a lambda scoring function"""
        s1, s2 = self.score_for_msg(msg1), self.score_for_msg(msg2)
        return msg1 if (s1 >= s2) else msg2

    def choose_msg(self, messages):
        """return best message of a group based on score and shuffle
        """
        messages = list(messages)
        if self.shuffle:
            shuffle(messages)
            LOG.debug("shuffling candidates")
        LOG.debug("choosing best of %d candidates" % len(messages))

        # if no scoring code, the first message is best
        if self.score_code is None:
            LOG.warning("choosing without a scoring mechanism - this should not happen")
            return messages[0]
        else:
            return reduce(self.compare, messages)

    def _dismiss_active_key(self, key):
        """do any needed handling of key we're not longer interested in
        """
        if self.msg_buffer is None:  # then we don't have anything to do - nothing is pending
            return
        competing_msgs = self.msg_buffer[key]
        if not competing_msgs:
            LOG.error("no messages to compete for key %s" % repr(key))
            return
        del self.msg_buffer[key]
        routing_key_lut = dict([(id(msg), (host, routing_key)) for when, host, routing_key, msg in competing_msgs])
        winner = self.choose_msg([x[-1] for x in competing_msgs])
        host, routing_key = routing_key_lut[id(winner)]
        LOG.info("chose message from %s:%s among %d competitors for key %s in this window" % (host, routing_key, len(competing_msgs), repr(key)))
        self.emit(routing_key, winner)

    def _clean_expired(self):
        now = datetime.utcnow()
        delset = []
        for key, when in self.active_keys.items():
            if (when + self.horizon) < now:
                delset.append(key)
                self._dismiss_active_key(key)
        if delset:
            LOG.debug("closing window for keys: %s" % repr(delset))
        for key in delset:
            del self.active_keys[key]

    def max_sleep_til_next_window(self):
        """how many seconds we can afford to stay blocked before we have to check on something"""
        now = datetime.utcnow()
        if not self.active_keys:
            return None   # indefinite wait
        expiry = min(self.active_keys.values()) + self.horizon
        delay = max(0.0, (expiry - now).total_seconds())
        # LOG.debug("next cleanup happens no later than %s from now" % delay)
        return delay

    def _dispatch_race(self, when, host, routing_key, msg):
        """emit the first message for a given key within a given window, then ignore same key within window"""
        # mk = set(msg.keys())
        key = self.key_for_msg(msg)
        # if set(msg.keys()) != mk:
        #     raise AssertionError('message was modified: keys %r changed' % (mk ^ set(msg.keys())))
        if key not in self.active_keys:
            LOG.debug("emitting race winner %s:%s for key %s" % (host, when, repr(key)))
            self.emit(routing_key, msg)
            self.active_keys[key] = when  # record when window for this was started
        else:
            LOG.info('ignoring redundant message from %s for key %s' % (host, repr(key)))

    def _dispatch_compete(self, when, host, routing_key, msg):
        key = self.key_for_msg(msg)
        if key not in self.active_keys:
            LOG.debug("window opening for key %s" % repr(key))
            self.active_keys[key] = when  # record when window for this was started
        self.msg_buffer[key].append((when, host, routing_key, msg))
        LOG.debug("added %s:%s to competition for key %s, started at %s, has %d entries" % (host, when, repr(key), repr(self.active_keys[key]), len(self.msg_buffer[key])))
        # we'll come back to this content when the window expires

    def __call__(self, when=None, host=None, routing_key=None, msg=None, *args, **kwargs):
        """dispatch a message, returning how long to wait for next message, or None"""
        if self.active_keys:
            self._clean_expired()

        # Case 0: No message provided, we just wanted aisle 6 cleaned up
        if msg is None:
            return self.max_sleep_til_next_window()

        # Case 1: if we have a key but no score, first message for a given key goes out
        #         and the rest are dropped within the window
        if (self.key_code is not None) and (self.score_code is None):
            self._dispatch_race(when, host, routing_key, msg)
            return self.max_sleep_til_next_window()

        # Case 2: if we have a key and a scoring mechanism, buffer messages
        #         and compete them when window closes
        elif (self.key_code is not None) and (self.score_code is not None):
            self._dispatch_compete(when, host, routing_key, msg)
            return self.max_sleep_til_next_window()

        # Case 3: no key and no score, we just emit everything
        else:
            self.emit(routing_key, msg)
            return None


def multi_main(servers, opts, args, extra_eval_namespace):
    """ simultaneously receive messages from multiple servers
    include deduplication window and optional scoring of messages
    ref https://gitlab.ssec.wisc.edu/rayg/flaregun/issues/2
    :param servers: list of dictionaries with server parameters to be dished out to process pool
    :param opts: global configuration options
    :return: 0 for success
    """
    queue = mp.Queue()
    LOG.debug("creating payload transforms")
    transforms = Transforms(opts.transforms, extra_eval_namespace)
    LOG.debug("creating message dispatcher")
    dispatch = Dispatcher(transforms, extra_eval_namespace, opts)
    LOG.debug("creating %d worker processes" % len(servers))
    workers = [mp.Process(target=worker_main, args=(server, queue)) for server in servers]
    LOG.info("starting %d worker processes" % len(workers))
    [w.start() for w in workers]

    import atexit
    for w in workers:
        def exterminate(pid=w.pid):
            try:
                os.kill(pid, SIGTERM)
            except:
                pass
        atexit.register(exterminate)

    if opts.timeout:
        LOG.info("priming timeout for %s seconds" % opts.timeout)
        signal(SIGALRM, handle_timeout)
        alarm(opts.timeout)

    rc = 0
    max_wait = None
    try:
        while max_wait is not Dispatcher.QUIT:
            try:
                when, host, routing_key, msg = queue.get(True, max_wait)
                LOG.debug("received message from %s" % host)
            except Queue.Empty as timed_out_for_cleanup:
                LOG.debug("dispatching a cleanup pass after timeout")
                max_wait = dispatch()
                continue
            max_wait = dispatch(when, host, routing_key, msg)
            LOG.debug("have %ss to await next message before cleanup" % (max_wait if (max_wait is not None) else "eon"))
    except KeyboardInterrupt as solongandthanksforallthefish:
        LOG.warning("keyboard interrupt, exiting")
    except TimeoutException as zzz:
        LOG.warning("timeout due to no messages emitted, exiting")
        rc = 2
    LOG.debug("QUIT received, exiting")
    [w.terminate() for w in workers]
    LOG.debug("awaiting child processes")
    [w.join() for w in workers]
    LOG.debug("goodbye")
    return rc


def single_main(server_params, opts, args, extra_eval_namespace):
    """single-server main without multiprocessing
    """
    LOG.info("classical mode")
    if len(args) == 1:
        server_params['key'] = args[0]
    session = AmqpExchange(**server_params)

    LOG.debug("creating payload transforms")
    transforms = Transforms(opts.transforms, extra_eval_namespace)
    LOG.debug("creating message dispatcher")
    extra_defaults = {'__reception_host__': server_params['host']}
    dispatch = Dispatcher(transforms, extra_eval_namespace, opts, extra_defaults)

    def callback(routing_key, body, dispatch=dispatch):
        dispatch.emit(routing_key, body)
    # if (opts.json is not None) or opts.transforms:
    #     def callback(routing_key, body, json_fmt=opts.json, transforms=transforms):
    #         jsondict_callback(routing_key, body, json_fmt, transforms)
    # else:
    #     LOG.info("no JSON payload assumptions (i.e. flat printable text)")
    #     # classical payload dump
    #     callback = default_callback
    if 'TEST_ADDE_ABI' in os.environ:
        callback = test_adde_abi_callback()

    if opts.timeout:
        signal(SIGALRM, handle_timeout)
        alarm(opts.timeout)

    try:
        session.consume(callback, timeout=opts.timeout)
    except KeyboardInterrupt as solongandthanksforallthefish:
        LOG.warning("keyboard interrupt, exiting")
        return 0
    except TimeoutException as zzz:
        LOG.warning("timeout due to no messages emitted, exiting")
        return 1
    session.close()
    return 0


def zap(*seqs):
    """zip, but with basic broadcasting and length consistency checks
    """
    seqs = [(list(s) if (s is not None) else []) for s in seqs]
    iters = max([len(q) for q in seqs])
    lens = [len(s) for s in seqs]
    for i in range(iters):
        z = []
        for n,el in zip(lens,seqs):
            if n==0:
                z.append(None)
            elif n==1:
                z.append(el[0])
            elif n==iters:
                z.append(el[i])
            else:
                raise ValueError('inconsistent number of values is ambiguous, need %d more after %s' % (iters-n, repr(el)))
        yield tuple(z)

USAGE="""Subscribes to and outputs messages from one or more AMQP server generating JSON dictionary payloads.
Typically writes out single line of text per message emitted. 
Typically used with xargs -L1 or other downstream pipe-accepting scripts as an AMQP alternative to 'find'. 

Can emit specific formats of message content using --json '{payloadkey} {payloadkey} {payloadkey}' per python format strings.
Can race (--window with --key) multiple servers for messages having matching keys.
Can score (--window with --key and --score) messages from multiple servers to emit best-in-window for a given key-window.
Can provide elementary output dumps for debug / development purposes.
Use of -vvv "debug" mode is highly recommended for multi-server recipe creation.
"re" regular expressions module available for expressions and transform statements applied to content.
Advanced users can wrap amqpfind.main() to create custom functions for use in --key, --transform, --score expressions.
More information at https://docs.google.com/document/d/1OQV0ewHOyupsFO_MNyu0qBowS3InsprvWmiMunbXbMg/edit 
Repository at https://gitlab.ssec.wisc.edu/rayg/flaregun

"""

def main_options():
    """Return application settings"""
    global OPTS
    return OPTS

def main(add_options=[], **extra_eval_namespace):
    parser = optparse.OptionParser(usage=USAGE)
    parser.add_option('-v', '--verbose', dest='verbosity', action="count", default=0,
                        help='each occurrence increases verbosity 1 level through ERROR-WARNING-INFO-DEBUG')
    parser.add_option('-d', '--debug', dest='debug', action='store_true',
                        help="enable interactive PDB debugger on exception")
    # http://docs.python.org/2.7/library/argparse.html#nargs
    # parser.add_option('--stuff', nargs='5', dest='my_stuff',
    # help="one or more random things")

    # server connection options
    parser.add_option('-H', '--host', dest='hosts', action="append",
                        help='name of AMQP server to connect to [MULTIPLE ALLOWED]')
    parser.add_option('-X', '--exchange', dest='exchanges', action="append",
                        help='name of exchange to connect to [MULTIPLE ALLOWED]')
    parser.add_option('-u', '--user', dest='users', action="append",
                        help='user id to talk to AMQP exchange as [MULTIPLE ALLOWED]')
    parser.add_option('-p', '--passwd', dest='passwds', action="append",
                        help='password for user [MULTIPLE ALLOWED]')
    parser.add_option('-C', '--consume', dest='consumes', action="append",
                        help='AMQP topic pattern to listen for and consume [MULTIPLE ALLOWED]')
    parser.add_option('-D', '--durable', dest='durables', action="append",
                        help='ALPHA: name of durable queue to use, or @ for hostname; default is to auto-delete queue [MULTIPLE ALLOWED]')

    # basic multi-server options
    parser.add_option('-P', '--produce', dest='produce', default=None,
                        help='topic pattern to send an event under using JSON from stdin')
    parser.add_option('-j', '--json', dest='json',
                        help='parse content as json and print this expression using python string.format(). -j \'?\' for easy-to-read debug output. -j \'*\' for null-terminated [topic, contentdict] JSON for use with xargs -0 -n1')
    parser.add_option('-c', '--callback', dest='callback', default=None,
                        help="name of custom python function of form callback(topic: str, content: dict) to emit messages (see guide; for advanced users)")
    parser.add_option('-T', '--transform', dest='transforms', action="append",
                        help='pythonic transform content of selected message key before output, e.g. "path=path.replace(\'/here\', \'/there\')" [MULTIPLE ALLOWED]')
    parser.add_option('-t', '--timeout', dest='timeout', type=int, default=0,
                        help='exit with error if no messages emitted for N seconds')
    parser.add_option('-i', '--id', dest='id', default=None,
                        help='dummy argument to help identify an amqpfind process amongst all other amqpfind processes. Not used anywhere in the code')

    # deduplication and scoring options
    parser.add_option('-w', '--window', dest='window', type="float",
                      help='number of seconds to keep keys around for scoring or racing competing messages')
    parser.add_option('-k', '--key', dest='key', default=None,
                        help='simple expression returning a key tuple for multi-server JSON message deduplication, e.g. "(sat, scene, time)"')
    parser.add_option('-s', '--score', dest='score', default=None,
                      help='simple expression returning a comparable score value to maximize e.g. "defects" for message having a defects integer value. or: python lambda returning the better of two message dictionaries, e.g. lambda a,b: a if a["defects"] < b["defects"] else b')

    if callable(add_options):
        add_options(parser)

    opts, args = parser.parse_args()
    global OPTS
    OPTS = opts

    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(3, opts.verbosity)])

    if PIKA_MAJOR_VERSION < 1:
        LOG.warning("pika module provided in environment is deprecated, please consider updating to pika >= 1.0, or removing pika from environment. amqpfind comes bundled with a more recent pika module.")

    if opts.debug:
        sys.excepthook = _debug

    # if not opts.exchanges:
    #     unittest.main()
    #     return 0

    # set up additional symbols to make available in key/score/transform expressions
    if extra_eval_namespace:
        LOG.info("extra symbols for evaluation namespace: %s" % repr(tuple(sorted(extra_eval_namespace.keys()))))

    servers = []
    for host, exchange, user, passwd, consume, durable in zap(opts.hosts, opts.exchanges, opts.users, opts.passwds, opts.consumes, opts.durables):
        servers.append(dict(host=host, exchange=exchange,
                            user=user, password=passwd,
                            key=consume, json=opts.json,
                            timeout=opts.timeout, durable=True if durable=="@" else durable))
    LOG.info('server settings: ' + repr(servers))

    if opts.produce is not None:
        assert(len(servers)==1)  # for now, only support send to single server
        session = AmqpExchange(**(servers[0]))
        jsontxt = sys.stdin.read().strip()
        content = json.loads(jsontxt)
        session.produce(content, key=opts.produce, exchange=opts.exchange)
        return 0

    if len(servers)==1:  # classical configuration
        if opts.key or opts.window or opts.score:
            LOG.error("Key, Window, and Score options require multiple servers to operate")
            return 1
        return single_main(servers[0], opts, args, extra_eval_namespace)
    else:  # more than one server, time for multiprocessing
        return multi_main(servers, opts, args, extra_eval_namespace)


if __name__ == '__main__':
    sys.exit(main())
