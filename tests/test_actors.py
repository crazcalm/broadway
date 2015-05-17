__author__ = 'kevinschoon@gmail.com'

import asyncio
import random


from broadway.actor import Actor
from broadway.actorsystem import ActorSystem
from broadway.context import Props
from broadway.eventbus import ActorEventBus


import unittest


class DummyActor(Actor):
    def __init__(self, name, partner=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.partner = partner

    @asyncio.coroutine
    def receive(self, message):
        print(self.name, message)
        if self.partner:
            yield from self.partner.tell(message)


class EchoActor(Actor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @asyncio.coroutine
    def receive(self, message):
        yield from self.context.sender.tell(message)


class TestActorSystem(unittest.TestCase):

    def setUp(self):
        self.system = ActorSystem()
        self.a = self.system.actor_of(Props(DummyActor, "repeat"))
        self.b = self.system.actor_of(Props(DummyActor, "hello", self.a))
        self.c = self.system.actor_of(Props(DummyActor, "bye   ", ))
        self.echoer = self.system.actor_of(Props(EchoActor), "echoer")

    @asyncio.coroutine
    def initialize(self, bus, a, b, c, echoer):
        count = 0
        while count < 60:
            count += 1
            if count <= 20:
                if random.random() < 0.5:
                    yield from a.tell("actor %s" % count)
                else:
                    yield from c.tell("actor %s" % count)
            elif count <= 40:
                if random.random() < 0.5:
                    yield from bus.publish("/hello", "eventbus %s" % count)
                else:
                    yield from bus.publish("/bye", "eventbus %s" % count)
            else:
                message = yield from echoer.ask("echo %s" % count)
                print(message)
            yield from asyncio.sleep(0.01)
        yield from self.system.stop()

    def test_setup(self):
        self.assertIsInstance(self.system, ActorSystem)
        for actor in [self.a, self.b, self.c]:
            self.assertIsInstance(actor, DummyActor)
        self.assertIsInstance(self.echoer, EchoActor)

    def test_run(self):
        bus = self.system.actor_of(Props(ActorEventBus), "bus")
        bus.subscribe("/hello", [self.b])
        bus.subscribe("/bye", [self.c])
        coro = [self.initialize(bus, self.a, self.b, self.c, self.echoer)]
        self.system.run_until_stop(coro, exit_after=False)
