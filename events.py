import sys
import os

class Event(object):
    pass

class BarEvent(Event):
    def __init__(self):
        self.TYPE = "BAR"

class TickEvent(Event):
    def __init__(self):
        self.TYPE = "TICK"

class OrderEvent(Event):
    def __init__(self):
        self.TYPE = "ORDER"

class OrderSubmitEvent(OrderEvent):
    def __init__(self):
        super().__init__()
        self.ORDER_TYPE = "SUBMIT"

class OrderCancelEvent(OrderEvent):
    def __init__(self):
        super().__init__()
        self.ORDER_TYPE = "CANCEL"

class OrderAmendEvent(OrderEvent):
    def __init__(self):
        super().__init__()
        self.ORDER_TYPE = "AMEND"

class AckEvent(Event):
    def __init__(self):
        self.TYPE = "ACK"

class OrderSubmittedEvent(AckEvent):
    def __init__(self):
        super().__init__()
        self.ACK_TYPE = "SUBMITTED"

class OrderCancelledEvent(AckEvent):
    def __init__(self):
        super().__init__()
        self.ACK_TYPE = "CANCELLED"

class OrderAmendedEvent(AckEvent):
    def __init__(self):
        super().__init__()
        self.ACK_TYPE = "AMENDED"

class OrderPartialFilledEvent(AckEvent):
    def __init__(self):
        super().__init__()
        self.ACK_TYPE = "PARTIAL_FILLED"

class OrderFilledEvent(AckEvent):
    def __init__(self):
        super().__init__()
        self.ACK_TYPE = "FILLED"

