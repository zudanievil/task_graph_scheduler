# encoding: utf-8
"""
Small stdlib-based library for scheduling `asyncio.Task` with complex logical relationships.
Works on a version of "result" monad (class `Result`)
and `asyncio.Future` wrapper -- class `Operation` and subclasses.
Helper class `TaskExecutor` provides scheduling logic.
Tested on python 3.7, 3.9, 3.10 but should work with earlier python3 versions.
"""

import logging
from typing import (
    Generic,
    TypeVar,
    Awaitable,
    Union,
    Callable,
    List,
    Optional,
    Iterable,
    Tuple,
    Any
)
import asyncio

_T1 = TypeVar("_T1")
_T2 = TypeVar("_T2")

_async_map_t = Callable[[_T1], Awaitable[_T2]]
_sync_map_t = Callable[[_T1], _T2]


def _assert(b: bool, msg: str):
    """permanent `assert`"""
    if not b:
        raise AssertionError("Illegal State. " + msg)


def _id_dict(it: Iterable[Tuple[_T1, _T2]]):
    return {id(k): v for k, v in it}


class Result(Generic[_T1]):
    """
    A variation of `result monad` that can have a Future as an underlying object.
    Has 3 states: WAIT (Future not awaited), OK, ERR.
    Apart from normal monadic methods (`lift`, `map`, `materialize`)
    has async `wait` method, that awaits underlying Future.
    Does not catch errors from IGNORED_ERRORS (because it would be stupid).
    """
    IGNORED_ERRORS = (KeyboardInterrupt, SyntaxError, NameError,)
    # I never understood why you can catch a SyntaxError. This is such a bad idea.
    __slots__ = "state", "box"
    ERR = 1
    OK = 0
    WAIT = -1

    def __init__(self, state: int, box: Union[_T1, Awaitable[_T1], Exception]):
        """
        This constructor does not perform type checks. prefer `lift`, which does.
        """
        self.state = state
        self.box = box

    @classmethod
    def lift(cls, x: Union[_T1, Awaitable[_T1], Exception]) -> "Result[_T1]":
        """Constructs monad from a normal value. aka return, unit."""
        if isinstance(x, BaseException):
            _assert(type(x) not in cls.IGNORED_ERRORS, f"{x} is in Result.IGNORED_ERRORS")
            return cls(Result.ERR, x)
        elif hasattr(x, "__await__"):
            return cls(Result.WAIT, x)
        else:
            return cls(Result.OK, x)

    def map(self, f: Union[_async_map_t, _sync_map_t]) -> "Result[_T2]":
        """
        :returns new Result with state WAIT or
        :returns new Result with state ERR
        :raises asyncio.InvalidStateError if `self.state` was WAIT
        """
        _assert(self.state != Result.WAIT, "Result was never awaited")
        if self.state == Result.OK:
            try:
                if asyncio.iscoroutinefunction(f):
                    return Result(Result.WAIT, f(self.box))
                else:
                    return Result(Result.OK, f(self.box))
            except self.IGNORED_ERRORS as e:
                raise e
            except Exception as e:
                return Result(Result.ERR, e)
        else:
            return Result(Result.ERR, self.box)

    async def wait(self) -> "Result[_T1]":
        """
        await until Result is complete. This method MUTATES internal STATE
        :returns self for chaining methods
        """
        if self.state == Result.WAIT:
            try:
                self.box = await self.box
                self.state = Result.OK
            except self.IGNORED_ERRORS as e:
                raise e
            except Exception as e:
                self.box = e
                self.state = Result.ERR
        return self

    def materialize(self) -> _T1:
        """
        :returns boxed value
        :raises error captured in `self.box`
        :raises `asyncio.InvalidStateError` if never awaited
        """
        _assert(self.state != Result.WAIT, "Result was never awaited")
        if self.state == Result.OK:
            return self.box
        else:
            raise self.box

    def __repr__(self) -> str:
        return f"{__name__}.{self.__class__.__name__}({self.state}, {self.box})"

    def __eq__(self, other: "Result") -> bool:
        return (self.state == other.state) and (self.box == other.box)

    __or__ = map
    __invert__ = materialize


_op_counter = iter(range((1 << 16) - 1))
_operation_trace_type = List[Tuple[Optional["Operation"], "Operation", Optional[Any]]]
_operation_run_ret_type = Tuple[List["Operation"], "Operation"]


class Operation:
    """
    Allows to chain instances :class `Result` together. Must be subclassed.
    Essentially, it is a Future with separated instantiation (`__init__`, `add_subscriber`),
    scheduling (`run`) and result usage (`materialize`)
    and with improved reflection capabilities.
    fields:
    subscribers: operations that will be triggered after this one is completed
    result: stores :type Result that represents operation result

    """
    __slots__ = "name", "subscribers", "result"

    def __init__(self, name: Optional[str] = None):
        """call `super().__init__()` when subclassing."""
        self.name = self._make_name() if name is None else name
        # it's not likely someone will need > 65K Operations in one run
        self.subscribers: List[Operation] = []
        self.result: Optional[Result] = None

    def _make_name(self) -> str:
        return f"{next(_op_counter):04x}_{self.__class__.__name__}"

    def add_subscriber(self, other: "Operation") -> "Operation":
        """:returns `other` for chaining methods"""
        self.subscribers.append(other)
        return other

    async def run(self, trigger: Optional["Operation"]) -> _operation_run_ret_type:
        """
        trigger is an operation that called this one. it MUST NOT have Result.WAIT state.
        override in subclasses. CAN await and mutate `self.result` if needed.
        MUST :return list of operations to schedule (may schedule itself) or `[]` (Nothing will be scheduled)
        AND :return self (for convenience).
        """
        ...

    def unwrap(self) -> Result:
        """:returns self.result
        :raises AssertionError if result is None"""
        _assert(self.result is not None, "No result")
        return self.result

    def __repr__(self) -> str:
        # although it does not conform to "recreate an object" standard, it gives useful info for debugging
        return f"{self.name}({repr(self.result)})"

    def get_trace(self) -> _operation_trace_type:
        """
        for reflection. override if needed. returns list of (self, subscriber_operation, connection_type)
        ie (vertex1, vertex2, edge). connection_type is None if not overridden.
        """
        return [(self, sub, None) for sub in self.subscribers]

    __ge__ = add_subscriber
    __invert__ = unwrap


TERM_OP = Operation(name="TERM_OP")


class NoOp(Operation):
    def __init__(self, result: Result, name: Optional[str] = None):
        super().__init__(name)
        self.result = result

    async def run(self, trigger: Optional[Operation]) -> _operation_run_ret_type:
        """Ignores trigger state. awaits result. :returns list of subscribers"""
        assert (trigger is None) or (trigger.result.state != Result.WAIT), \
            "ops with Result.WAIT should not be triggers"
        await self.result.wait()
        return self.subscribers, self


_alias1 = Union[Operation, List[Operation]]


class Cond(Operation):
    __slots__ = "condition", "false_subs"

    def __init__(
            self,
            true_subs: List[Operation],
            false_subs: List[Operation],
            condition: Callable[[Operation], bool] = lambda op: op.unwrap().state == Result.OK,
            name: Optional[str] = None):
        super().__init__(name)
        self.subscribers = true_subs
        self.condition: Callable[[Operation], bool] = condition
        self.false_subs: List[Operation] = false_subs

    def add_false_sub(self, other: Operation) -> Operation:
        """analogous to self.add_subscriber"""
        self.false_subs.append(other)
        return other

    __matmul__ = add_false_sub

    async def run(self, trigger: Operation) -> _operation_run_ret_type:
        """
        set self.result to trigger.result.
        evaluate condition on a trigger.
        if True :return self.subscribers, if False :return self.false_subs
        """
        assert trigger.result.state != Result.WAIT, "ops with Result.WAIT cannot be triggers"
        self.result = ~trigger
        return (self.subscribers if self.condition(trigger) else self.false_subs), self

    def get_trace(self) -> _operation_trace_type:
        return [
            *((self, sub, True) for sub in self.subscribers),
            *((self, sub, False) for sub in self.false_subs),
        ]

    @classmethod
    def chained(cls, false: Iterable[_alias1], true: _alias1,
                condition: Callable[[Operation], bool] = lambda op: op.unwrap().state == Result.OK, ) -> List["Cond"]:
        """
        if :param false length is N, returnes a list  of N-1 Conds.
        Conds[i] gets subscribed to false[i] and has false[i+1] if false_subs.
        every Cond adds true to it's true subscribers.
        """
        conds = []
        true = _coerce_to_list(true)
        for false_i, false_ip1 in zip(false[:-1], false[1:]):
            false_i = _coerce_to_list(false_i)
            false_ip1 = _coerce_to_list(false_ip1)
            cond = cls(true, false_ip1, condition)
            for f in false_i:
                _ = f >= cond
            conds.append(cond)
        return conds


def _coerce_to_list(x: Union[List[_T1], _T1]) -> List[_T1]:
    """:return x if isinstance(x, list) else [x, ]"""
    return x if isinstance(x, list) else [x, ]


class AllOk(Operation):
    __slots__ = "wait_list",

    def __init__(self, wait_list: List[Operation], name: Optional[str] = None):
        """subscribes to all ops on the wait_list"""
        super().__init__(name)
        self.wait_list: List[Operation] = wait_list
        for op in self.wait_list:
            op.add_subscriber(self)

    async def run(self, ignore: Optional[Operation]) -> _operation_run_ret_type:
        """
        ignores trigger value. scans wait_list.
        If one of the ops on the wait_list results in Result.ERR,
        assigns it to self.result and notifies subscribers.
        If all ops on the list are success, assigns new Result with state
        Result.OK and (wait_list_result_name: result.box) pairs, then notifies subscribers.
        Else returns []
        """
        waiting = False
        for op in self.wait_list:
            if op.result.state == Result.ERR:
                self.result = op.result
                return self.subscribers, self
            if op.result.state == Result.WAIT:
                waiting = True
                continue  # there still may be errors
        if waiting:
            return [], self
        else:
            self.result = Result(Result.OK, {op.name: ~~op for op in self.wait_list})
            return self.subscribers, self


class GatherN(Operation):
    __slots__ = "gathered", "n"

    def __init__(self, subscribe_to: List[Operation], n: Optional[int] = None, name: Optional[str] = None):
        """subscribes to all ops on the subscribe_to list. :param n defaults to len(subscribe_to)"""
        super().__init__(name)
        self.gathered: List[Operation] = []
        self.n = n or len(subscribe_to)
        for op in subscribe_to:
            op.add_subscriber(self)

    async def run(self, trigger: Optional["Operation"]) -> _operation_run_ret_type:
        """adds trigger to the gathered list. if n members is reached adds a new OK Result with
        underlying dict {op.name: op.result} to self.result and notifies the subscribers"""
        self.gathered.append(trigger)
        if len(self.gathered) >= self.n:
            self.result = Result(Result.OK, {op.name: (~op).box for op in self.gathered})
            return self.subscribers, self
        else:
            return [], self


class Func(Operation):
    """Unlike other Operations, this carries computational (rather than logical) workload"""
    __slots__ = "f",

    def __init__(self, f: Union[_async_map_t, _sync_map_t], name: Optional[str] = None):
        super().__init__(name)
        self.f = f

    async def run(self, trigger: Operation) -> _operation_run_ret_type:
        """applies (:class `Result.map`) async function to the trigger.result and awaits it's own result"""
        self.result = trigger.result.map(self.f)
        await self.result.wait()
        return self.subscribers, self

    def map(self, f: Union[_async_map_t, _sync_map_t]) -> "Func":
        """:return self >= Func(f)"""
        return self >= Func(f)

    __or__ = map


class TaskExecutor:
    """
    Helper class for the ease of partial application.
    Use `run_graph` to start task execution.
    """
    __slots__ = "logger", "term_event", "term_trigger"

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        :param logger: if None, `logging.getLogger(__name__)` is used.
        logging.INFO: logs operation scheduling, completion
        """
        self.logger: logging.Logger = logger or logging.getLogger(__name__)
        self.term_event: Optional[asyncio.Event] = None
        self.term_trigger: Optional[Operation] = None

    async def run_graph(self, ops: Iterable[Operation]) -> Operation:
        """
        executes operations until a special operation `TERM_OP` is encountered.
        returns operation that returned TERM_OP.
        you cannot have one instance running multiple graphs at the same time.
        """
        _assert(self.term_event is None, "self.term_event must be None at the start of execution")
        self.term_event = asyncio.Event()
        for op in ops:
            asyncio.Task(op.run(None)).add_done_callback(self._op_done_callback)
        await self.term_event.wait()
        return self.term_trigger

    __call__ = run_graph

    def _op_done_callback(self, task: "asyncio.Task[Tuple[List[Operation], Operation]]") -> None:
        """
        partially applied callback for asyncio.Task(operation.run(trigger_operation))
        if `self.term_event.is_set()`, exits immediately.
        if TERM_OP in subscribers, terminates `self.run_graph` and exits.
        else creates new `asyncio.Task` from the subscribers of the operation.
        """
        if self.term_event.is_set():
            return
        next_ops, trigger = task.result()
        self.logger.info(f"complete:  {trigger}")
        if TERM_OP in next_ops:
            self.logger.info(f"TERM_OP from:  {trigger}")
            self.term_trigger = trigger
            self.term_event.set()
            return
        for op in next_ops:
            asyncio.Task(op.run(trigger)).add_done_callback(self._op_done_callback)
            self.logger.info(f"running:  {op}  :with trigger:  {trigger}")

    @staticmethod
    def trace_graph(ops: Iterable[Operation]) -> _operation_trace_type:
        """trace operations (pseudo-recursively) with help of `Operation.get_trace`"""
        trace_accum = [(None, op, None) for op in ops]
        op_stack = [op for op in ops]
        processed = set(id(op) for op in ops)
        while len(op_stack) > 0:
            op = op_stack.pop()
            processed.add(id(op))
            op_trace = op.get_trace()
            trace_accum += op_trace
            op_stack.extend(ot[1] for ot in op_trace if id(ot[1]) not in processed)
        return trace_accum
