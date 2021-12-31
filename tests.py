#! /usr/bin/env python3
# encoding: utf-8

import unittest
import logging
import asyncio

from time import time

from task_graph import (
    Result,
    TaskExecutor,
    Cond,
    Func,
    NoOp,
    TERM_OP,
    GatherN,
    AllOk
)


TAU_SHORT = 0  # for really small delays


async def parse_int(s: str) -> int:
    await asyncio.sleep(TAU_SHORT)
    return int(s)


async def double_int(f: int) -> int:
    await asyncio.sleep(TAU_SHORT)
    return f * 2


class ResultTest(unittest.TestCase):
    def test_lift(self):
        e1 = Exception("abc")
        c1 = parse_int("5")
        self.assertEqual(Result.lift(1), Result(Result.OK, 1))
        self.assertEqual(Result.lift(e1), Result(Result.ERR, e1))
        self.assertEqual(Result.lift(c1), Result(Result.WAIT, c1))
        self.assertRaises(AssertionError, lambda: Result.lift(Result.IGNORED_ERRORS[0]("abc")))

    def test_map(self):
        # async
        self.assertEqual((Result.lift("5") | parse_int).box.__qualname__, parse_int("5").__qualname__)
        self.assertRaises(ValueError, asyncio.run((Result.lift("x") | parse_int).wait()).materialize)
        # sync
        self.assertEqual(Result.lift("5") | int, Result(Result.OK, 5))
        self.assertRaises(ValueError, (Result.lift("x") | int).materialize)


class NoOpTest(unittest.TestCase):
    def test_correct(self):
        """here we test simplest use-case"""
        fst = NoOp(Result.lift("1") | parse_int)  # this will be our task graph root
        fst >= NoOp(Result.lift("two") | parse_int) >= TERM_OP  # chain `add_subscriber` calls
        ((snd_,), fst_) = asyncio.run(fst.run(trigger=None))  # NoOp can have no trigger
        del fst
        ((term_,), snd_) = asyncio.run(snd_.run(trigger=fst_))  # second was `triggered` by first
        self.assertEqual(~~fst_, 1)  # unwrap Operation, materialize Result monad
        self.assertRaises(ValueError, lambda: ~~snd_)  # unwrapping failed task causes error 
        self.assertIs(term_, TERM_OP)  # failed NoOp still notifies subscribers

    def test_trigger_err(self):
        fst = NoOp(Result.lift("minus one") | parse_int)
        fst >= NoOp(Result.lift("2") | parse_int) >= TERM_OP
        ((snd_,), fst_) = asyncio.run(fst.run(trigger=None))
        del fst
        ((term_,), snd_) = asyncio.run(snd_.run(trigger=fst_))
        self.assertRaises(ValueError, lambda: ~~fst_)
        self.assertEqual(term_, TERM_OP)
        self.assertEqual(snd_.result.state, Result.OK)  # NoOp basically ignores trigger.

    def test_not_awaited(self):
        fst = NoOp(Result.lift("1") | parse_int)
        snd = fst >= NoOp(Result.lift("2") | parse_int)
        self.assertRaises(AssertionError, asyncio.run, snd.run(fst))  # fails if assert statements are optimized


class CondTest(unittest.TestCase):
    def test_true(self):
        fst = NoOp(Result.lift("1") | parse_int)
        cond: Cond = fst >= Cond([], [], )
        true = cond >= NoOp(Result.lift(True))
        false = cond @ NoOp(Result.lift(False))
        del cond
        ((cond_,), fst_) = asyncio.run(fst.run(None))
        ((sub_,), cond_) = asyncio.run(cond_.run(fst_))
        self.assertIs(sub_, true)

    def test_false(self):
        fst = NoOp(Result.lift("abracadabra") | parse_int)
        cond: Cond = fst >= Cond([], [], )  # type: ignore
        true = cond >= NoOp(Result.lift(True))
        false = cond @ NoOp(Result.lift(False))
        del cond
        ((cond_,), fst_) = asyncio.run(fst.run(None))
        ((sub_,), cond_) = asyncio.run(cond_.run(fst_))
        self.assertIs(sub_, false)

    def test_chained(self):
        false = [NoOp(Result.lift(x) | parse_int) for x in "ab3d"]
        Cond.chained(false, true=TERM_OP)
        op = false[0]
        trig = None
        for i in range(6):  # true will be for Cond3, which will be called on the 6th step
            (op,), trig = asyncio.run(op.run(trig))
        self.assertIs(op, TERM_OP)


class FuncTest(unittest.TestCase):
    def test_normal(self):
        fst = NoOp(Result.lift("1") | parse_int)
        f: Func = fst >= Func(double_int)  # type: ignore
        asyncio.run(fst.run(None))
        self.assertRaises(AssertionError, f.unwrap)
        asyncio.run(f.run(fst))
        self.assertEqual(~~f, 2)

    def test_previous_error(self):
        fst = NoOp(Result.lift("int") | parse_int)
        f: Func = fst >= Func(double_int)  # type: ignore
        f >= TERM_OP
        asyncio.run(fst.run(None))
        ((term_,), f_) = asyncio.run(f.run(fst))
        self.assertRaises(ValueError, lambda: ~~f)
        self.assertIs(term_, TERM_OP)  # note that it still returns subscribers, even if result is an error


class AllOkTest(unittest.TestCase):
    def test_success(self):
        ok1 = NoOp(Result.lift(1), name="a")
        ok2 = NoOp(Result.lift(2), name="b")
        wait = AllOk([ok1, ok2])
        asyncio.run(wait.run(None))
        self.assertEqual(~~wait, {"a": 1, "b": 2})

    def test_failure(self):
        ok1 = NoOp(Result.lift(1))
        err2 = NoOp(Result.lift(KeyError("key was lost")))
        wait = AllOk([ok1, err2])
        asyncio.run(wait.run(None))
        self.assertRaises(KeyError, lambda: ~~wait)

    def test_waiting(self):
        ok1 = NoOp(Result.lift(1))
        pending2 = NoOp(Result.lift("1") | parse_int)
        err3 = NoOp(Result.lift(KeyError("This key is secret")))
        wait = AllOk([ok1, pending2])  # should return []
        empty_, _ = asyncio.run(wait.run(None))
        self.assertEqual(empty_, [])
        self.assertRaises(AssertionError, wait.unwrap)

        wait_err = AllOk([ok1, pending2, err3])  # should return subs and assign failed Result to result
        wait_err >= TERM_OP
        x_, _ = asyncio.run(wait_err.run(None))
        self.assertEqual(x_, [TERM_OP, ])
        self.assertRaises(KeyError, lambda: ~~wait_err)


TAU_LONG = 1
EPSILON = 0.02  # error in percents


async def linear_time_complexity(x: float) -> float:
    if x < 0:
        raise ValueError("x < 0")
    await asyncio.sleep(x * TAU_LONG)
    return x


class RunGraphTest(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        logging.basicConfig(
            filename=None,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            level=logging.INFO)

    def test_all_ok(self):
        a, b, c, z = (NoOp(Result.lift(x) | parse_int, name=x) for x in "123z")
        z >= a >= c
        b >= c
        AllOk([a, b, c]) >= TERM_OP
        executor = TaskExecutor(logging.getLogger("all-ok-test"))
        wait = asyncio.run(executor(ops=[z, b]))
        self.assertRaises(ValueError, lambda: ~~z)
        self.assertEqual({"1": 1, "2": 2, "3": 3}, ~~wait)

    def test_race(self):
        roots = [NoOp(Result.lift(x) | parse_int, name=x) for x in ("1.2", "2", "1")]
        stems = [r >= Func(linear_time_complexity, name=r.name) for r in roots]
        GatherN(stems, 2) >= TERM_OP
        executor = TaskExecutor(logging.getLogger("race-test"))
        gather = asyncio.run(executor(ops=roots))
        self.assertTrue(isinstance((~~gather)["1.2"], ValueError))
        self.assertTrue("2" not in ~~gather)

    def test_timings(self):
        roots = [NoOp(Result.lift(x) | parse_int, name=x) for x in ("1.2", "2", "1")]
        stems = [r >= Func(linear_time_complexity, name=r.name) for r in roots]
        GatherN(stems, 2) >= TERM_OP
        executor = TaskExecutor(logging.getLogger("timings-test"))
        start = time()
        asyncio.run(executor(roots), debug=True)
        stop = time()
        should_take = 1.0  # TAU
        self.assertTrue(
            (stop - start) / TAU_LONG - should_take < EPSILON,
            msg=f"took: {(stop - start) / TAU_LONG}, but should: {should_take}+/-({EPSILON}%)"
        )

    def test_tracing(self):
        self.maxDiff = None
        target_graph = list()
        # noop
        roots = [NoOp(Result.lift(x) | parse_int, name=x) for x in ("1.2", "2", "1")]
        target_graph.extend((None, r, None) for r in roots)
        # func
        stems = [r >= Func(linear_time_complexity, name="f" + r.name) for r in roots]
        target_graph.extend((r, s, None) for r, s in zip(roots, stems))
        # cond
        unachievable = NoOp(Result.lift(Exception("Unachievable")), name="Unachievable")
        achievable = NoOp(Result.lift("This is achievable"), name="Achievable")
        cond = stems[0] >= Cond([unachievable], [achievable], name="condition")
        target_graph.extend((
            (stems[0], cond, None),
            (cond, unachievable, True),
            (cond, achievable, False)
        ))
        # gather, wait
        gather = GatherN(stems, 2, name="gatherer")
        target_graph.extend((s, gather, None) for s in stems)
        wait_all = AllOk([gather, achievable], name="waiter")
        wait_all >= TERM_OP
        target_graph.extend((
            (gather, wait_all, None),
            (achievable, wait_all, None),
            (wait_all, TERM_OP, None)
         ))
        traced = TaskExecutor.trace_graph(roots)
        self.assertEqual(len(traced), len(set(traced)))
        self.assertEqual(set(traced), set(target_graph))
        logging.getLogger("tracing-test").info(
            "\n==== graph trace ====\n" +
            "\n".join(f"{v1 if v1 is None else v1.name}  --|  {e}  |-->  {v2.name}" for v1, v2, e in traced) +
            "\n" + "=" * 10
        )


class ReadMeExampleTests(unittest.TestCase):
    def test_t1(self):
        from time import time
        import asyncio
        import task_graph as tg

        # set up some dummy async functions:
        async def str_to_int(x: str) -> int:
            await asyncio.sleep(10)  # conversion to int is really complex :\
            return int(x)

        async def str_to_float(x: str) -> float:
            await asyncio.sleep(3)  # float is much easier
            return float(x)

        async def linear_complexity(x: float) -> float:
            await asyncio.sleep(x)
            return x

        async def bad_task(x) -> None:
            raise Exception("error occurred mid-task")

        def gather_unwrap(x: dict) -> float:
            return x.popitem()[1]  # tg.GatherN result is a Dict[str, Union[Any, Exception]]

        # now build the graph:
        start_node = tg.NoOp(tg.Result.lift("2.0"))
        to_int_node = start_node >= tg.Func(str_to_int)
        # `>=` is a "do_after" operator for tg.Operation subclasses. returns second operand
        to_float_node = start_node >= tg.Func(str_to_float)  # parallel with to_int_node
        gather = tg.GatherN([to_int_node, to_float_node], n=1)  # wait for one task to finish
        unwrap_result_node: tg.Func = gather >= tg.Func(gather_unwrap)
        bad_node = unwrap_result_node >= NoOp(tg.Result.lift("any").map(bad_task))  # will immediately fail
        lin_complexity_node = unwrap_result_node | linear_complexity
        lin_complexity_node >= tg.TERM_OP  # TERM_OP is a termination signal

        # print the graph
        print("Task Graph:\n" + "\n".join(
            f"{v1}  --| {e12} |-->  {v2}" for (v1, v2, e12)
            in tg.TaskExecutor.trace_graph([start_node, ])
        ))

        # run the graph
        executor = tg.TaskExecutor()
        start_time = time()
        lin_complexity_node_ = asyncio.run(executor([start_node, ]))  # Operation preceding TERM_OP is returned
        execution_time = time() - start_time

        # examine the results
        print("execution time: ", execution_time)
        assert (~~lin_complexity_node_) == 2.0
        print(f"{bad_node} result was {repr(bad_node.result.box)}")  # this was bound to fail, remember?
        try:
            ~~to_int_node  # trying to unwrap it will raise an error,
            # since task takes 10 s but the TERM_OP was encountered in ~ 5s
        except asyncio.futures.CancelledError as e:
            print(f"{to_int_node} was cancelled")


if __name__ == "__main__":
    unittest.main()
