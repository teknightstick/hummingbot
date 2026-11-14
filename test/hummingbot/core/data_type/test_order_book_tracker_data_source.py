#!/usr/bin/env python
"""
Tests for OrderBookTrackerDataSource base class.

This module tests:
- add_trading_pair: Adds a trading pair to the internal list
- remove_trading_pair: Removes a trading pair from the internal list
"""
import ast
import asyncio
import sys
import unittest
from pathlib import Path
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, patch

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSResponse
from hummingbot.core.web_assistant.ws_assistant import WSAssistant


_END_OF_STREAM = object()


EXPECTED_ORDER_STREAM_DEFINITIONS = {
    "hummingbot/connector/derivative/binance_perpetual/binance_perpetual_api_order_book_data_source.py:"
    "BinancePerpetualAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/derivative/bitget_perpetual/bitget_perpetual_api_order_book_data_source.py:"
    "BitgetPerpetualAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py:"
    "BybitPerpetualAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/derivative/evedex_perpetual/evedex_perpetual_api_order_book_data_source.py:"
    "EvedexPerpetualAPIOrderBookDataSource._on_order_stream_interruption",
    "hummingbot/connector/derivative/injective_v2_perpetual/"
    "injective_v2_perpetual_api_order_book_data_source.py:"
    "InjectiveV2PerpetualAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/derivative/pacifica_perpetual/pacifica_perpetual_api_order_book_data_source.py:"
    "PacificaPerpetualAPIOrderBookDataSource._on_order_stream_interruption",
    "hummingbot/connector/exchange/bing_x/bing_x_api_order_book_data_source.py:"
    "BingXAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/exchange/bitget/bitget_api_order_book_data_source.py:"
    "BitgetAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/exchange/bybit/bybit_api_order_book_data_source.py:"
    "BybitAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/exchange/gemini/gemini_api_order_book_data_source.py:"
    "GeminiAPIOrderBookDataSource._on_order_stream_interruption",
    "hummingbot/connector/exchange/injective_v2/injective_v2_api_order_book_data_source.py:"
    "InjectiveV2APIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/connector/exchange/xrpl/xrpl_api_order_book_data_source.py:"
    "XRPLAPIOrderBookDataSource.listen_for_subscriptions",
    "hummingbot/core/data_type/order_book_tracker_data_source.py:"
    "OrderBookTrackerDataSource.listen_for_subscriptions",
    "hummingbot/core/data_type/order_book_tracker_data_source.py:"
    "OrderBookTrackerDataSource._on_order_stream_interruption",
    "hummingbot/data_feed/candles_feed/bitget_perpetual_candles/bitget_perpetual_candles.py:"
    "BitgetPerpetualCandles.listen_for_subscriptions",
    "hummingbot/data_feed/candles_feed/bitget_spot_candles/bitget_spot_candles.py:"
    "BitgetSpotCandles.listen_for_subscriptions",
    "hummingbot/data_feed/candles_feed/btc_markets_spot_candles/btc_markets_spot_candles.py:"
    "BtcMarketsSpotCandles.listen_for_subscriptions",
    "hummingbot/data_feed/candles_feed/candles_base.py:CandlesBase.listen_for_subscriptions",
    "hummingbot/data_feed/candles_feed/candles_base.py:CandlesBase._on_order_stream_interruption",
    "hummingbot/data_feed/candles_feed/evedex_perpetual_candles/evedex_perpetual_candles.py:"
    "EvedexPerpetualCandles._on_order_stream_interruption",
    "hummingbot/data_feed/candles_feed/hyperliquid_perpetual_candles/hyperliquid_perpetual_candles.py:"
    "HyperliquidPerpetualCandles._on_order_stream_interruption",
    "hummingbot/data_feed/candles_feed/lighter_perpetual_candles/lighter_perpetual_candles.py:"
    "LighterPerpetualCandles.listen_for_subscriptions",
    "hummingbot/data_feed/candles_feed/lighter_spot_candles/lighter_spot_candles.py:"
    "LighterSpotCandles.listen_for_subscriptions",
    "hummingbot/data_feed/liquidations_feed/liquidations_base.py:LiquidationsBase.listen_for_subscriptions",
    "hummingbot/data_feed/liquidations_feed/liquidations_base.py:LiquidationsBase._on_order_stream_interruption",
}

ALTERNATE_LIFECYCLE_BOUNDARIES = {
    "hummingbot/core/data_type/order_book_tracker_data_source.py:"
    "OrderBookTrackerDataSource.listen_for_subscriptions": {"await_primary_then_cleanup"},
    "hummingbot/data_feed/candles_feed/candles_base.py:"
    "CandlesBase.listen_for_subscriptions": {"await_primary_then_cleanup"},
    "hummingbot/connector/derivative/bybit_perpetual/bybit_perpetual_api_order_book_data_source.py:"
    "BybitPerpetualAPIOrderBookDataSource.listen_for_subscriptions": {"tasks_future"},
    "hummingbot/connector/derivative/injective_v2_perpetual/"
    "injective_v2_perpetual_api_order_book_data_source.py:"
    "InjectiveV2PerpetualAPIOrderBookDataSource.listen_for_subscriptions": {"start"},
    "hummingbot/connector/exchange/bing_x/bing_x_api_order_book_data_source.py:"
    "BingXAPIOrderBookDataSource.listen_for_subscriptions": {"disconnect"},
    "hummingbot/connector/exchange/bybit/bybit_api_order_book_data_source.py:"
    "BybitAPIOrderBookDataSource.listen_for_subscriptions": {"disconnect"},
    "hummingbot/connector/exchange/injective_v2/injective_v2_api_order_book_data_source.py:"
    "InjectiveV2APIOrderBookDataSource.listen_for_subscriptions": {"start"},
    "hummingbot/connector/exchange/xrpl/xrpl_api_order_book_data_source.py:"
    "XRPLAPIOrderBookDataSource.listen_for_subscriptions": {"_close_subscription_connection"},
    "hummingbot/data_feed/candles_feed/btc_markets_spot_candles/btc_markets_spot_candles.py:"
    "BtcMarketsSpotCandles.listen_for_subscriptions": {"_polling_task"},
    "hummingbot/data_feed/candles_feed/lighter_perpetual_candles/lighter_perpetual_candles.py:"
    "LighterPerpetualCandles.listen_for_subscriptions": {"fetch_candles"},
    "hummingbot/data_feed/candles_feed/lighter_spot_candles/lighter_spot_candles.py:"
    "LighterSpotCandles.listen_for_subscriptions": {"fetch_candles"},
}


class OrderStreamOverrideStaticTests(unittest.TestCase):
    _UNKNOWN = object()

    @staticmethod
    def _production_definitions():
        repository_root = Path(__file__).resolve().parents[4]
        definitions = {}
        for path in sorted((repository_root / "hummingbot").rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            if not any(
                signature in source
                for signature in (
                    "async def listen_for_subscriptions",
                    "async def _on_order_stream_interruption",
                )
            ):
                continue
            tree = ast.parse(source, filename=str(path))
            relative_path = path.relative_to(repository_root).as_posix()
            for class_node in (node for node in tree.body if isinstance(node, ast.ClassDef)):
                for function_node in (
                    node
                    for node in class_node.body
                    if isinstance(node, ast.AsyncFunctionDef)
                    and node.name in {"listen_for_subscriptions", "_on_order_stream_interruption"}
                ):
                    key = f"{relative_path}:{class_node.name}.{function_node.name}"
                    definitions[key] = function_node
        return definitions

    @staticmethod
    def _assigned_self_attribute(node):
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
                return target.attr
        return None

    @classmethod
    def _constant_value(cls, node, shadowed_names=frozenset()):
        if isinstance(node, ast.Name) and node.id == "__debug__":
            return __debug__
        if isinstance(node, ast.UnaryOp):
            operand = cls._constant_value(node.operand, shadowed_names)
            if operand is cls._UNKNOWN:
                return cls._UNKNOWN
            try:
                if isinstance(node.op, ast.Not):
                    return not operand
                if isinstance(node.op, ast.UAdd):
                    return +operand
                if isinstance(node.op, ast.USub):
                    return -operand
                if isinstance(node.op, ast.Invert):
                    return ~operand
            except (TypeError, ValueError, OverflowError):
                return cls._UNKNOWN
            return cls._UNKNOWN
        if isinstance(node, ast.BoolOp):
            result = cls._UNKNOWN
            for value_node in node.values:
                value = cls._constant_value(value_node, shadowed_names)
                if value is cls._UNKNOWN:
                    return cls._UNKNOWN
                if isinstance(node.op, ast.And) and not bool(value):
                    return value
                if isinstance(node.op, ast.Or) and bool(value):
                    return value
                result = value
            return result
        if isinstance(node, ast.Compare):
            left = cls._constant_value(node.left, shadowed_names)
            if left is cls._UNKNOWN:
                return cls._UNKNOWN
            for operator, comparator_node in zip(node.ops, node.comparators):
                right = cls._constant_value(comparator_node, shadowed_names)
                if right is cls._UNKNOWN:
                    return cls._UNKNOWN
                try:
                    if isinstance(operator, ast.Eq):
                        comparison = left == right
                    elif isinstance(operator, ast.NotEq):
                        comparison = left != right
                    elif isinstance(operator, ast.Lt):
                        comparison = left < right
                    elif isinstance(operator, ast.LtE):
                        comparison = left <= right
                    elif isinstance(operator, ast.Gt):
                        comparison = left > right
                    elif isinstance(operator, ast.GtE):
                        comparison = left >= right
                    elif isinstance(operator, ast.Is):
                        comparison = left is right
                    elif isinstance(operator, ast.IsNot):
                        comparison = left is not right
                    elif isinstance(operator, ast.In):
                        comparison = left in right
                    elif isinstance(operator, ast.NotIn):
                        comparison = left not in right
                    else:
                        return cls._UNKNOWN
                except (TypeError, ValueError):
                    return cls._UNKNOWN
                if not comparison:
                    return False
                left = right
            return True
        if isinstance(node, ast.IfExp):
            truth_value = cls._constant_truth_value(node.test, shadowed_names)
            if truth_value is None:
                return cls._UNKNOWN
            return cls._constant_value(node.body if truth_value else node.orelse, shadowed_names)
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "range"
            and "range" not in shadowed_names
            and not node.keywords
        ):
            arguments = [cls._constant_value(argument, shadowed_names) for argument in node.args]
            if all(argument is not cls._UNKNOWN for argument in arguments):
                try:
                    return range(*arguments)
                except (TypeError, ValueError):
                    return cls._UNKNOWN
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "set"
            and not node.args
            and not node.keywords
        ):
            return cls._UNKNOWN if "set" in shadowed_names else set()
        try:
            return ast.literal_eval(node)
        except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
            return cls._UNKNOWN

    @classmethod
    def _constant_truth_value(cls, node, shadowed_names=frozenset()):
        if isinstance(node, ast.BoolOp):
            truth_values = [cls._constant_truth_value(value, shadowed_names) for value in node.values]
            if isinstance(node.op, ast.And):
                if False in truth_values:
                    return False
                return True if all(value is True for value in truth_values) else None
            if isinstance(node.op, ast.Or):
                if True in truth_values:
                    return True
                return False if all(value is False for value in truth_values) else None
        value = cls._constant_value(node, shadowed_names)
        return None if value is cls._UNKNOWN else bool(value)

    @classmethod
    def _executable_nodes_in_statements(
        cls,
        statements,
        shadowed_names=frozenset(),
        nonraising_names=frozenset(),
    ):
        nodes = []
        node_ids = set()

        def add_node(node):
            if id(node) not in node_ids:
                node_ids.add(id(node))
                nodes.append(node)

        def constant_value(node):
            return cls._constant_value(node, shadowed_names)

        def constant_truth_value(node):
            return cls._constant_truth_value(node, shadowed_names)

        def target_binding_succeeds(target, value):
            if isinstance(target, ast.Name):
                return True
            if isinstance(target, ast.Starred):
                return target_binding_succeeds(target.value, value)
            if not isinstance(target, (ast.Tuple, ast.List)):
                return None
            try:
                values = list(value)
            except TypeError:
                return False

            starred = [index for index, element in enumerate(target.elts) if isinstance(element, ast.Starred)]
            if not starred:
                if len(values) != len(target.elts):
                    return False
                pairs = zip(target.elts, values)
            else:
                starred_index = starred[0]
                trailing = len(target.elts) - starred_index - 1
                if len(values) < starred_index + trailing:
                    return False
                starred_end = len(values) - trailing
                pairs = [
                    *zip(target.elts[:starred_index], values[:starred_index]),
                    (target.elts[starred_index], values[starred_index:starred_end]),
                    *zip(target.elts[starred_index + 1:], values[len(values) - trailing:]),
                ]
            statuses = [target_binding_succeeds(element, item) for element, item in pairs]
            if False in statuses:
                return False
            return None if None in statuses else True

        def visit_comprehension(generators, index, visit_result):
            generator = generators[index]
            visit_expression(generator.iter)
            iterable = constant_value(generator.iter)
            if iterable is cls._UNKNOWN:
                item = cls._UNKNOWN
            else:
                try:
                    item = next(iter(iterable))
                except StopIteration:
                    return False
                except TypeError:
                    return False

            visit_expression(generator.target)
            if item is not cls._UNKNOWN and target_binding_succeeds(generator.target, item) is False:
                return False
            for condition in generator.ifs:
                visit_expression(condition)
                if constant_truth_value(condition) is False:
                    return False
            if index + 1 < len(generators):
                return visit_comprehension(generators, index + 1, visit_result)
            visit_result()
            return True

        def visit_expression(node):
            if node is None:
                return
            add_node(node)
            if isinstance(node, ast.Lambda):
                for default in (*node.args.defaults, *node.args.kw_defaults):
                    visit_expression(default)
                return
            if isinstance(node, ast.GeneratorExp):
                if node.generators:
                    visit_expression(node.generators[0].iter)
                return
            if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp)):
                def visit_result():
                    if isinstance(node, ast.DictComp):
                        visit_expression(node.key)
                        visit_expression(node.value)
                    else:
                        visit_expression(node.elt)

                if node.generators:
                    visit_comprehension(node.generators, 0, visit_result)
                return
            if isinstance(node, ast.BoolOp):
                for value_node in node.values:
                    visit_expression(value_node)
                    truth_value = constant_truth_value(value_node)
                    if isinstance(node.op, ast.And) and truth_value is False:
                        break
                    if isinstance(node.op, ast.Or) and truth_value is True:
                        break
                return
            if isinstance(node, ast.IfExp):
                visit_expression(node.test)
                truth_value = constant_truth_value(node.test)
                if truth_value is True:
                    visit_expression(node.body)
                elif truth_value is False:
                    visit_expression(node.orelse)
                else:
                    visit_expression(node.body)
                    visit_expression(node.orelse)
                return
            if isinstance(node, ast.Compare):
                visit_expression(node.left)
                left = node.left
                for operator, comparator in zip(node.ops, node.comparators):
                    visit_expression(comparator)
                    single_comparison = ast.Compare(left=left, ops=[operator], comparators=[comparator])
                    if constant_truth_value(single_comparison) is False:
                        break
                    left = comparator
                return
            for child in ast.iter_child_nodes(node):
                if not isinstance(child, ast.stmt):
                    visit_expression(child)

        def visit_pattern(node):
            add_node(node)
            if isinstance(node, ast.MatchValue):
                visit_expression(node.value)
            elif isinstance(node, ast.MatchSequence):
                for pattern in node.patterns:
                    visit_pattern(pattern)
            elif isinstance(node, ast.MatchMapping):
                for key in node.keys:
                    visit_expression(key)
                for pattern in node.patterns:
                    visit_pattern(pattern)
            elif isinstance(node, ast.MatchClass):
                visit_expression(node.cls)
                for pattern in (*node.patterns, *node.kwd_patterns):
                    visit_pattern(pattern)
            elif isinstance(node, ast.MatchAs):
                if node.pattern is not None:
                    visit_pattern(node.pattern)
            elif isinstance(node, ast.MatchOr):
                for pattern in node.patterns:
                    visit_pattern(pattern)

        def is_irrefutable_pattern(node):
            if isinstance(node, ast.MatchAs):
                return node.pattern is None or is_irrefutable_pattern(node.pattern)
            if isinstance(node, ast.MatchOr):
                return any(is_irrefutable_pattern(pattern) for pattern in node.patterns)
            if isinstance(node, ast.MatchClass):
                return (
                    isinstance(node.cls, ast.Name)
                    and node.cls.id == "object"
                    and "object" not in shadowed_names
                    and not node.patterns
                    and not node.kwd_patterns
                )
            return False

        def expression_may_raise(node):
            if node is None or isinstance(node, ast.Constant):
                return False
            if isinstance(node, ast.Name):
                return node.id != "__debug__" and node.id not in nonraising_names
            if isinstance(node, ast.Lambda):
                return any(expression_may_raise(default) for default in (*node.args.defaults, *node.args.kw_defaults))
            if isinstance(node, ast.GeneratorExp):
                return bool(node.generators) and expression_may_raise(node.generators[0].iter)
            if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
                return any(expression_may_raise(element) for element in node.elts)
            if isinstance(node, ast.Dict):
                return any(expression_may_raise(key) for key in node.keys if key is not None) or any(
                    expression_may_raise(value) for value in node.values
                )
            if isinstance(node, ast.BoolOp):
                for value_node in node.values:
                    if expression_may_raise(value_node):
                        return True
                    truth_value = constant_truth_value(value_node)
                    if isinstance(node.op, ast.And) and truth_value is False:
                        return False
                    if isinstance(node.op, ast.Or) and truth_value is True:
                        return False
                return False
            if isinstance(node, ast.IfExp):
                if expression_may_raise(node.test):
                    return True
                truth_value = constant_truth_value(node.test)
                if truth_value is True:
                    return expression_may_raise(node.body)
                if truth_value is False:
                    return expression_may_raise(node.orelse)
                return True
            if isinstance(node, ast.Compare):
                if expression_may_raise(node.left):
                    return True
                left = node.left
                for operator, comparator in zip(node.ops, node.comparators):
                    if expression_may_raise(comparator):
                        return True
                    comparison = ast.Compare(left=left, ops=[operator], comparators=[comparator])
                    truth_value = constant_truth_value(comparison)
                    if truth_value is None:
                        return True
                    if truth_value is False:
                        return False
                    left = comparator
                return False
            if isinstance(node, ast.UnaryOp):
                return expression_may_raise(node.operand) or constant_value(node) is cls._UNKNOWN
            return True

        def target_may_raise(node):
            if isinstance(node, ast.Name):
                return False
            if isinstance(node, (ast.Tuple, ast.List)):
                return any(target_may_raise(element) for element in node.elts)
            return True

        def statement_may_raise(node):
            if isinstance(node, (ast.Pass, ast.Break, ast.Continue)):
                return False
            if isinstance(node, ast.Expr):
                return expression_may_raise(node.value)
            if isinstance(node, ast.Return):
                return expression_may_raise(node.value)
            if isinstance(node, ast.Assign):
                return expression_may_raise(node.value) or any(target_may_raise(target) for target in node.targets)
            if isinstance(node, ast.AnnAssign):
                return expression_may_raise(node.value) or target_may_raise(node.target)
            if isinstance(node, ast.Assert):
                if not __debug__:
                    return False
                truth_value = constant_truth_value(node.test)
                return truth_value is not True or expression_may_raise(node.test)
            if isinstance(node, ast.If):
                if expression_may_raise(node.test):
                    return True
                truth_value = constant_truth_value(node.test)
                if truth_value is True:
                    return statements_may_raise(node.body)
                if truth_value is False:
                    return statements_may_raise(node.orelse)
                return True
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                expressions = [*node.decorator_list, *node.args.defaults, *node.args.kw_defaults]
                return any(expression_may_raise(expression) for expression in expressions if expression is not None)
            return True

        def statements_may_raise(block):
            for statement in block:
                if statement_may_raise(statement):
                    return True
                if isinstance(statement, (ast.Return, ast.Break, ast.Continue)):
                    return False
            return False

        def visit_statements(block):
            exits = {"fallthrough"}
            for statement in block:
                if "fallthrough" not in exits:
                    break
                completed_paths = exits - {"fallthrough"}
                exits = completed_paths | visit_statement(statement)
            return exits

        def visit_statement(node):
            add_node(node)
            if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                for decorator in node.decorator_list:
                    visit_expression(decorator)
                for default in (*node.args.defaults, *node.args.kw_defaults):
                    visit_expression(default)
                return {"fallthrough"}
            if isinstance(node, ast.ClassDef):
                for expression in (*node.decorator_list, *node.bases):
                    visit_expression(expression)
                for keyword in node.keywords:
                    visit_expression(keyword.value)
                return {"fallthrough"}
            if isinstance(node, ast.Return):
                visit_expression(node.value)
                return {"return"}
            if isinstance(node, ast.Raise):
                visit_expression(node.exc)
                visit_expression(node.cause)
                return {"raise"}
            if isinstance(node, ast.Break):
                return {"break"}
            if isinstance(node, ast.Continue):
                return {"continue"}
            if isinstance(node, ast.If):
                visit_expression(node.test)
                truth_value = constant_truth_value(node.test)
                if truth_value is True:
                    return visit_statements(node.body)
                if truth_value is False:
                    return visit_statements(node.orelse)
                return visit_statements(node.body) | visit_statements(node.orelse)
            if isinstance(node, ast.While):
                visit_expression(node.test)
                truth_value = constant_truth_value(node.test)
                if truth_value is False:
                    return visit_statements(node.orelse)
                body_exits = visit_statements(node.body)
                exits = body_exits & {"return", "raise"}
                if "break" in body_exits:
                    exits.add("fallthrough")
                if truth_value is True:
                    return exits
                return exits | visit_statements(node.orelse)
            if isinstance(node, (ast.For, ast.AsyncFor)):
                visit_expression(node.target)
                visit_expression(node.iter)
                iterable = constant_value(node.iter)
                if iterable is not cls._UNKNOWN:
                    try:
                        if len(iterable) == 0:
                            return visit_statements(node.orelse)
                    except TypeError:
                        pass
                body_exits = visit_statements(node.body)
                exits = body_exits & {"return", "raise"}
                if "break" in body_exits:
                    exits.add("fallthrough")
                normal_completion = bool(body_exits & {"fallthrough", "continue"})
                if iterable is cls._UNKNOWN or normal_completion:
                    exits |= visit_statements(node.orelse)
                return exits
            if isinstance(node, ast.Try):
                body_exits = visit_statements(node.body)
                protected_exits = body_exits - {"fallthrough"}
                if statements_may_raise(node.body):
                    for handler in node.handlers:
                        add_node(handler)
                        visit_expression(handler.type)
                        protected_exits |= visit_statements(handler.body)
                if "fallthrough" in body_exits:
                    if node.orelse:
                        protected_exits |= visit_statements(node.orelse)
                    else:
                        protected_exits.add("fallthrough")
                if not node.finalbody:
                    return protected_exits
                final_exits = visit_statements(node.finalbody)
                exits = final_exits - {"fallthrough"}
                if "fallthrough" in final_exits:
                    exits |= protected_exits
                return exits
            if isinstance(node, ast.Match):
                visit_expression(node.subject)
                exits = set()
                exhaustive = False
                for case in node.cases:
                    if exhaustive:
                        break
                    add_node(case)
                    visit_pattern(case.pattern)
                    guard_truth = True
                    if case.guard is not None:
                        visit_expression(case.guard)
                        guard_truth = constant_truth_value(case.guard)
                    if guard_truth is False:
                        continue
                    exits |= visit_statements(case.body)
                    exhaustive = is_irrefutable_pattern(case.pattern) and guard_truth is True
                if not exhaustive:
                    exits.add("fallthrough")
                return exits
            if isinstance(node, (ast.With, ast.AsyncWith)):
                for item in node.items:
                    visit_expression(item.context_expr)
                    visit_expression(item.optional_vars)
                return visit_statements(node.body)
            if isinstance(node, ast.Assert):
                if not __debug__:
                    return {"fallthrough"}
                visit_expression(node.test)
                truth_value = constant_truth_value(node.test)
                if truth_value is not True:
                    visit_expression(node.msg)
                return {"raise"} if truth_value is False else {"fallthrough", "raise"}
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.stmt):
                    visit_statement(child)
                else:
                    visit_expression(child)
            return {"fallthrough"}

        visit_statements(statements)
        return nodes

    @classmethod
    def _executable_body_nodes(cls, function_node):
        return cls._executable_nodes_in_statements(
            function_node.body,
            shadowed_names=cls._function_local_bindings(function_node),
            nonraising_names=cls._function_argument_bindings(function_node),
        )

    @classmethod
    def _is_direct_self_or_super_receiver(cls, function_node, node):
        return (
            isinstance(node, ast.Name)
            and node.id == "self"
            and "self" not in cls._function_rebound_names(function_node)
        ) or (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "super"
            and "super" not in cls._function_local_bindings(function_node)
            and not node.args
            and not node.keywords
        )

    @staticmethod
    def _annotation_names_ws_assistant(annotation):
        return annotation is not None and "WSAssistant" in ast.unparse(annotation)

    @classmethod
    def _permitted_websocket_receivers(cls, function_node):
        receivers = {
            argument.arg
            for argument in [*function_node.args.posonlyargs, *function_node.args.args, *function_node.args.kwonlyargs]
            if cls._annotation_names_ws_assistant(argument.annotation)
        }

        class ReceiverVisitor(ast.NodeVisitor):
            def visit_AsyncFunctionDef(self, node):
                return None

            def visit_FunctionDef(self, node):
                return None

            def visit_Lambda(self, node):
                return None

            def visit_ClassDef(self, node):
                return None

            def visit_AnnAssign(self, node):
                if isinstance(node.target, ast.Name) and cls._annotation_names_ws_assistant(node.annotation):
                    receivers.add(node.target.id)
                self.generic_visit(node)

            def visit_Assign(self, node):
                value = node.value.value if isinstance(node.value, ast.Await) else node.value
                if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute):
                    if value.func.attr in {"get_ws_assistant", "_connected_websocket_assistant"}:
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                receivers.add(target.id)
                self.generic_visit(node)

        visitor = ReceiverVisitor()
        for statement in function_node.body:
            visitor.visit(statement)
        return receivers

    @staticmethod
    def _function_argument_bindings(function_node):
        bindings = {
            argument.arg
            for argument in [
                *function_node.args.posonlyargs,
                *function_node.args.args,
                *function_node.args.kwonlyargs,
            ]
        }
        if function_node.args.vararg is not None:
            bindings.add(function_node.args.vararg.arg)
        if function_node.args.kwarg is not None:
            bindings.add(function_node.args.kwarg.arg)
        return bindings

    @classmethod
    def _function_local_bindings(cls, function_node):
        bindings = cls._function_argument_bindings(function_node)

        class BindingVisitor(ast.NodeVisitor):
            def visit_Name(self, node):
                if isinstance(node.ctx, (ast.Store, ast.Del)):
                    bindings.add(node.id)

            def visit_comprehension(self, node):
                self.visit(node.iter)
                for condition in node.ifs:
                    self.visit(condition)

            def visit_AsyncFunctionDef(self, node):
                bindings.add(node.name)

            def visit_FunctionDef(self, node):
                bindings.add(node.name)

            def visit_Lambda(self, node):
                return None

            def visit_ClassDef(self, node):
                bindings.add(node.name)

            def visit_Import(self, node):
                bindings.update(alias.asname or alias.name.split(".")[0] for alias in node.names)

            def visit_ImportFrom(self, node):
                bindings.update(alias.asname or alias.name for alias in node.names)

            def visit_ExceptHandler(self, node):
                if node.name is not None:
                    bindings.add(node.name)
                self.generic_visit(node)

            def visit_Global(self, node):
                bindings.update(node.names)

            def visit_Nonlocal(self, node):
                bindings.update(node.names)

        visitor = BindingVisitor()
        for statement in function_node.body:
            visitor.visit(statement)
        return bindings

    @classmethod
    def _function_rebound_names(cls, function_node):
        bindings = set()

        class BindingVisitor(ast.NodeVisitor):
            def visit_Name(self, node):
                if isinstance(node.ctx, (ast.Store, ast.Del)):
                    bindings.add(node.id)

            def visit_comprehension(self, node):
                self.visit(node.iter)
                for condition in node.ifs:
                    self.visit(condition)

            def visit_AsyncFunctionDef(self, node):
                bindings.add(node.name)

            def visit_FunctionDef(self, node):
                bindings.add(node.name)

            def visit_Lambda(self, node):
                return None

            def visit_ClassDef(self, node):
                bindings.add(node.name)

            def visit_Import(self, node):
                bindings.update(alias.asname or alias.name.split(".")[0] for alias in node.names)

            def visit_ImportFrom(self, node):
                bindings.update(alias.asname or alias.name for alias in node.names)

            def visit_ExceptHandler(self, node):
                if node.name is not None:
                    bindings.add(node.name)
                self.generic_visit(node)

            def visit_Global(self, node):
                bindings.update(node.names)

            def visit_Nonlocal(self, node):
                bindings.update(node.names)

        visitor = BindingVisitor()
        for statement in function_node.body:
            visitor.visit(statement)
        return bindings

    @classmethod
    def _is_valid_tasks_future(cls, function_node, await_node):
        for node in cls._executable_body_nodes(function_node):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.lineno >= await_node.lineno:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if not any(isinstance(target, ast.Name) and target.id == "tasks_future" for target in targets):
                continue
            value = node.value
            if (
                isinstance(value, ast.Call)
                and isinstance(value.func, ast.Attribute)
                and isinstance(value.func.value, ast.Name)
                and value.func.value.id == "asyncio"
                and value.func.attr == "gather"
            ):
                return True
        return False

    @staticmethod
    def _awaited_callable(node):
        if not isinstance(node, ast.Await):
            return None
        awaited = node.value
        return awaited.func if isinstance(awaited, ast.Call) else awaited

    @classmethod
    def _is_disconnect_boundary_await(cls, function_node, node):
        return cls._is_lifecycle_boundary_await(
            function_node,
            node,
            {"_on_order_stream_interruption", "disconnect", "await_primary_then_cleanup"},
        )

    @classmethod
    def _awaited_name(cls, node):
        function = cls._awaited_callable(node)
        return function.attr if isinstance(function, ast.Attribute) else (
            function.id if isinstance(function, ast.Name) else None
        )

    @classmethod
    def _is_lifecycle_boundary_await(cls, function_node, node, expected_names):
        function = cls._awaited_callable(node)
        function_name = cls._awaited_name(node)
        if function_name not in expected_names:
            return False
        if isinstance(function, ast.Name):
            if function_name == "tasks_future":
                return cls._is_valid_tasks_future(function_node, node)
            return (
                function_name in {"await_primary_then_cleanup", "safe_gather"}
                and function_name not in cls._function_local_bindings(function_node)
            )
        if not isinstance(function, ast.Attribute):
            return False
        if function_name == "disconnect":
            return (
                isinstance(function.value, ast.Name)
                and function.value.id in cls._permitted_websocket_receivers(function_node)
            )
        if function_name == "_on_order_stream_interruption":
            return cls._is_direct_self_or_super_receiver(function_node, function.value)
        if function_name == "start":
            return (
                isinstance(function.value, ast.Attribute)
                and function.value.attr == "_data_source"
                and cls._is_direct_self_or_super_receiver(function_node, function.value.value)
            )
        return cls._is_direct_self_or_super_receiver(function_node, function.value)

    @staticmethod
    def _expected_lifecycle_boundary_names(definition, function_node):
        alternate_names = ALTERNATE_LIFECYCLE_BOUNDARIES.get(definition)
        if alternate_names is not None:
            return alternate_names
        if function_node.name == "_on_order_stream_interruption":
            return {"_on_order_stream_interruption", "disconnect", "await_primary_then_cleanup"}
        if definition in EXPECTED_ORDER_STREAM_DEFINITIONS:
            return {"_on_order_stream_interruption"}
        return {"_on_order_stream_interruption", "disconnect", "await_primary_then_cleanup"}

    @classmethod
    def _is_owned_task_cleanup(cls, node):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            attribute = cls._assigned_self_attribute(node)
            target = node.targets[0] if isinstance(node, ast.Assign) and len(node.targets) == 1 else (
                node.target if isinstance(node, ast.AnnAssign) else None
            )
            local_task_clear = (
                isinstance(target, ast.Name)
                and ("task" in target.id or target.id.endswith("_future"))
                and isinstance(node.value, ast.Constant)
                and node.value.value is None
            )
            return local_task_clear or (
                attribute is not None
                and attribute.endswith("_task")
                and isinstance(node.value, ast.Constant)
                and node.value.value is None
            )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "cancel":
            task = node.func.value
            return (
                (
                    isinstance(task, ast.Name)
                    and ("task" in task.id or task.id.endswith("_future"))
                )
                or (
                    isinstance(task, ast.Attribute)
                    and task.attr.endswith(("_task", "_future"))
                )
            )
        if isinstance(node, ast.Call):
            function = node.func
            function_name = function.attr if isinstance(function, ast.Attribute) else (
                function.id if isinstance(function, ast.Name) else ""
            )
            return function_name.startswith("_cleanup") and "task" in function_name
        return False

    @classmethod
    def _cleanup_is_in_finally_for_await(cls, function_node, cleanup_node, await_node):
        if any(nested is cleanup_node for nested in cls._executable_nodes_in_statements([await_node])):
            return True
        for try_node in (node for node in cls._executable_body_nodes(function_node) if isinstance(node, ast.Try)):
            protected_statements = [
                *try_node.body,
                *(statement for handler in try_node.handlers for statement in handler.body),
                *try_node.orelse,
            ]
            await_is_in_body = any(
                nested is await_node for nested in cls._executable_nodes_in_statements(protected_statements)
            )
            cleanup_is_in_finally = any(
                nested is cleanup_node for nested in cls._executable_nodes_in_statements(try_node.finalbody)
            )
            if await_is_in_body and cleanup_is_in_finally:
                return True
        return False

    def _audit_definition(self, definition, function_node):
        executable_nodes = self._executable_body_nodes(function_node)
        for node in executable_nodes:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                assigned_attribute = self._assigned_self_attribute(node)
                clears_assistant = (
                    assigned_attribute == "_ws_assistant"
                    and isinstance(node.value, ast.Constant)
                    and node.value.value is None
                )
                self.assertFalse(
                    function_node.name == "_on_order_stream_interruption" and clears_assistant,
                    f"{definition} must leave exact assistant identity cleanup to its owning listener",
                )

        expected_boundary_names = self._expected_lifecycle_boundary_names(definition, function_node)
        lifecycle_awaits = [node for node in executable_nodes
                            if self._is_lifecycle_boundary_await(function_node, node, expected_boundary_names)]
        self.assertTrue(
            lifecycle_awaits,
            f"{definition} must contain a recognized awaited lifecycle boundary",
        )

        disconnect_awaits = [
            node for node in executable_nodes if self._is_disconnect_boundary_await(function_node, node)
        ]
        task_cleanups = [node for node in executable_nodes if self._is_owned_task_cleanup(node)]
        for disconnect_await in disconnect_awaits:
            for task_cleanup in task_cleanups:
                if task_cleanup.lineno <= disconnect_await.lineno:
                    continue
                self.assertTrue(
                    self._cleanup_is_in_finally_for_await(function_node, task_cleanup, disconnect_await),
                    f"{definition} must protect post-disconnect task cleanup with finally",
                )

    def test_all_order_stream_definitions_obey_interruption_ownership_boundaries(self):
        definitions = self._production_definitions()
        self.assertEqual(EXPECTED_ORDER_STREAM_DEFINITIONS, set(definitions))
        self.assertEqual(25, len(definitions))

        audited_definitions = set()
        for definition, function_node in definitions.items():
            audited_definitions.add(definition)
            self._audit_definition(definition, function_node)
        self.assertEqual(EXPECTED_ORDER_STREAM_DEFINITIONS, audited_definitions)

    def test_static_audit_rejects_listener_without_awaited_lifecycle_boundary(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    self._listen_task.cancel()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_deletion_of_expected_hook_when_other_awaits_remain(self):
        definition = (
            "hummingbot/core/data_type/order_book_tracker_data_source.py:"
            "OrderBookTrackerDataSource.listen_for_subscriptions"
        )
        function_node = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    websocket = await self._connected_websocket_assistant()\n"
            "    await self._sleep(1)\n"
            "    self._listen_task.cancel()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition(definition, function_node)

    def test_static_audit_rejects_unsafe_exact_local_task_cleanup(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, websocket: WSAssistant):\n"
            "    task = self._listen_task\n"
            "    await websocket.disconnect()\n"
            "    task.cancel()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "protect post-disconnect task cleanup"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_constant_unreachable_lifecycle_boundary(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    if False:\n"
            "        await self._on_order_stream_interruption()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_lifecycle_boundary_in_uncalled_nested_coroutine(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    async def cleanup():\n"
            "        await self._on_order_stream_interruption()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_lifecycle_boundary_on_unrelated_receiver(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, unrelated):\n"
            "    await unrelated._on_order_stream_interruption()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_bare_lifecycle_boundary_name(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    await _on_order_stream_interruption()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_disconnect_on_unrelated_receiver(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, unrelated):\n"
            "    await unrelated.disconnect()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_chained_self_receiver(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    await self.unrelated._on_order_stream_interruption()\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_shadowed_helper_parameter(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, await_primary_then_cleanup):\n"
            "    await await_primary_then_cleanup(None, None, None, 'cleanup')\n"
        ).body[0]

        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_builtin_folding_uses_bindings_from_the_entire_function_scope(self):
        range_function = ast.parse(
            "async def listen_for_subscriptions(self, range):\n"
            "    for _ in range(1):\n"
            "        await self._on_order_stream_interruption()\n"
        ).body[0]
        set_function = ast.parse(
            "async def listen_for_subscriptions(self):\n"
            "    for _ in set():\n"
            "        await self._on_order_stream_interruption()\n"
            "    set = lambda: {1}\n"
        ).body[0]

        range_call = range_function.body[0].iter
        set_call = set_function.body[0].iter
        self.assertIs(
            self._UNKNOWN,
            self._constant_value(range_call, self._function_local_bindings(range_function)),
        )
        self.assertIs(
            self._UNKNOWN,
            self._constant_value(set_call, self._function_local_bindings(set_function)),
        )

    def test_static_audit_rejects_shadowed_super_and_rebound_self_receivers(self):
        sources = (
            "async def listen_for_subscriptions(self, super):\n"
            "    await super()._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self, other):\n"
            "    await self._on_order_stream_interruption()\n"
            "    self = other\n",
            "async def listen_for_subscriptions(self):\n"
            "    await super()._on_order_stream_interruption()\n"
            "    def super():\n"
            "        return self\n",
        )

        for source in sources:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition(
                        "mutated:Listener.listen_for_subscriptions",
                        ast.parse(source).body[0],
                    )

    def test_static_audit_rejects_dominated_boolean_and_nonraising_parameter_handlers(self):
        cases = (
            (
                "async def listen_for_subscriptions(self, flag):\n"
                "    if flag and False:\n"
                "        await self._on_order_stream_interruption()\n",
                ((False,), (True,)),
            ),
            (
                "async def listen_for_subscriptions(self, flag):\n"
                "    if flag or True:\n"
                "        return\n"
                "    await self._on_order_stream_interruption()\n",
                ((False,), (True,)),
            ),
            (
                "async def listen_for_subscriptions(self, flag):\n"
                "    try:\n"
                "        flag\n"
                "    except Exception:\n"
                "        await self._on_order_stream_interruption()\n",
                ((False,), (True,)),
            ),
        )

        for source, argument_sets in cases:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition(
                        "mutated:Listener.listen_for_subscriptions",
                        ast.parse(source).body[0],
                    )
                hits = 0

                class Probe:
                    async def _on_order_stream_interruption(probe_self):
                        nonlocal hits
                        hits += 1

                namespace = {}
                exec(compile(source, "<binding-runtime-probe>", "exec"), namespace)
                for arguments in argument_sets:
                    asyncio.run(namespace["listen_for_subscriptions"](Probe(), *arguments))
                self.assertEqual(0, hits)

    def test_comprehension_target_binding_precedes_filters_and_result(self):
        rejected_sources = (
            "async def listen_for_subscriptions(self):\n"
            "    [await self._on_order_stream_interruption() for (a, b) in [1]]\n",
            "async def listen_for_subscriptions(self):\n"
            "    [await self._on_order_stream_interruption() for (a, b) in [1, (2, 3)]]\n",
        )
        accepted_sources = (
            "async def listen_for_subscriptions(self):\n"
            "    [await self._on_order_stream_interruption() for (a, b) in [(1, 2), 3]]\n",
            "async def listen_for_subscriptions(self, values):\n"
            "    [await self._on_order_stream_interruption() for (a, b) in values]\n",
        )

        for source in rejected_sources:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition(
                        "mutated:Listener.listen_for_subscriptions",
                        ast.parse(source).body[0],
                    )
        for source in accepted_sources:
            with self.subTest(source=source):
                self._audit_definition(
                    "mutated:Listener.listen_for_subscriptions",
                    ast.parse(source).body[0],
                )

        hits = 0

        class Probe:
            async def _on_order_stream_interruption(probe_self):
                nonlocal hits
                hits += 1

        namespace = {}
        exec(compile(rejected_sources[0], "<unpack-runtime-probe>", "exec"), namespace)
        with self.assertRaises(TypeError):
            asyncio.run(namespace["listen_for_subscriptions"](Probe()))
        self.assertEqual(0, hits)

        namespace = {}
        exec(compile(accepted_sources[0], "<sequential-unpack-runtime-probe>", "exec"), namespace)
        with self.assertRaises(TypeError):
            asyncio.run(namespace["listen_for_subscriptions"](Probe()))
        self.assertEqual(1, hits)

    def test_static_audit_rejects_false_literal_comparison_and_boolean_operands(self):
        sources = (
            "async def listen_for_subscriptions(self):\n"
            "    if 1 == 2:\n"
            "        await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self):\n"
            "    False and await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self):\n"
            "    True or await self._on_order_stream_interruption()\n",
        )

        for source in sources:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition(
                        "mutated:Listener.listen_for_subscriptions",
                        ast.parse(source).body[0],
                    )

    def test_static_audit_rejects_boundary_inside_constant_empty_iterables(self):
        for iterable in ("()", "[]", "{}", "set()", "range(0)"):
            with self.subTest(iterable=iterable):
                function_node = ast.parse(
                    "async def listen_for_subscriptions(self):\n"
                    f"    for _ in {iterable}:\n"
                    "        await self._on_order_stream_interruption()\n"
                ).body[0]

                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_boundary_after_unconditional_terminator(self):
        for terminator in ("return", "raise RuntimeError('stop')"):
            with self.subTest(terminator=terminator):
                function_node = ast.parse(
                    "async def listen_for_subscriptions(self):\n"
                    f"    {terminator}\n"
                    "    await self._on_order_stream_interruption()\n"
                ).body[0]

                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_unreachable_while_body_and_else(self):
        sources = (
            "async def listen_for_subscriptions(self):\n"
            "    while False:\n"
            "        await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self):\n"
            "    while True:\n"
            "        return\n"
            "    else:\n"
            "        await self._on_order_stream_interruption()\n",
        )

        for source in sources:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition(
                        "mutated:Listener.listen_for_subscriptions",
                        ast.parse(source).body[0],
                    )

    def test_static_audit_includes_reachable_try_except_else_and_finally_paths(self):
        sources = (
            "async def listen_for_subscriptions(self):\n"
            "    try:\n"
            "        await self._sleep(1)\n"
            "    except Exception:\n"
            "        await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self):\n"
            "    try:\n"
            "        await self._sleep(1)\n"
            "    except Exception:\n"
            "        raise\n"
            "    else:\n"
            "        await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self):\n"
            "    try:\n"
            "        await self._sleep(1)\n"
            "    finally:\n"
            "        await self._on_order_stream_interruption()\n",
        )

        for source in sources:
            with self.subTest(source=source):
                self._audit_definition(
                    "mutated:Listener.listen_for_subscriptions",
                    ast.parse(source).body[0],
                )

    def test_static_audit_includes_boundaries_in_exhaustive_match_cases(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case 1:\n"
            "            await self._on_order_stream_interruption()\n"
            "        case _:\n"
            "            await self._on_order_stream_interruption()\n"
        ).body[0]

        match_node = function_node.body[0]
        executable_nodes = self._executable_body_nodes(function_node)
        case_awaits = [case.body[0].value for case in match_node.cases]

        self.assertTrue(all(await_node in executable_nodes for await_node in case_awaits))
        self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_visits_later_match_case_after_earlier_case_terminates(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case 1:\n"
            "            return\n"
            "        case _:\n"
            "            await self._on_order_stream_interruption()\n"
        ).body[0]

        self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_includes_match_subject_and_guard_expressions(self):
        sources = (
            "async def listen_for_subscriptions(self):\n"
            "    match await self._on_order_stream_interruption():\n"
            "        case _:\n"
            "            return\n",
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case _ if await self._on_order_stream_interruption():\n"
            "            return\n",
        )

        for source in sources:
            with self.subTest(source=source):
                self._audit_definition(
                    "mutated:Listener.listen_for_subscriptions",
                    ast.parse(source).body[0],
                )

    def test_static_audit_keeps_following_boundary_after_nonexhaustive_match(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case 1:\n"
            "            return\n"
            "    await self._on_order_stream_interruption()\n"
        ).body[0]

        self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_keeps_following_boundary_when_an_exhaustive_case_falls_through(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case 1:\n"
            "            return\n"
            "        case _:\n"
            "            self._listen_task.cancel()\n"
            "    await self._on_order_stream_interruption()\n"
        ).body[0]

        self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_rejects_boundary_after_exhaustive_terminating_match(self):
        sources = [
            (
                pattern,
                ast.parse(
                    "async def listen_for_subscriptions(self, value):\n"
                    "    match value:\n"
                    f"        case {pattern}:\n"
                    "            return\n"
                    "    await self._on_order_stream_interruption()\n"
                ).body[0],
            )
            for pattern in ("_", "captured")
        ]
        sources.append(
            (
                "multiple terminating cases",
                ast.parse(
                    "async def listen_for_subscriptions(self, value):\n"
                    "    match value:\n"
                    "        case 1:\n"
                    "            return\n"
                    "        case _:\n"
                    "            raise RuntimeError('stop')\n"
                    "    await self._on_order_stream_interruption()\n"
                ).body[0],
            )
        )

        for pattern, function_node in sources:
            with self.subTest(pattern=pattern):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_static_audit_does_not_treat_refutable_or_guarded_patterns_as_exhaustive(self):
        sources = (
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case 1 | 2:\n"
            "            return\n"
            "    await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case captured if captured:\n"
            "            return\n"
            "    await self._on_order_stream_interruption()\n",
        )

        for source in sources:
            with self.subTest(source=source):
                self._audit_definition(
                    "mutated:Listener.listen_for_subscriptions",
                    ast.parse(source).body[0],
                )

    def test_static_audit_rejects_match_boundaries_in_false_guards_and_nested_scopes(self):
        sources = (
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case _ if False:\n"
            "            await self._on_order_stream_interruption()\n",
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case captured:\n"
            "            async def nested():\n"
            "                await self._on_order_stream_interruption()\n",
        )

        for source in sources:
            with self.subTest(source=source):
                with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                    self._audit_definition(
                        "mutated:Listener.listen_for_subscriptions",
                        ast.parse(source).body[0],
                    )

    def test_static_audit_skips_unreachable_match_patterns_guards_and_bodies(self):
        function_node = ast.parse(
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case _:\n"
            "            return\n"
            "        case namespace.VALUE if await self._on_order_stream_interruption():\n"
            "            await self._on_order_stream_interruption()\n"
        ).body[0]
        match_node = function_node.body[0]
        unreachable_case = match_node.cases[1]
        executable_nodes = self._executable_body_nodes(function_node)

        self.assertNotIn(unreachable_case.pattern, executable_nodes)
        self.assertNotIn(unreachable_case.guard, executable_nodes)
        self.assertNotIn(unreachable_case.body[0], executable_nodes)
        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

    def test_constant_evaluator_matches_unary_comparison_boolean_and_debug_semantics(self):
        cases = {
            "not True": False,
            "+2": 2,
            "-2": -2,
            "~0": -1,
            "1 < 2 == 2": True,
            "0 or 3": 3,
            "2 and 5": 5,
            "__debug__": __debug__,
        }

        for expression, expected in cases.items():
            with self.subTest(expression=expression):
                node = ast.parse(expression, mode="eval").body
                self.assertEqual(expected, self._constant_value(node))

    def test_static_audit_matches_runtime_for_reviewer_adversarial_probes(self):
        def decorator(function):
            return function
        cases = (
            (
                "unary short circuit",
                "async def listen_for_subscriptions(self):\n"
                "    not True and await self._on_order_stream_interruption()\n",
                (),
                True,
                0,
            ),
            (
                "unary exhaustive guard skips later case",
                "async def listen_for_subscriptions(self, value):\n"
                "    match value:\n"
                "        case _ if not False:\n"
                "            return\n"
                "        case _:\n"
                "            await self._on_order_stream_interruption()\n",
                (None,),
                True,
                0,
            ),
            (
                "unary exhaustive guard skips following statement",
                "async def listen_for_subscriptions(self, value):\n"
                "    match value:\n"
                "        case _ if not False:\n"
                "            return\n"
                "    await self._on_order_stream_interruption()\n",
                (None,),
                True,
                0,
            ),
            (
                "constant unary boolean guard",
                "async def listen_for_subscriptions(self):\n"
                "    if +1 < 0 or ~0 != -1:\n"
                "        await self._on_order_stream_interruption()\n",
                (),
                True,
                0,
            ),
            (
                "pass try cannot reach handler",
                "async def listen_for_subscriptions(self):\n"
                "    try:\n"
                "        pass\n"
                "    except Exception:\n"
                "        await self._on_order_stream_interruption()\n",
                (),
                True,
                0,
            ),
            (
                "constant try cannot reach handler",
                "async def listen_for_subscriptions(self):\n"
                "    try:\n"
                "        1\n"
                "    except Exception:\n"
                "        await self._on_order_stream_interruption()\n",
                (),
                True,
                0,
            ),
            (
                "constant assignment try cannot reach handler",
                "async def listen_for_subscriptions(self):\n"
                "    try:\n"
                "        value = 1\n"
                "    except Exception:\n"
                "        await self._on_order_stream_interruption()\n",
                (),
                True,
                0,
            ),
            (
                "return try cannot reach handler",
                "async def listen_for_subscriptions(self):\n"
                "    try:\n"
                "        return\n"
                "    except Exception:\n"
                "        await self._on_order_stream_interruption()\n",
                (),
                True,
                0,
            ),
            (
                "generator body is lazy",
                "async def listen_for_subscriptions(self):\n"
                "    (await self._on_order_stream_interruption() for _ in (1,))\n",
                (),
                True,
                0,
            ),
            (
                "generator filter is lazy",
                "async def listen_for_subscriptions(self):\n"
                "    (_ for _ in (1,) if await self._on_order_stream_interruption())\n",
                (),
                True,
                0,
            ),
            (
                "empty eager comprehension skips body",
                "async def listen_for_subscriptions(self):\n"
                "    [await self._on_order_stream_interruption() for _ in ()]\n",
                (),
                True,
                0,
            ),
            (
                "nested async definition default is eager",
                "async def listen_for_subscriptions(self):\n"
                "    async def nested(value=await self._on_order_stream_interruption()):\n"
                "        return value\n",
                (),
                True,
                1,
            ),
            (
                "lambda default is eager",
                "async def listen_for_subscriptions(self):\n"
                "    lambda value=await self._on_order_stream_interruption(): value\n",
                (),
                True,
                1,
            ),
            (
                "nested definition decorator is eager",
                "async def listen_for_subscriptions(self):\n"
                "    @await self._on_order_stream_interruption()\n"
                "    async def nested():\n"
                "        return None\n",
                (),
                decorator,
                1,
            ),
            (
                "builtin object class pattern is exhaustive",
                "async def listen_for_subscriptions(self, value):\n"
                "    match value:\n"
                "        case object():\n"
                "            return\n"
                "        case _:\n"
                "            await self._on_order_stream_interruption()\n",
                (None,),
                True,
                0,
            ),
            (
                "shadowed object class pattern stays refutable",
                "async def listen_for_subscriptions(self, value, object):\n"
                "    match value:\n"
                "        case object():\n"
                "            return\n"
                "        case _:\n"
                "            await self._on_order_stream_interruption()\n",
                (None, str),
                True,
                1,
            ),
            (
                "assert follows optimization mode",
                "async def listen_for_subscriptions(self):\n"
                "    assert await self._on_order_stream_interruption()\n",
                (),
                True,
                1 if __debug__ else 0,
            ),
            (
                "debug branch follows optimization mode",
                "async def listen_for_subscriptions(self):\n"
                "    if __debug__:\n"
                "        await self._on_order_stream_interruption()\n",
                (),
                True,
                1 if __debug__ else 0,
            ),
        )

        for name, source, arguments, boundary_result, expected_hits in cases:
            with self.subTest(name=name):
                function_node = ast.parse(source).body[0]
                if expected_hits:
                    self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)
                else:
                    with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
                        self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

                hits = 0

                class Probe:
                    async def _on_order_stream_interruption(probe_self):
                        nonlocal hits
                        hits += 1
                        return boundary_result

                namespace = {}
                exec(compile(source, "<audit-runtime-probe>", "exec"), namespace)
                asyncio.run(namespace["listen_for_subscriptions"](Probe(), *arguments))
                self.assertEqual(expected_hits, hits)

    def test_generator_outer_iterable_expression_is_eager(self):
        source = (
            "async def listen_for_subscriptions(self):\n"
            "    (_ for _ in await self._on_order_stream_interruption())\n"
        )
        function_node = ast.parse(source).body[0]
        self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

        hits = 0

        class Probe:
            async def _on_order_stream_interruption(probe_self):
                nonlocal hits
                hits += 1
                return ()

        namespace = {}
        exec(compile(source, "<generator-outer-runtime-probe>", "exec"), namespace)
        asyncio.run(namespace["listen_for_subscriptions"](Probe()))
        self.assertEqual(1, hits)

    def test_builtin_object_pattern_is_runtime_exhaustive_for_reviewer_subjects(self):
        source = (
            "async def listen_for_subscriptions(self, value):\n"
            "    match value:\n"
            "        case object():\n"
            "            return\n"
            "        case _:\n"
            "            await self._on_order_stream_interruption()\n"
        )
        function_node = ast.parse(source).body[0]
        with self.assertRaisesRegex(AssertionError, "awaited lifecycle boundary"):
            self._audit_definition("mutated:Listener.listen_for_subscriptions", function_node)

        hits = 0

        class Probe:
            async def _on_order_stream_interruption(probe_self):
                nonlocal hits
                hits += 1

        namespace = {}
        exec(compile(source, "<builtin-object-runtime-probe>", "exec"), namespace)

        async def run_subjects():
            for subject in (None, 1, "value", [], object()):
                await namespace["listen_for_subscriptions"](Probe(), subject)

        asyncio.run(run_subjects())
        self.assertEqual(0, hits)


class _ControllableConnection:
    def __init__(
        self,
        disconnect_release: Optional[asyncio.Event] = None,
        disconnect_error: Optional[Exception] = None,
    ):
        self.connected = True
        self.disconnect_calls = 0
        self.disconnect_started = asyncio.Event()
        self.disconnect_release = disconnect_release
        self.disconnect_error = disconnect_error
        self.incoming_messages = asyncio.Queue()

    async def disconnect(self):
        self.disconnect_calls += 1
        self.disconnect_started.set()
        if self.disconnect_release is not None:
            await self.disconnect_release.wait()
        if self.disconnect_error is not None:
            raise self.disconnect_error
        self.connected = False

    async def receive(self):
        message = await self.incoming_messages.get()
        if message is _END_OF_STREAM:
            self.connected = False
            return None
        return WSResponse(message)


class ListenerTestOrderBookTrackerDataSource(OrderBookTrackerDataSource):
    """Concrete base data source with deterministic WebSocket attempts."""

    def __init__(self):
        super().__init__(["BTC-USDT"])
        self.assistants = asyncio.Queue()
        self.connected_assistants = []
        self.subscribed_assistants = asyncio.Queue()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return {pair: 100.0 for pair in trading_pairs}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        assistant = await self.assistants.get()
        self.connected_assistants.append(assistant)
        return assistant

    async def _subscribe_channels(self, ws: WSAssistant):
        self.subscribed_assistants.put_nowait(ws)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return event_message["channel"]

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True


class MockOrderBookTrackerDataSource(OrderBookTrackerDataSource):
    """Concrete implementation of OrderBookTrackerDataSource for testing."""

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return {pair: 100.0 for pair in trading_pairs}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError

    async def _subscribe_channels(self, ws: WSAssistant):
        raise NotImplementedError

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        return ""

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True


class OrderBookTrackerDataSourceTests(unittest.TestCase):
    """Tests for the OrderBookTrackerDataSource base class methods."""

    def setUp(self):
        self.trading_pairs = ["BTC-USDT", "ETH-USDT"]
        self.data_source = MockOrderBookTrackerDataSource(trading_pairs=self.trading_pairs.copy())

    def test_initial_trading_pairs(self):
        """Test that trading pairs are correctly initialized."""
        self.assertEqual(self.trading_pairs, self.data_source._trading_pairs)

    def test_add_trading_pair_new_pair(self):
        """Test adding a new trading pair."""
        new_pair = "SOL-USDT"

        self.assertNotIn(new_pair, self.data_source._trading_pairs)

        self.data_source.add_trading_pair(new_pair)

        self.assertIn(new_pair, self.data_source._trading_pairs)
        self.assertEqual(3, len(self.data_source._trading_pairs))

    def test_add_trading_pair_existing_pair(self):
        """Test that adding an existing pair doesn't create duplicates."""
        existing_pair = "BTC-USDT"

        self.assertIn(existing_pair, self.data_source._trading_pairs)
        initial_count = len(self.data_source._trading_pairs)

        self.data_source.add_trading_pair(existing_pair)

        # Should not create duplicate
        self.assertEqual(initial_count, len(self.data_source._trading_pairs))

    def test_remove_trading_pair_existing_pair(self):
        """Test removing an existing trading pair."""
        pair_to_remove = "BTC-USDT"

        self.assertIn(pair_to_remove, self.data_source._trading_pairs)

        self.data_source.remove_trading_pair(pair_to_remove)

        self.assertNotIn(pair_to_remove, self.data_source._trading_pairs)
        self.assertEqual(1, len(self.data_source._trading_pairs))

    def test_remove_trading_pair_nonexistent_pair(self):
        """Test that removing a nonexistent pair doesn't raise an error."""
        nonexistent_pair = "NONEXISTENT-PAIR"

        self.assertNotIn(nonexistent_pair, self.data_source._trading_pairs)
        initial_count = len(self.data_source._trading_pairs)

        # Should not raise
        self.data_source.remove_trading_pair(nonexistent_pair)

        # Count should remain the same
        self.assertEqual(initial_count, len(self.data_source._trading_pairs))

    def test_add_and_remove_trading_pair_sequence(self):
        """Test adding and removing trading pairs in sequence."""
        new_pair = "SOL-USDT"

        # Add new pair
        self.data_source.add_trading_pair(new_pair)
        self.assertIn(new_pair, self.data_source._trading_pairs)

        # Remove it
        self.data_source.remove_trading_pair(new_pair)
        self.assertNotIn(new_pair, self.data_source._trading_pairs)

        # Remove an original pair
        self.data_source.remove_trading_pair("BTC-USDT")
        self.assertNotIn("BTC-USDT", self.data_source._trading_pairs)

        # Only ETH-USDT should remain
        self.assertEqual(["ETH-USDT"], self.data_source._trading_pairs)

    def test_ws_assistant_initialization(self):
        """Test that ws_assistant is initially None."""
        self.assertIsNone(self.data_source._ws_assistant)

    def test_order_book_create_function(self):
        """Test that order_book_create_function returns OrderBook by default."""
        order_book = self.data_source.order_book_create_function()
        self.assertIsInstance(order_book, OrderBook)

    def test_order_book_create_function_setter(self):
        """Test setting a custom order_book_create_function."""
        class CustomOrderBook(OrderBook):
            pass

        self.data_source.order_book_create_function = lambda: CustomOrderBook()

        order_book = self.data_source.order_book_create_function()
        self.assertIsInstance(order_book, CustomOrderBook)


class OrderBookTrackerDataSourceListenerTests(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.data_source = ListenerTestOrderBookTrackerDataSource()

    async def _start_listener(self, assistant: WSAssistant) -> asyncio.Task:
        self.data_source.assistants.put_nowait(assistant)
        listener_task = asyncio.create_task(self.data_source.listen_for_subscriptions())
        subscribed_assistant = await asyncio.wait_for(
            self.data_source.subscribed_assistants.get(),
            timeout=0.2,
        )
        self.assertIs(assistant, subscribed_assistant)
        self.assertIs(assistant, self.data_source._ws_assistant)
        return listener_task

    async def _cancel_listener(self, listener_task: asyncio.Task):
        if not listener_task.done():
            listener_task.cancel("test cleanup")
        await asyncio.gather(listener_task, return_exceptions=True)

    def _assert_task_boundary_cancellation(self, cancellation, expected_args):
        self.assertIs(type(cancellation), asyncio.CancelledError)
        self.assertEqual(expected_args if sys.version_info >= (3, 11) else (), cancellation.args)

    async def test_listener_clears_exact_assistant_after_disconnect_returns(self):
        connection = _ControllableConnection()
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)

        listener_task.cancel("listener cancelled")
        with self.assertRaises(asyncio.CancelledError) as cancellation_context:
            await listener_task

        self._assert_task_boundary_cancellation(cancellation_context.exception, ("listener cancelled",))
        self.assertEqual(1, connection.disconnect_calls)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_listener_logs_disconnect_error_clears_exact_assistant_and_retries(self):
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)

        with self.assertLogs(self.data_source.logger(), level="ERROR") as log_context:
            connection.incoming_messages.put_nowait(_END_OF_STREAM)
            for _ in range(20):
                if connection.disconnect_calls == 1 and self.data_source._ws_assistant is None:
                    break
                await asyncio.sleep(0)

        self.assertEqual(1, connection.disconnect_calls)
        self.assertFalse(listener_task.done())
        self.assertTrue(any("disconnect failed" in record for record in log_context.output))
        self.assertIsNone(self.data_source._ws_assistant)
        await self._cancel_listener(listener_task)

    async def test_tracker_data_source_loop_keeps_running_after_logged_disconnect_error(self):
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        self.data_source.assistants.put_nowait(assistant)
        tracker = OrderBookTracker(self.data_source, ["BTC-USDT"])
        listener_task = tracker._schedule_data_source_task(0, "listen_for_subscriptions")
        subscribed_assistant = await asyncio.wait_for(
            self.data_source.subscribed_assistants.get(),
            timeout=0.2,
        )
        self.assertIs(assistant, subscribed_assistant)
        with self.assertLogs(self.data_source.logger(), level="ERROR") as log_context:
            connection.incoming_messages.put_nowait(_END_OF_STREAM)
            for _ in range(20):
                if connection.disconnect_calls == 1 and self.data_source._ws_assistant is None:
                    break
                await asyncio.sleep(0)

        self.assertFalse(listener_task.done())
        self.assertTrue(any("disconnect failed" in record for record in log_context.output))
        self.assertIsNone(self.data_source._ws_assistant)
        listener_task.cancel("tracker cleanup")
        await asyncio.gather(listener_task, return_exceptions=True)

    async def test_listener_keeps_exact_assistant_until_blocked_disconnect_returns(self):
        disconnect_release = asyncio.Event()
        connection = _ControllableConnection(disconnect_release=disconnect_release)
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        await asyncio.wait_for(connection.disconnect_started.wait(), timeout=0.2)
        self.assertFalse(listener_task.done())
        self.assertIs(assistant, self.data_source._ws_assistant)

        disconnect_release.set()
        for _ in range(10):
            if self.data_source._ws_assistant is None:
                break
            await asyncio.sleep(0)
        self.assertIsNone(self.data_source._ws_assistant)
        await self._cancel_listener(listener_task)

    async def test_listener_clears_exact_assistant_when_disconnect_is_cancelled(self):
        disconnect_release = asyncio.Event()
        connection = _ControllableConnection(disconnect_release=disconnect_release)
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        await asyncio.wait_for(connection.disconnect_started.wait(), timeout=0.2)
        self.assertIs(assistant, self.data_source._ws_assistant)
        listener_task.cancel("disconnect cancelled")
        with self.assertRaises(asyncio.CancelledError) as cancellation_context:
            await listener_task

        self._assert_task_boundary_cancellation(cancellation_context.exception, ("disconnect cancelled",))
        self.assertEqual(1, connection.disconnect_calls)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_listener_primary_cancellation_wins_over_disconnect_failure(self):
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        listener_task = await self._start_listener(assistant)

        with self.assertLogs(self.data_source.logger(), level="ERROR") as log_context:
            listener_task.cancel("primary cancellation")
            with self.assertRaises(asyncio.CancelledError) as cancellation_context:
                await listener_task

        self._assert_task_boundary_cancellation(cancellation_context.exception, ("primary cancellation",))
        self.assertTrue(listener_task.cancelled())
        self.assertEqual(1, connection.disconnect_calls)
        self.assertTrue(any("disconnect failed" in record for record in log_context.output))
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_direct_listener_await_preserves_exact_primary_cancellation_and_args(self):
        primary_error = asyncio.CancelledError("direct primary", 230)
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        self.data_source.assistants.put_nowait(assistant)
        self.data_source._process_websocket_messages = AsyncMock(side_effect=primary_error)

        with self.assertLogs(self.data_source.logger(), level="ERROR"):
            with self.assertRaises(asyncio.CancelledError) as cancellation_context:
                await self.data_source.listen_for_subscriptions()

        self.assertIs(primary_error, cancellation_context.exception)
        self.assertEqual(("direct primary", 230), cancellation_context.exception.args)
        self.assertEqual(1, connection.disconnect_calls)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_scheduled_listener_identity_follows_the_cpython_task_substrate(self):
        for tracker_wrapped in (False, True):
            with self.subTest(tracker_wrapped=tracker_wrapped):
                self.data_source = ListenerTestOrderBookTrackerDataSource()
                primary_error = asyncio.CancelledError("scheduled primary", tracker_wrapped)
                processing_release = asyncio.Event()
                connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
                assistant = WSAssistant(connection)

                async def process_messages(*args, **kwargs):
                    await processing_release.wait()
                    raise primary_error

                self.data_source._process_websocket_messages = process_messages
                self.data_source.assistants.put_nowait(assistant)
                if tracker_wrapped:
                    tracker = OrderBookTracker(self.data_source, ["BTC-USDT"])
                    listener_task = tracker._schedule_data_source_task(0, "listen_for_subscriptions")
                    subscribed = await asyncio.wait_for(self.data_source.subscribed_assistants.get(), timeout=0.2)
                    self.assertIs(assistant, subscribed)
                else:
                    listener_task = await self._start_listener(assistant)

                with self.assertLogs(self.data_source.logger(), level="ERROR"):
                    processing_release.set()
                    with self.assertRaises(asyncio.CancelledError) as cancellation_context:
                        await listener_task

                self.assertIs(type(primary_error), type(cancellation_context.exception))
                if sys.version_info < (3, 11):
                    self.assertIsNot(primary_error, cancellation_context.exception)
                    self.assertEqual((), cancellation_context.exception.args)
                else:
                    self.assertIs(primary_error, cancellation_context.exception)
                    self.assertEqual(("scheduled primary", tracker_wrapped), cancellation_context.exception.args)
                self.assertEqual(1, connection.disconnect_calls)
                self.assertIsNone(self.data_source._ws_assistant)

    async def test_tracker_task_preserves_primary_cancellation_over_disconnect_failure(self):
        connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        assistant = WSAssistant(connection)
        self.data_source.assistants.put_nowait(assistant)
        tracker = OrderBookTracker(self.data_source, ["BTC-USDT"])
        listener_task = tracker._schedule_data_source_task(0, "listen_for_subscriptions")
        subscribed_assistant = await asyncio.wait_for(
            self.data_source.subscribed_assistants.get(),
            timeout=0.2,
        )
        self.assertIs(assistant, subscribed_assistant)

        with self.assertLogs(self.data_source.logger(), level="ERROR") as log_context:
            listener_task.cancel("tracker primary cancellation")
            with self.assertRaises(asyncio.CancelledError) as cancellation_context:
                await listener_task

        self._assert_task_boundary_cancellation(
            cancellation_context.exception,
            ("tracker primary cancellation",),
        )
        self.assertTrue(listener_task.cancelled())
        self.assertEqual(1, connection.disconnect_calls)
        self.assertTrue(any("disconnect failed" in record for record in log_context.output))
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_old_listener_finishing_cannot_clear_replacement_assistant(self):
        disconnect_release = asyncio.Event()
        old_connection = _ControllableConnection(disconnect_release=disconnect_release)
        old_assistant = WSAssistant(old_connection)
        old_listener_task = await self._start_listener(old_assistant)
        old_connection.incoming_messages.put_nowait(_END_OF_STREAM)
        await asyncio.wait_for(old_connection.disconnect_started.wait(), timeout=0.2)

        replacement_connection = _ControllableConnection()
        replacement_assistant = WSAssistant(replacement_connection)
        replacement_listener_task = await self._start_listener(replacement_assistant)

        disconnect_release.set()
        await asyncio.sleep(0)
        self.assertIs(replacement_assistant, self.data_source._ws_assistant)

        await self._cancel_listener(old_listener_task)
        self.assertIs(replacement_assistant, self.data_source._ws_assistant)
        await self._cancel_listener(replacement_listener_task)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_old_listener_raising_cannot_clear_replacement_assistant(self):
        disconnect_release = asyncio.Event()
        old_connection = _ControllableConnection(
            disconnect_release=disconnect_release,
            disconnect_error=RuntimeError("old disconnect failed"),
        )
        old_assistant = WSAssistant(old_connection)
        old_listener_task = await self._start_listener(old_assistant)
        old_connection.incoming_messages.put_nowait(_END_OF_STREAM)
        await asyncio.wait_for(old_connection.disconnect_started.wait(), timeout=0.2)

        replacement_connection = _ControllableConnection()
        replacement_assistant = WSAssistant(replacement_connection)
        replacement_listener_task = await self._start_listener(replacement_assistant)

        with self.assertLogs(self.data_source.logger(), level="ERROR") as log_context:
            disconnect_release.set()
            for _ in range(20):
                if old_connection.disconnect_calls == 1:
                    await asyncio.sleep(0)
                    break
                await asyncio.sleep(0)
        self.assertTrue(any("old disconnect failed" in record for record in log_context.output))
        self.assertIs(replacement_assistant, self.data_source._ws_assistant)

        await self._cancel_listener(old_listener_task)
        await self._cancel_listener(replacement_listener_task)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_reconnect_uses_fresh_assistant_and_ignores_stale_frames(self):
        old_connection = _ControllableConnection(disconnect_error=RuntimeError("disconnect failed"))
        old_assistant = WSAssistant(old_connection)
        fresh_connection = _ControllableConnection()
        fresh_assistant = WSAssistant(fresh_connection)
        listener_task = await self._start_listener(old_assistant)
        self.data_source.assistants.put_nowait(fresh_assistant)

        with self.assertLogs(self.data_source.logger(), level="ERROR"):
            old_connection.incoming_messages.put_nowait(_END_OF_STREAM)
            subscribed_assistant = await asyncio.wait_for(
                self.data_source.subscribed_assistants.get(),
                timeout=0.2,
            )
        self.assertIs(fresh_assistant, subscribed_assistant)

        stale_frame = {"channel": self.data_source._trade_messages_queue_key, "session": "stale"}
        old_connection.incoming_messages.put_nowait(stale_frame)
        fresh_frame = {"channel": self.data_source._trade_messages_queue_key, "session": "fresh"}
        fresh_connection.incoming_messages.put_nowait(fresh_frame)

        message_queue = self.data_source._message_queue[self.data_source._trade_messages_queue_key]
        received_frame = await asyncio.wait_for(message_queue.get(), timeout=0.2)
        self.assertEqual(fresh_frame, received_frame)
        self.assertTrue(message_queue.empty())
        self.assertEqual([old_assistant, fresh_assistant], self.data_source.connected_assistants)

        await self._cancel_listener(listener_task)
        self.assertIsNone(self.data_source._ws_assistant)

    async def test_failed_reconnect_does_not_disconnect_previous_assistant_twice(self):
        connection = _ControllableConnection()
        assistant = WSAssistant(connection)
        connection.incoming_messages.put_nowait(_END_OF_STREAM)

        with patch.object(
            self.data_source,
            "_connected_websocket_assistant",
            new=AsyncMock(side_effect=[assistant, RuntimeError("connect failed")]),
        ), patch.object(
            self.data_source,
            "_sleep",
            new=AsyncMock(side_effect=asyncio.CancelledError("stop retry")),
        ):
            with self.assertRaises(asyncio.CancelledError) as cancellation_context:
                await self.data_source.listen_for_subscriptions()

        self.assertEqual(("stop retry",), cancellation_context.exception.args)
        self.assertEqual(1, connection.disconnect_calls)
