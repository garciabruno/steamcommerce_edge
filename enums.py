#!/usr/bin/env python
# -*- coding:Utf-8 -*-

from enum import IntEnum


class ERelationCommitment(IntEnum):
    Uncommited = 0
    AddedToCart = 1
    Purchased = 2
    FailedToAddToCart = 3
    PushedToCart = 4


class EEdgeServerStatus(IntEnum):
    Enabled = 1
    Disabled = 2


class EEdgeBotStatus(IntEnum):
    StandingBy = 1
    PushingItemsToCart = 2
    PurchasingCart = 3
    WatingForSufficientFunds = 4
    BlockedForTooManyPurchases = 5
    BlockedForUnknownReason = 6


class EEdgeBotType(IntEnum):
    Purchases = 1
    Delivery = 2
    AntiCheatPurchases = 3
    Notification = 4


class EdgeResult(IntEnum):
    IncompleteForm = 1
    ParamNotSerializable = 2
    TaskNotFound = 3
