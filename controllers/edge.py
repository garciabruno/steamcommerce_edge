#!/usr/bin/env python
# -*- coding:Utf-8 -*-

import re
import time
import json
import requests
import datetime

import enums
import config

from controllers.relations import RelationController

from steamcommerce_api.api import logger
from steamcommerce_api.core import models

from steam import SteamID
from steam.enums import EResult
from coinbase.wallet.client import Client

log = logger.Logger('edge.controller', 'edge.controller.log').get_logger()


class EdgeController(object):
    def __init__(self, owner_id, payment_method='steamaccount'):
        self.owner_id = owner_id
        self.payment_method = payment_method

        self.user_model = models.User
        self.edge_bot_model = models.EdgeBot
        self.edge_task_model = models.EdgeTask
        self.edge_server_model = models.EdgeServer

    '''
    Task methods
    '''

    def create_edge_task(self, edge_bot_id, edge_server_id, data):
        edge_task = self.edge_task_model(**data)

        edge_task.edge_bot = edge_bot_id
        edge_task.edge_server = edge_server_id

        edge_task.save()

        log.info(
            u'Created task_id {0} for network_id {1} on edge server #{2}'.format(
                edge_task.task_id,
                edge_task.edge_bot.network_id,
                edge_server_id
            )
        )

        return edge_task.id

    def update_edge_task_status(self, task_id, task_status):
        self.edge_task_model.update(
            task_status=task_status
        ).where(
            self.edge_task_model.task_id == task_id
        ).execute()

    def get_pending_tasks(self):
        return self.edge_task_model.select().where(
            self.edge_task_model.task_status == 'PENDING'
        )

    def process_cart_result(self, edge_task, task_result):
        succesful_items = task_result.get('items')
        failed_items = task_result.get('failed_items')
        failed_shopping_cart_gids = task_result.get('failed_shopping_cart_gids')

        RelationController().rollback_pushed_relations(edge_task.task_id)

        if len(failed_shopping_cart_gids):
            log.info(u'Received a list of previously commited shoppingCartGID that failed')

            for shopping_cart_gid in failed_shopping_cart_gids:
                RelationController().rollback_failed_relations(shopping_cart_gid)

        if len(failed_items):
            log.info(u'Received a list of relations that fail to add to cart')

            for item in failed_items:
                relation_type = item.get('relation_type')
                relation_id = item.get('relation_id')

                RelationController().set_relation_commitment(
                    relation_type,
                    relation_id,
                    enums.ERelationCommitment.FailedToAddToCart.value,
                    edge_task.task_id,
                    commited_on_bot=edge_task.edge_bot.network_id
                )

        log.info(u'Received {} succesful items'.format(len(succesful_items)))

        for item in succesful_items:
            shopping_cart_gid = task_result.get('shoppingCartGID')

            relation_type = item.get('relation_type')
            relation_id = item.get('relation_id')

            RelationController().set_relation_commitment(
                relation_type,
                relation_id,
                enums.ERelationCommitment.AddedToCart.value,
                shopping_cart_gid=shopping_cart_gid
            )

        if len(succesful_items):
            # Assume there is one cart-push per user, so just grab the first on the list

            user_id = succesful_items[0].get('user_id')
            user = self.user_model.get(id=user_id)
            account_id = SteamID(user.steam).as_32

            self.call_checkout(
                edge_task.edge_bot,
                edge_task.edge_server,
                account_id
            )
        else:
            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.StandingBy.value
            )

    def process_cart_checkout(self, edge_task, task_result):
        if type(task_result) is int:
            transaction_result = enums.ETransactionResult(task_result)

            if (
                transaction_result == enums.ETransactionResult.Fail or
                transaction_result == enums.ETransactionResult.TransIdNotFound
            ):
                log.info(u'Unable to purchase cart for unknown reason')

                self.set_edge_bot_status(
                    edge_task.edge_bot.network_id,
                    enums.EEdgeBotStatus.BlockedForUnknownReason
                )
            elif transaction_result == enums.ETransactionResult.ShoppingCartGIDNotFound:
                log.info(u'Attemped to purchase a cart without shoppingCartGID')

                self.set_edge_bot_status(
                    edge_task.edge_bot.network_id,
                    enums.EEdgeBotStatus.StandingBy.value
                )
            elif transaction_result == enums.ETransactionResult.InsufficientFunds:
                log.info(u'Insufficient funds to complete cart checkout')

                self.set_edge_bot_status(
                    edge_task.edge_bot.network_id,
                    enums.EEdgeBotStatus.WaitingForSufficientFunds.value
                )
            elif transaction_result == enums.ETransactionResult.TooManyPurchases:
                log.info(u'Too many purchases made in the last few hours')

                self.set_edge_bot_status(
                    edge_task.edge_bot.network_id,
                    enums.EEdgeBotStatus.BlockedForTooManyPurchases.value
                )

            return transaction_result

        if type(task_result) is dict:
            transid = task_result.get('transid')
            result = EResult(task_result.get('result'))
            payment_method = task_result.get('payment_method')
            shopping_cart_gid = task_result.get('shopping_cart_gid')

            log.info(
                u'Cart checkout with payment method {0} received {1}'.format(
                    payment_method,
                    repr(result)
                )
            )

            if result == EResult.OK:
                if payment_method == 'bitcoin':
                    self.get_transaction_link(edge_task.edge_bot, edge_task.edge_server, transid)
                elif payment_method == 'steamaccount':
                    RelationController().commit_purchased_relations(shopping_cart_gid, self.owner_id)

                    self.set_edge_bot_status(
                        edge_task.edge_bot.network_id,
                        enums.EEdgeBotStatus.StandingBy.value
                    )

    def process_external_transaction(self, edge_task, task_result):
        if type(task_result) is int:
            log.error(u'Unable to complete external transaction, received {}'.format(task_result))

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        bitpay_url = task_result.get('link')
        shopping_cart_gid = task_result.get('shopping_cart_gid')

        if not bitpay_url:
            log.error(u'Failed to retrieve a bitpay invoice url')

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        log.info(u'Received bitpay url {}'.format(bitpay_url))

        invoice_matches = re.findall('/i/([a-zA-Z0-9]+)', bitpay_url, re.DOTALL)

        if not len(invoice_matches):
            log.error(u'Failed to extract invoice_id from {}'.format(bitpay_url))

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        invoice_id = invoice_matches[0]
        log.info(u'Found bitpay invoice_id {}'.format(invoice_id))

        try:
            req = requests.get('https://bitpay.com/invoices/{}'.format(invoice_id), timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Bitpay API timed out')

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None
        except Exception, e:
            log.error(u'Unable to contact Bitpay API, raised {}'.format(e))

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize data, received {}'.format(req.text))

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        data = response.get('data')

        if data.get('status') != 'new':
            log.error(u'Bitpay Invoice id {0} status is {1}'.format(invoice_id, data.get('status')))

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        log.info(
            u'Invoice BTC price is {0} (${1} {2}) to address {3}'.format(
                data.get('btcDue'),
                data.get('price'),
                data.get('currency'),
                data.get('bitcoinAddress')
            )
        )

        client = Client(config.COINBASE_API_KEY, config.COINBASE_API_SECRET)
        primary_account = client.get_primary_account()

        if float(primary_account.get('balance').get('amount')) < float(data.get('btcDue')):
            log.info(u'Insufficient Coinbase funds for transaction')

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.WaitingForSufficientFunds.value
            )

            return None

        btc_amount = data.get('btcDue')
        to_address = data.get('bitcoinAddress')

        log.info(
            u'Sending {0} BTC to address {1} for shoppingCartGID {2}'.format(
                btc_amount,
                to_address,
                shopping_cart_gid
            )
        )

        try:
            tx = primary_account.send_money(
                to=to_address,
                amount=btc_amount,
                currency='BTC',
                idem=str(shopping_cart_gid)
            )
        except Exception, e:
            log.error(u'Unable to perform Coinbase transaction, raised {}'.format(e))

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        log.info(
            u'Coinbase transaction id {0} created for {1} BTC ({2} {3})'.format(
                tx.get('id'),
                tx.get('amount').get('amount'),
                tx.get('native_amount').get('amount'),
                tx.get('native_amount').get('currency')
            )
        )

        RelationController().commit_purchased_relations(shopping_cart_gid, self.owner_id)
        self.reset_shopping_cart(edge_task.edge_bot, edge_task.edge_server)

        self.set_edge_bot_status(
            edge_task.edge_bot.network_id,
            enums.EEdgeBotStatus.StandingBy.value
        )

    def get_task_callback(self, task_name):
        callbacks = {
            'add_subids_to_cart': self.process_cart_result,
            'checkout_cart': self.process_cart_checkout,
            'get_external_link_from_transid': self.process_external_transaction
        }

        return callbacks.get(task_name)

    def process_pending_tasks(self):
        edge_tasks = self.get_pending_tasks()
        tasks_count = edge_tasks.count()

        if not tasks_count:
            return None

        log.info(u'Processing {} pending tasks'.format(tasks_count))

        for edge_task in edge_tasks:
            log.info(
                u'Processing task {0} id {1}'.format(edge_task.task_name, edge_task.task_id)
            )

            response = self.get_edge_bot_task_status(edge_task)

            if not response:
                self.update_edge_task_status(edge_task.task_id, 'FAILURE')

                continue

            if not response.get('success'):
                log.info(u'Failed to retrieve task status for {}'.format(edge_task.task_id))
                self.update_edge_task_status(edge_task.task_id, 'FAILURE')

                continue

            task_result = response.get('task_result')
            task_status = response.get('task_status')

            if task_status == 'PENDING' or task_status == 'RUNNING':
                log.info(u'Edge task {} has not been completed yet'.format(edge_task.task_id))

                continue

            if task_status == 'FAILURE':
                log.error(u'Edge task id {} returned FAILURE'.format(edge_task.task_id))

                continue

            log.info(
                u'Received SUCCESS on task {0} id {1}'.format(
                    edge_task.task_name,
                    edge_task.task_id
                )
            )

            task_callback = self.get_task_callback(edge_task.task_name)

            if not task_callback:
                log.error(u'Could not find a callback for task {}'.format(edge_task.task_name))

                self.update_edge_task_status(edge_task.task_id, task_status)

                continue

            if not task_result:
                log.error(u'Received SUCCESS from task id {} but no result was found'.format(edge_task.task_id))

                self.update_edge_task_status(edge_task.task_id, task_status)

                continue

            task_callback.__call__(edge_task, task_result)

            self.update_edge_task_status(edge_task.task_id, task_status)

    def get_edge_bot_task_status(self, edge_task):
        url = self.get_edge_api_url(edge_task.edge_server.ip_address, 'task/state/')

        data = {
            'task_name': edge_task.task_name,
            'task_id': edge_task.task_id
        }

        try:
            req = requests.post(url, data=data, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server #{} timed out'.format(edge_task.edge_server.id))

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        return response

    '''
    Edge methods
    '''

    def get_edge_servers(self):
        return self.edge_server_model.select()

    def get_edge_server_for_currency(self, currency_code):
        try:
            return self.edge_server_model.get(
                currency_code=currency_code,
                status=enums.EEdgeServerStatus.Enabled
            )
        except self.edge_server_model.DoesNotExist:
            return None

    def get_edge_bot_by_network_id(self, network_id):
        try:
            return self.edge_bot_model.get(
                network_id=network_id,
                status=enums.EEdgeBotStatus.StandingBy
            )
        except self.edge_bot_model.DoesNotExist:
            pass

        return None

    def get_edge_bot_for_currency(self, currency_code, bot_type=enums.EEdgeBotType.Purchases):
        try:
            return self.edge_bot_model.get(
                currency_code=currency_code,
                status=enums.EEdgeBotStatus.StandingBy,
                bot_type=bot_type
            )
        except self.edge_bot_model.DoesNotExist:
            pass

        return None

    def get_edge_api_url(self, ip_address, endpoint_name):
        return 'http://{0}/edge/{1}'.format(ip_address, endpoint_name)

    def get_isteamuser_api_url(self, ip_address, endpoint_name):
        return 'http://{0}/ISteamUser/{1}/'.format(ip_address, endpoint_name)

    def update_edge_server_healthy_check(self, edge_server_id):
        return self.edge_server_model.update(
            last_health_check=datetime.datetime.now()
        ).where(
            self.edge_server_model.id == edge_server_id
        ).execute()

    def edge_server_is_healthy(self, edge_server):
        url = self.get_edge_api_url(edge_server.ip_address, 'healthcheck')

        requested_at = time.time()
        HEADERS = {'X-Requested-At': str(requested_at)}

        try:
            req = requests.get(url, headers=HEADERS, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server #{} timed out'.format(edge_server.id))

            return False
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return False

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return False

        delay = req.text
        log.info(u'Delay to edge server #{0} is {1} seconds'.format(edge_server.id, delay))

        self.update_edge_server_healthy_check(edge_server.id)

        return True

    def set_edge_bot_status(self, network_id, status):
        return self.edge_bot_model.update(status=status).where(
            self.edge_bot_model.network_id == network_id
        ).execute()

    def get_edge_bot_friends_list(self, edge_bot, edge_server):
        log.info(
            u'Getting FriendsList for edge bot with network id {0} through edge server #{1}'.format(
                edge_bot.network_id,
                edge_server.id
            )
        )

        url = self.get_isteamuser_api_url(edge_server.ip_address, 'GetFriendsList')
        params = {'network_id': edge_bot.network_id, 'ids': 1}

        try:
            req = requests.get(
                url,
                params=params,
                timeout=(10.0, 20.0)
            )
        except requests.exceptions.Timeout:
            log.error(u'Edge server #{} timed out'.format(edge_server.id))

            return False
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return False

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return False

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        return response

    def push_relations_to_edge_bot(self, edge_bot, edge_server, items):
        log.info(
            u'Pushing {0} relations to edge bot with network id {1} through edge server #{2}'.format(
                len(items),
                edge_bot.network_id,
                edge_server.id
            )
        )

        url = self.get_edge_api_url(edge_server.ip_address, 'cart/push/')

        data = {
            'network_id': edge_bot.network_id,
            'items': json.dumps(items)
        }

        self.set_edge_bot_status(
            edge_bot.network_id,
            enums.EEdgeBotStatus.PushingItemsToCart.value
        )

        try:
            req = requests.post(url, data=data, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server {} timed out'.format(edge_server.id))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        if not response.get('success'):
            log.info(
                u'Received {0} from edge bot {1}'.format(
                    repr(enums.EdgeResult(response.get('result'))),
                    edge_bot.network_id
                )
            )

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        self.create_edge_task(edge_bot.id, edge_server.id, response)

        RelationController().commit_relations(
            items,
            commited_on_bot=edge_bot.network_id,
            task_id=response.get('task_id'),
            commitment_level=enums.ERelationCommitment.PushedToCart.value
        )

        RelationController().assign_requests_to_user(self.owner_id, items)

    def get_add_friends_result(self, edge_bot, edge_server):
        log.info(
            u'Getting friend add results on edge bot with network_id {1} through edge server {2}'.format(
                edge_bot.network_id,
                edge_server.id
            )
        )

        url = self.get_isteamuser_api_url(edge_server.ip_address, 'GetFriendAddResults')

        params = {
            'network_id': edge_bot.network_id
        }

        try:
            req = requests.get(url, params=params, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server {} timed out'.format(edge_server.id))

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        return response

    def send_invitation(self, edge_bot, edge_server, steam_id):
        log.info(
            u'Adding SteamID {0} on edge bot with network_id {1} through edge server {2}'.format(
                steam_id,
                edge_bot.network_id,
                edge_server.id
            )
        )

        url = self.get_isteamuser_api_url(edge_server.ip_address, 'AddFriend')

        params = {
            'steam_id': steam_id,
            'network_id': edge_bot.network_id
        }

        try:
            req = requests.get(url, params=params, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server {} timed out'.format(edge_server.id))

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        if '0' in response.keys():
            log.error(u'Edge bot with network id {} friendlist is full!'.format(edge_bot.network_id))

            return False

        return response

    def send_invitations(self, anticheat_policy=False):
        items = RelationController().get_relations(
            self.owner_id,
            enums.ERelationCommitment.Uncommited.value,
            anticheat_policy=anticheat_policy
        )

        if not len(items.keys()):
            log.info(u'No Uncommited relations found to send invitations')

        edge_bots_friendslists = {}

        for user_id in items.keys():
            for currency_code in items[user_id].keys():
                log.info(u'Processing relations for currency {}'.format(currency_code))

                if anticheat_policy:
                    edge_bot = self.get_edge_bot_for_currency(
                        currency_code,
                        bot_type=enums.EEdgeBotType.AntiCheatPurchases
                    )
                else:
                    edge_bot = self.get_edge_bot_for_currency(currency_code)

                if not edge_bot:
                    log.info(u'No available edge bot found for currency {}'.format(currency_code))

                    continue

                log.info(
                    u'Edge Bot with network id {0} selected for currency {1}'.format(
                        edge_bot.network_id,
                        currency_code
                    )
                )

                edge_server = self.get_edge_server_for_currency(currency_code)

                if not edge_server:
                    log.info(u'Not available edge server found for currency {}'.format(currency_code))

                    continue

                if not self.edge_server_is_healthy(edge_server):
                    log.info(u'Edge server #{} is not currently healthy'.format(edge_server.id))

                    continue

                if not edge_bots_friendslists.get(edge_bot.network_id):
                    friendslist = self.get_edge_bot_friends_list(edge_bot, edge_server)

                    if not friendslist:
                        continue

                    edge_bots_friendslists[edge_bot.network_id] = friendslist

                user = self.user_model.get(id=user_id)

                if int(user.steam) not in edge_bots_friendslists[edge_bot.network_id]:
                    invitation_result = self.send_invitation(edge_bot, edge_server, user.steam)

                    if not invitation_result:
                        # TODO: EdgeBot's friendlist is full, clean it!

                        continue

                RelationController().commit_relations(
                    items[user_id][currency_code],
                    commited_on_bot=edge_bot.network_id,
                    commitment_level=enums.ERelationCommitment.WaitingForInviteAccept.value
                )

    def push_relations(self, anticheat_policy=False):
        items = RelationController().get_relations(
            self.owner_id,
            enums.ERelationCommitment.WaitingForInviteAccept.value,
            anticheat_policy=anticheat_policy
        )

        if not len(items.keys()):
            log.info(u'No WaitingForInviteAccept pending relations found')

        for user_id in items.keys():
            for currency_code in items[user_id].keys():
                # TODO: Perpahs we should filter by commited_on_bot in WaitingForInviteAccept commitment
                # {'user_id': {'currency': {'edge_bot_id': [items]}}}

                item = items[user_id][currency_code][0]

                relation = RelationController().get_relation(
                    item.get('relation_type'),
                    item.get('relation_id')
                )

                if not relation.commited_on_bot:
                    # This relation does not belong to any bot. Weird?

                    continue

                edge_bot = self.get_edge_bot_by_network_id(relation.commited_on_bot)

                if not edge_bot:
                    log.info(u'No available edge bot found for currency {}'.format(currency_code))

                    continue

                log.info(
                    u'Edge Bot with network id {0} selected for currency {1}'.format(
                        edge_bot.network_id,
                        currency_code
                    )
                )

                # For now assume there is only one EdgeServer per currency

                edge_server = self.get_edge_server_for_currency(currency_code)

                if not edge_server:
                    log.info(u'Not available edge server found for currency {}'.format(currency_code))

                    continue

                if not self.edge_server_is_healthy(edge_server):
                    log.info(u'Edge server #{} is not currently healthy'.format(edge_server.id))

                    continue

                friendslist = self.get_edge_bot_friends_list(edge_bot, edge_server)

                if not friendslist:
                    # TODO: EdgeBot's friendlist is full. Clean it

                    continue

                user = self.user_model.get(id=user_id)

                if int(user.steam) not in friendslist:
                    continue

                # User is EdgeBot's friendslist

                self.push_relations_to_edge_bot(
                    edge_bot,
                    edge_server,
                    items[user_id][currency_code]
                )

    def call_checkout(self, edge_bot, edge_server, account_id):
        log.info(
            u'Calling checkout to edge bot with network id {0} through edge server #{1}'.format(
                edge_bot.network_id,
                edge_server.id
            )
        )

        self.set_edge_bot_status(
            edge_bot.network_id,
            enums.EEdgeBotStatus.PurchasingCart.value
        )

        url = self.get_edge_api_url(edge_server.ip_address, 'cart/checkout/')

        data = {
            'network_id': edge_bot.network_id,
            'giftee_account_id': account_id,
            'payment_method': self.payment_method
        }

        try:
            req = requests.post(url, data=data, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server {} timed out'.format(edge_server.id))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            self.set_edge_bot_status(
                edge_bot.network_id,
                enums.EEdgeBotStatus.BlockedForUnknownReason.value
            )

            return None

        self.create_edge_task(edge_bot.id, edge_server.id, response)

    def get_transaction_link(self, edge_bot, edge_server, transid):
        log.info(
            u'Getting transaction link for transid {0} to edge bot with network id {1} through edge server #{2}'.format(
                transid,
                edge_bot.network_id,
                edge_server.id
            )
        )

        url = self.get_edge_api_url(edge_server.ip_address, 'transaction/link/')

        data = {
            'transid': transid,
            'network_id': edge_bot.network_id,
        }

        try:
            req = requests.post(url, data=data, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server {} timed out'.format(edge_server.id))

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        self.create_edge_task(edge_bot.id, edge_server.id, response)

    def reset_shopping_cart(self, edge_bot, edge_server):
        url = self.get_edge_api_url(edge_server.ip_address, 'cart/reset/')

        data = {
            'network_id': edge_bot.network_id,
        }

        try:
            req = requests.post(url, data=data, timeout=(10.0, 20.0))
        except requests.exceptions.Timeout:
            log.error(u'Edge server {} timed out'.format(edge_server.id))

            return None
        except Exception, e:
            log.error(u'Unable to contact edge server, raised {}'.format(e))

            return None

        if req.status_code != 200:
            log.error(u'Unable to contact edge server, received status code {}'.format(req.status_code))

            return None

        try:
            response = req.json()
        except ValueError:
            log.error(u'Unable to serialize response from edge server, received {}'.format(req.text))

            return None

        self.create_edge_task(edge_bot.id, edge_server.id, response)
