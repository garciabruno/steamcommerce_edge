#!/usr/bin/env python
# -*- coding:Utf-8 -*-

import time
import json
import requests

import enums
import datetime

from steamcommerce_api.api import logger
from steamcommerce_api.api import userrequest
from steamcommerce_api.api import paidrequest

from steamcommerce_api.core import models
from steamcommerce_api.api.caching import cache_layer

log = logger.Logger('edge.controller', 'edge.controller.log').get_logger()


class RelationController(object):
    def __init__(self):
        self.userrequest_model = models.UserRequest
        self.paidrequest_model = models.PaidRequest

        self.userrequest_relation_model = models.ProductUserRequestRelation
        self.paidrequest_relation_model = models.ProductPaidRequestRelation

    def get_relation(self, relation_type, relation_id):
        if relation_type == 'A':
            return self.userrequest_relation_model.get(id=relation_id)
        elif relation_type == 'C':
            return self.paidrequest_relation_model.get(id=relation_id)

    def get_uncommited_userrequest_relations(self, user_id):
        relations = self.userrequest_relation_model.select().where(
            self.userrequest_relation_model.commitment_level == enums.ERelationCommitment.Uncommited.value,
            self.userrequest_relation_model.sent == False
        ).join(self.userrequest_model).where(
            self.userrequest_model.paid == True,
            self.userrequest_model.visible == True,
            self.userrequest_model.accepted == False,
            (self.userrequest_model.assigned == None) | (self.userrequest_model.assigned == user_id)
        )

        return relations

    def get_uncommited_paidrequest_relations(self, user_id):
        relations = self.paidrequest_relation_model.select().where(
            self.paidrequest_relation_model.commitment_level == enums.ERelationCommitment.Uncommited.value,
            self.paidrequest_relation_model.sent == False
        ).join(self.paidrequest_model).where(
            self.paidrequest_model.authed == True,
            self.paidrequest_model.visible == True,
            self.paidrequest_model.accepted == False,
            (self.paidrequest_model.assigned == None) | (self.paidrequest_model.assigned == user_id)
        )

        return relations

    def get_uncommited_relations(self, user_id):
        paidrequest_relations = self.get_uncommited_paidrequest_relations(user_id)
        userrequest_relations = self.get_uncommited_userrequest_relations(user_id)

        items = {}
        commited_subids = []

        for relation in paidrequest_relations:
            product = relation.product

            sub_id = product.sub_id or product.store_sub_id
            currency_code = product.price_currency

            # TODO: Send product.id to re-crawl store_sub_id

            if not sub_id or sub_id in commited_subids:
                continue

            if not currency_code:
                continue

            if currency_code not in items.keys():
                items[currency_code] = []

            commited_subids.append(sub_id)

            items[currency_code].append({
                'sub_id': sub_id,
                'relation_type': 'C',
                'relation_id': relation.id
            })

        for relation in userrequest_relations:
            product = relation.product

            sub_id = product.sub_id or product.store_sub_id
            currency_code = product.price_currency

            # TODO: Send product.id to re-crawl store_sub_id

            if not sub_id or sub_id in commited_subids:
                continue

            if not currency_code:
                continue

            if currency_code not in items.keys():
                items[currency_code] = []

            commited_subids.append(sub_id)

            items[currency_code].append({
                'sub_id': sub_id,
                'relation_type': 'A',
                'relation_id': relation.id
            })

        return items

    def set_relation_commitment(
        self, relation_type, relation_id, commitment_level, task_id=None, commited_on_bot=None, shopping_cart_gid=None
    ):
        params = {
            'commitment_level': commitment_level
        }

        if task_id:
            params.update({'task_id': task_id})

        if commited_on_bot:
            params.update({'commited_on_bot': commited_on_bot})

        if shopping_cart_gid:
            params.update({'shopping_cart_gid': shopping_cart_gid})

        if relation_type == 'A':
            self.userrequest_relation_model.update(**params).where(
                self.userrequest_relation_model.id == relation_id
            ).execute()

            cache_keys = ['userrequest/relation/%d' % relation_id]
        elif relation_type == 'C':
            self.paidrequest_relation_model.update(**params).where(
                self.paidrequest_relation_model.id == relation_id
            ).execute()

            cache_keys = ['paidrequest/relation/%d' % relation_id]

        cache_layer.purge_cache_keys(cache_keys)

    def rollback_failed_relations(self, shopping_cart_gid):
        self.userrequest_relation_model.update(
            task_id=None,
            commited_on_bot=None,
            shopping_cart_gid=None,
            commitment_level=enums.ERelationCommitment.StandingBy.value,
        ).where(
            self.userrequest_relation_model.shopping_cart_gid == shopping_cart_gid
        ).execute()

        self.paidrequest_relation_model.update(
            task_id=None,
            commited_on_bot=None,
            shopping_cart_gid=None,
            commitment_level=enums.ERelationCommitment.StandingBy.value,
        ).where(
            self.paidrequest_relation_model.shopping_cart_gid == shopping_cart_gid
        ).execute()

        cache_keys = ['paidrequest/relation/*', 'userrequest/relation/*']
        cache_layer.purge_cache_keys(cache_keys)

    def rollback_pushed_relations(self, task_id):
        self.userrequest_relation_model.update(
            commitment_level=enums.ERelationCommitment.Uncommited.value
        ).where(
            self.userrequest_relation_model.task_id == task_id
        ).execute()

        self.paidrequest_relation_model.update(
            commitment_level=enums.ERelationCommitment.Uncommited.value
        ).where(
            self.paidrequest_relation_model.task_id == task_id
        ).execute()

        cache_keys = ['paidrequest/relation/*', 'userrequest/relation/*']
        cache_layer.purge_cache_keys(cache_keys)


class EdgeController(object):
    def __init__(self, owner_id):
        self.owner_id = owner_id

        self.edge_server_model = models.EdgeServer
        self.edge_bot_model = models.EdgeBot
        self.edge_task_model = models.EdgeTask

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

    def get_edge_bot_for_currency(self, currency_code, bot_type=enums.EEdgeBotType.Purchases):
        try:
            return self.edge_bot_model.get(
                currency_code=currency_code,
                status=enums.EEdgeBotStatus.StandingBy,
                bot_type=bot_type
            )
        except self.edge_bot_model.DoesNotExist:
            return None

    def get_edge_api_url(self, ip_address, endpoint_name):
        return 'http://{0}/edge/{1}'.format(ip_address, endpoint_name)

    def update_edge_server_healthy_check(self, edge_server_id):
        return self.edge_server_model.update(
            last_health_check=datetime.datetime.now()
        ).where(
            self.edge_server_model.id == edge_server_id
        )

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

    def create_edge_task(self, edge_server_id, edge_bot_id, data):
        edge_task = self.edge_task_model(**data)

        edge_task.edge_bot = edge_bot_id
        edge_task.edge_server = edge_server_id

        edge_task.save()

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

    def commit_relations_to_edge_bot(self, network_id, task_id, items):
        for item in items:
            relation_type = item.get('relation_type')
            relation_id = item.get('relation_id')

            RelationController().set_relation_commitment(
                relation_type,
                relation_id,
                enums.ERelationCommitment.PushedToCart.value,
                task_id,
                commited_on_bot=network_id
            )

    def assign_requests_to_user(self, items):
        for item in items:
            relation_id = item.get('relation_id')
            relation_type = item.get('relation_type')

            relation = RelationController().get_relation(relation_type, relation_id)

            if relation_type == 'A':
                log.info(u'Assigning user id {0} to request A-{1}'.format(self.owner_id, relation.request.id))

                userrequest.UserRequest().assign(relation.request.id, self.owner_id)
            elif relation_type == 'C':
                log.info(u'Assigning user id {0} to request C-{1}'.format(self.owner_id, relation.request.id))

                paidrequest.PaidRequest().assign(relation.request.id, self.owner_id)

    def push_relations_to_edge_bot(self, edge_server, edge_bot, items):
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

        if not response.get('success'):
            log.info(
                u'Received {0} from edge bot {1}'.format(
                    repr(enums.EdgeResult(response.get('result'))),
                    edge_bot.network_id
                )
            )

            return None

        self.set_edge_bot_status(edge_bot.network_id, enums.EEdgeBotStatus.PushingItemsToCart.value)

        self.create_edge_task(edge_server.id, edge_bot.id, response)
        self.commit_relations_to_edge_bot(edge_bot.network_id, response.get('task_id'), items)
        self.assign_requests_to_user(items)

        log.info(
            u'Created task_id {0} for network_id {1} on edge server #{2}'.format(
                response.get('task_id'),
                edge_bot.network_id,
                edge_server.id
            )
        )

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

    def get_task_callback(self, task_name):
        callbacks = {
            'add_subids_to_cart': self.process_cart_result,
        }

        return callbacks.get(task_name)

    def process_pending_tasks(self):
        edge_tasks = self.get_pending_tasks()

        log.info(u'Processing {} pending tasks'.format(edge_tasks.count()))

        for edge_task in edge_tasks:
            log.info(
                u'Processing task {0} id {1}'.format(edge_task.task_name, edge_task.task_id)
            )

            response = self.get_edge_bot_task_status(edge_task)

            if not response:
                continue

            if not response.get('success'):
                log.info(u'Failed to retrieve task status for {}'.format(edge_task.task_id))

                continue

            task_result = response.get('task_result')
            task_status = response.get('task_status')

            if task_status == 'PENDING' or task_status == 'RUNNING':
                log.info(u'Edge task {} has not been completed yet'.format(edge_task.task_id))

                continue

            if task_status == 'SUCCESS':
                log.info(
                    u'Received SUCCESS on task {0} id {1}'.format(
                        edge_task.task_name,
                        edge_task.task_id
                    )
                )

                task_callback = self.get_task_callback(edge_task.task_name)

                if not task_callback:
                    continue

                task_callback.__call__(edge_task, task_result)

            if task_status == 'FAILURE':
                log.error(u'Edge task id {} returned FAILURE'.format(edge_task.task_id))

            self.update_edge_task_status(edge_task.task_id, task_status)

            self.set_edge_bot_status(
                edge_task.edge_bot.network_id,
                enums.EEdgeBotStatus.StandingBy.value
            )

    def push_relations(self):
        items = RelationController().get_uncommited_relations(self.owner_id)

        for currency_code in items.keys():
            log.info(u'Processing relations for currency {}'.format(currency_code))

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

            self.push_relations_to_edge_bot(edge_server, edge_bot, items[currency_code])
