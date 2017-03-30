#!/usr/bin/env python
# -*- coding:Utf-8 -*-

import enums
import datetime

from steamcommerce_api.api import userrequest
from steamcommerce_api.api import paidrequest

from steamcommerce_api.core import models
from steamcommerce_api.caching import cache_layer


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

    def get_userrequest_relations(self, user_id, commitment_level, eq=True, informed=False):
        if eq:
            commitment_condition = self.userrequest_relation_model.commitment_level == commitment_level
        else:
            commitment_condition = self.userrequest_relation_model.commitment_level != commitment_level

        if informed:
            conditions = [
                self.userrequest_model.informed == True,
                self.userrequest_model.visible == True,
                self.userrequest_model.accepted == False,
                (self.userrequest_model.assigned == None) | (self.userrequest_model.assigned == user_id),
                self.userrequest_model.paid == False
            ]
        else:
            conditions = [
                self.userrequest_model.paid == True,
                self.userrequest_model.visible == True,
                self.userrequest_model.accepted == False,
                (self.userrequest_model.assigned == None) | (self.userrequest_model.assigned == user_id)
            ]

        relations = self.userrequest_relation_model.select().where(
            commitment_condition,
            self.userrequest_relation_model.sent == False
        ).join(self.userrequest_model).where(*conditions)

        return relations

    def get_paidrequest_relations(self, user_id, commitment_level, eq=True):
        if eq:
            commitment_condition = self.paidrequest_relation_model.commitment_level == commitment_level
        else:
            commitment_condition = self.paidrequest_relation_model.commitment_level != commitment_level

        relations = self.paidrequest_relation_model.select().where(
            commitment_condition,
            self.paidrequest_relation_model.sent == False
        ).join(self.paidrequest_model).where(
            self.paidrequest_model.authed == True,
            self.paidrequest_model.visible == True,
            self.paidrequest_model.accepted == False,
            (self.paidrequest_model.assigned == None) | (self.paidrequest_model.assigned == user_id)
        )

        return relations

    def get_commited_sub_ids(self, user_id, informed=False):
        userrequest_relations = self.get_userrequest_relations(
            user_id,
            enums.ERelationCommitment.AddedToCart.value,
            informed=informed
        )

        paidrequest_relations = self.get_paidrequest_relations(
            user_id,
            enums.ERelationCommitment.AddedToCart.value
        )

        subids = []

        for relation in userrequest_relations:
            product = relation.product

            sub_id = product.sub_id or product.store_sub_id

            if not sub_id or sub_id in subids:
                continue

            subids.append(sub_id)

        for relation in paidrequest_relations:
            product = relation.product

            sub_id = product.sub_id or product.store_sub_id

            if not sub_id or sub_id in subids:
                continue

            subids.append(sub_id)

        return subids

    def get_uncommited_relations(self, user_id, informed=False):
        paidrequest_relations = self.get_paidrequest_relations(
            user_id,
            enums.ERelationCommitment.Uncommited.value,
            informed=informed
        )

        userrequest_relations = self.get_userrequest_relations(
            user_id,
            enums.ERelationCommitment.Uncommited.value
        )

        items = {}
        commited_subids = self.get_commited_sub_ids(user_id)

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
            userrequest = relation.request

            if (
                userrequest.promotion and
                not userrequest.paid_before_promotion_end_date and
                not userrequest.informed and
                userrequest.expiration_date and
                userrequest.expiration_date < datetime.datetime.now()
            ):
                continue

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

    def rollback_failed_relations(self, shopping_cart_gid):
        self.userrequest_relation_model.update(
            task_id=None,
            commited_on_bot=None,
            shopping_cart_gid=None,
            commitment_level=enums.ERelationCommitment.Uncommited.value,
        ).where(
            self.userrequest_relation_model.shopping_cart_gid == shopping_cart_gid
        ).execute()

        self.paidrequest_relation_model.update(
            task_id=None,
            commited_on_bot=None,
            shopping_cart_gid=None,
            commitment_level=enums.ERelationCommitment.Uncommited.value,
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

    def set_relation_commitment(
        self,
        relation_type,
        relation_id,
        commitment_level,
        task_id=None,
        commited_on_bot=None,
        shopping_cart_gid=None
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

    def commit_relations(self, items, commitment_level=None, task_id=None, commited_on_bot=None):
        for item in items:
            relation_type = item.get('relation_type')
            relation_id = item.get('relation_id')

            self.set_relation_commitment(
                relation_type,
                relation_id,
                commitment_level,
                task_id=task_id,
                commited_on_bot=commited_on_bot
            )

    def assign_requests_to_user(self, owner_id, items):
        for item in items:
            relation_type = item.get('relation_type')
            relation_id = item.get('relation_id')

            relation = self.get_relation(relation_type, relation_id)

            if relation_type == 'A':
                userrequest.UserRequest().assign(relation.request.id, owner_id)
            elif relation_type == 'C':
                paidrequest.PaidRequest().assign(relation.request.id, owner_id)

    def commit_purchased_relations(self, shopping_cart_gid):
        userrequest_relations = self.userrequest_relation_model.select().where(
            self.userrequest_relation_model.shopping_cart_gid == shopping_cart_gid
        )

        paidrequest_relations = self.paidrequest_relation_model.select().where(
            self.paidrequest_relation_model.shopping_cart_gid == shopping_cart_gid
        )

        for relation in userrequest_relations:
            self.set_relation_commitment('A', relation.id, enums.ERelationCommitment.Purchased.value)

        for relation in paidrequest_relations:
            self.set_relation_commitment('C', relation.id, enums.ERelationCommitment.Purchased.value)
