#!/usr/bin/env python
# -*- coding:Utf-8 -*-

import config
from controllers import edge

if __name__ == '__main__':
    edge_controller = edge.EdgeController(
        config.OWNER_ID,
        config.GIFTEE_ACCOUNT_ID,
        payment_method=config.PAYMENT_METHOD
    )

    edge_controller.push_relations()
