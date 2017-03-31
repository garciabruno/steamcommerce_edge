#!/usr/bin/env python
# -*- coding:Utf-8 -*-

import config
import rollbar

from controllers import edge

rollbar.init(config.ROLLBAR_TOKEN, config.ROLLBAR_ENV)

if __name__ == '__main__':
    try:
        edge_controller = edge.EdgeController(
            config.OWNER_ID,
            config.GIFTEE_ACCOUNT_ID,
            payment_method=config.PAYMENT_METHOD
        )

        edge_controller.push_relations(informed=config.USE_INFORMED)
        edge_controller.push_relations(informed=config.USE_INFORMED, anticheat_policy=True)
    except IOError:
        rollbar.report_message('Got an IOError in the main loop', 'warning')
    except:
        rollbar.report_exc_info()
