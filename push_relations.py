#!/usr/bin/env python
# -*- coding:Utf-8 -*-

import config
import rollbar

from controllers import edge

rollbar.init(config.ROLLBAR_TOKEN, config.ROLLBAR_ENV)

if __name__ == '__main__':
    try:
        edge_controller = edge.EdgeController(
            config.OWNER_ID
        )

        edge_controller.send_invitations()
        edge_controller.push_relations()

        edge_controller.send_invitations(anticheat_policy=True)
        edge_controller.push_relations(anticheat_policy=True)
    except IOError:
        rollbar.report_message('Got an IOError in the main loop', 'warning')
    except:
        rollbar.report_exc_info()
