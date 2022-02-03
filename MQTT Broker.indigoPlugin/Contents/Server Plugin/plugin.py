#! /usr/bin/env python
# -*- coding: utf-8 -*-
####################

import logging
import asyncio
from amqtt.broker import Broker
try:
    import indigo
except ImportError as error:
    print(f"Error importing indigo: {error}")

# Need to expose some of these parameters in plugin config dialog

broker_config = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "0.0.0.0:1883",
        },
    },
    "sys_interval": 10,
    "auth": {
        "allow-anonymous": True,
        "plugins": ["auth_anonymous"],
    },
    "topic-check": {
        "enabled": False,
        "plugins": [],
    },
}

do_shutdown = False

async def start_broker(broker):
    await broker.start()

async def stop_broker(broker):
    global do_shutdown

    while True:
        await asyncio.sleep(1.0)
        if do_shutdown:
            await broker.shutdown()
            break

################################################################################
class Plugin(indigo.PluginBase):

    def __init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs):
        indigo.PluginBase.__init__(self, pluginId, pluginDisplayName, pluginVersion, pluginPrefs)

        self.logLevel = int(pluginPrefs.get(u"logLevel", logging.INFO))
        self.indigo_log_handler.setLevel(self.logLevel)
        log_format = logging.Formatter('%(asctime)s.%(msecs)03d\t[%(levelname)8s] %(name)20s.%(funcName)-25s%(msg)s',
                                       datefmt='%Y-%m-%d %H:%M:%S')
        self.plugin_file_handler.setFormatter(log_format)
        self.logger.debug(f"MQTT Connector: logLevel = {self.logLevel}")

    def runConcurrentThread(self):
        self.logger.debug("RunConcurrentThread")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        broker = Broker(broker_config, loop)
        loop.run_until_complete(start_broker(broker))
        loop.run_until_complete(stop_broker(broker))

    def stopConcurrentThread(self):
        self.stopThread = True
        global do_shutdown
        do_shutdown = True

    ########################################
    # Plugin Preference Methods
    ########################################

    def closedPrefsConfigUi(self, valuesDict, userCancelled):
        if not userCancelled:
            try:
                self.logLevel = int(valuesDict[u"logLevel"])
            except ValueError:
                self.logLevel = logging.INFO
            self.indigo_log_handler.setLevel(self.logLevel)
