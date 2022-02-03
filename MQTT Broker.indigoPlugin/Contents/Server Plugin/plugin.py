#! /usr/bin/env python
# -*- coding: utf-8 -*-
####################

import logging
import asyncio
from amqtt.broker import Broker
import indigo # noqa

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

        port = pluginPrefs.get(u"port", None)
        if port:
            broker_config["listeners"]["default"]["bind"] = f"0.0.0.0:{port}"

    def runConcurrentThread(self):
        self.logger.debug("RunConcurrentThread")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.logger.debug(f"Creating Broker with config = {broker_config}")
        broker = Broker(broker_config, loop)
        loop.run_until_complete(self.start_broker(broker))
        loop.run_until_complete(self.stop_broker(broker))

    async def start_broker(self, broker):
        await broker.start()

    async def stop_broker(self, broker):
        while True:
            await asyncio.sleep(1.0)
            if self.stopThread:
                await broker.shutdown()
                break

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
