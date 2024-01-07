#! /usr/bin/env python
# -*- coding: utf-8 -*-
####################

import logging
import threading
import asyncio
from amqtt.broker import Broker
import indigo # noqa

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
        "plugins": ["auth_anonymous", "auth_user"],
        "auth_user_name": "user",
        "auth_user_password": "password",
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

        log_format = logging.Formatter('%(asctime)s.%(msecs)03d\t[%(levelname)8s] %(name)20s.%(funcName)-25s%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.plugin_file_handler.setFormatter(log_format)
        self.logLevel = int(pluginPrefs.get("logLevel", logging.INFO))
        self.indigo_log_handler.setLevel(self.logLevel)
        self.logger.debug(f"MQTT Connector: logLevel = {self.logLevel}")

        self._event_loop = None
        self._async_thread = None

        # override the default config with the plugin prefs

        host = pluginPrefs.get("host", "0.0.0.0")
        port = pluginPrefs.get("port", "1883")
        broker_config["listeners"]["default"]["bind"] = f"{host}:{port}"

        broker_config["auth"]["auth_user_name"] = pluginPrefs.get("username", None)
        broker_config["auth"]["auth_user_password"] = pluginPrefs.get("password", None)

        self.logger.debug(f"Creating Broker with config = {broker_config}")
        self.broker = Broker(broker_config, self._event_loop)

    def closedPrefsConfigUi(self, valuesDict, userCancelled):
        if not userCancelled:
            self.logLevel = int(valuesDict.get("logLevel", logging.INFO))
            self.indigo_log_handler.setLevel(self.logLevel)

    def startup(self):  # noqa
        self.logger.info(f"MQTT Broker starting")
        self._async_thread = threading.Thread(target=self.run_async_thread)
        self._async_thread.start()

    def shutdown(self):  # noqa
        self.logger.info(f"MQTT Broker stopping")
        self._async_thread.join()

#   Async Methods

    def run_async_thread(self):
        self.logger.debug("run_async_thread starting")

        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)
        self._event_loop.create_task(self._async_start())
        self._event_loop.run_until_complete(self._async_stop())

        self.logger.debug("run_async_thread ending")

    async def _async_start(self):
        self.logger.debug("_async_start")
        await self.broker.start()

    async def _async_stop(self):
        self.logger.debug("_async_stop")
        while True:
            await asyncio.sleep(1.0)
            if self.stopThread:
                await self.broker.shutdown()
                break
