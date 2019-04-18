# pingstat - A maubot plugin that aggregates ping statistics.
# Copyright (C) 2018 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Tuple, Iterable, NamedTuple, Optional
from pkg_resources import resource_string
from statistics import median
from aiohttp import web
from time import time
import json

from sqlalchemy import Table, Column, MetaData, String, Integer, BigInteger, and_
from jinja2 import Template

from mautrix.types import MessageType, RoomID, EventID

from maubot import Plugin, MessageEvent
from maubot.handlers import command

Pong = NamedTuple("Pong", room_id=RoomID, ping_id=EventID, pong_server=str, ping_server=str,
                  receive_diff=int, pong_timestamp=int)


class PingStatBot(Plugin):
    pong: Table
    stats_tpl: Template

    async def start(self):
        await super().start()
        metadata = MetaData()
        self.pong = Table("pongs", metadata,
                          Column("room_id", String, primary_key=True),
                          Column("ping_id", String, primary_key=True),
                          Column("pong_server", String, primary_key=True),
                          Column("ping_server", String),
                          Column("receive_diff", Integer),
                          Column("pong_timestamp", BigInteger))
        metadata.create_all(self.database)
        self.webapp.add_get("/stats", self.stats)
        self.webapp.add_get("/stats.json", self.stats_json)
        self.webapp.add_get("/stats.raw.json", self.stats_raw_json)
        self.stats_tpl = Template(resource_string("pingstat", "stats.html.j2").decode("utf-8"))

    def save_pong(self, pong: Pong) -> None:
        self.database.execute(self.pong.insert().values(
            room_id=pong.room_id, ping_id=pong.ping_id, pong_server=pong.pong_server,
            ping_server=pong.ping_server, receive_diff=pong.receive_diff,
            pong_timestamp=pong.pong_timestamp))

    def iter_pongs(self, room_id: RoomID, max_age: int = 7 * 24 * 60 * 60 * 1000) -> Iterable[Pong]:
        rows = self.database.execute(self.pong.select().where(and_(
            self.pong.c.room_id == room_id,
            self.pong.c.pong_timestamp >= int(time() * 1000) - max_age)))
        for row in rows:
            yield Pong(*row)

    @command.passive(r"^@.+:.+: Pong! \(ping took .+ to arrive\)$", msgtypes=(MessageType.NOTICE,))
    async def pong_handler(self, evt: MessageEvent, _: Tuple[str]) -> None:
        try:
            pong_data = evt.content["pong"]
        except KeyError:
            return

        try:
            diff = pong_data["ms"]
            server = pong_data["from"]
            ping = pong_data["ping"]
            to = evt.sender.split(":", 1)[1]
        except KeyError:
            self.log.warning(f"{evt.event_id} from {evt.sender} had invalid pong data!")
            return
        except IndexError:
            self.log.warning(f"{evt.event_id} from {evt.sender} had an invalid sender ID!")
            return

        pong = Pong(room_id=evt.room_id, ping_id=ping, pong_server=to, ping_server=server,
                    receive_diff=diff, pong_timestamp=evt.timestamp)
        self.save_pong(pong)
        await evt.mark_read()

    def get_room_data(self, room_id: RoomID) -> dict:
        data = {}
        pongservers = set()
        for pong in self.iter_pongs(room_id):
            pongservers.add(pong.pong_server)
            ping_server_data = data.setdefault(pong.ping_server,
                                               {"pongs": {}, "pings": set()})
            ping_server_data["pings"].add(pong.ping_id)
            pong_server_data = ping_server_data["pongs"].setdefault(pong.pong_server,
                                                                    {"sum": 0, "diffs": {}})
            pong_server_data["sum"] += pong.receive_diff
            pong_server_data["diffs"][pong.ping_id] = pong.receive_diff

        for ping_server in data.values():
            ping_server["pings"] = list(ping_server["pings"])
            total_sum = 0
            total_len = 0
            diffs = []
            for pong_server in ping_server["pongs"].values():
                total_sum += pong_server["sum"]
                total_len += len(pong_server["diffs"])
                pong_server["mean"] = pong_server["sum"] / len(pong_server["diffs"])
                server_diffs = pong_server["diffs"].values()
                pong_server["median"] = median(server_diffs)
                diffs += server_diffs
                del pong_server["sum"]
            ping_server["mean"] = total_sum / total_len
            ping_server["median"] = median(diffs)

        data = dict(sorted(data.items(), key=lambda kv: kv[1]["median"]))
        return {
            "pings": data,
            "mean": (sum(ping_server["mean"] for ping_server in data.values()) / len(data)
                     if len(data) > 0 else 0),
            "pongservers": sorted(list(pongservers))
        }

    @staticmethod
    def plural(num: float, unit: str, decimals: Optional[int] = None) -> str:
        num = round(num, decimals)
        if num == 1:
            return f"{num} {unit}"
        else:
            return f"{num} {unit}s"

    @classmethod
    def prettify_diff(cls, diff: int) -> str:
        if abs(diff) < 10 * 1_000:
            return f"{round(diff, 1)} ms"
        elif abs(diff) < 60 * 1_000:
            return cls.plural(diff / 1_000, 'second', decimals=1)
        minutes, seconds = divmod(diff / 1_000, 60)
        if abs(minutes) < 60:
            return f"{cls.plural(minutes, 'minute')} and {cls.plural(seconds, 'second')}"
        hours, minutes = divmod(minutes, 60)
        if abs(hours) < 24:
            return (f"{cls.plural(hours, 'hour')}, {cls.plural(minutes, 'minute')}"
                    f" and {cls.plural(seconds, 'second')}")
        days, hours = divmod(hours, 24)
        return (f"{cls.plural(days, 'day')}, {cls.plural(hours, 'hour')},"
                f"{cls.plural(minutes, 'minute')} and {cls.plural(seconds, 'second')}")

    async def stats(self, request: web.Request) -> web.Response:
        try:
            room_id = request.query["room_id"]
        except KeyError:
            return web.Response(status=400,
                                text="Room ID query param missing\n" + str(request.rel_url))
        data = self.get_room_data(room_id)
        n = 1
        for ping in data["pings"].values():
            ping["n"] = n
            n += 1
        return web.Response(status=200, content_type="text/html",
                            text=self.stats_tpl.render(**data, prettify_diff=self.prettify_diff))

    async def stats_raw_json(self, request: web.Request) -> web.Response:
        try:
            room_id = request.query["room_id"]
        except KeyError:
            return web.Response(status=400,
                                text="Room ID query param missing\n" + str(request.rel_url))
        try:
            max_age = int(request.query["max_age"]) * 1000
        except (KeyError, ValueError):
            max_age = 7 * 24 * 60 * 60 * 1000
        return web.Response(status=200, content_type="application/json",
                            text=json.dumps([pong._asdict() for pong in
                                             self.iter_pongs(room_id, max_age=max_age)]))

    async def stats_json(self, request: web.Request) -> web.Response:
        try:
            room_id = request.query["room_id"]
        except KeyError:
            return web.Response(status=400,
                                text="Room ID query param missing\n" + str(request.rel_url))
        data = self.get_room_data(room_id)
        return web.Response(status=200, content_type="application/json", text=json.dumps(data))
