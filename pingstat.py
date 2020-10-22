# pingstat - A maubot plugin that aggregates ping statistics.
# Copyright (C) 2019 Tulir Asokan
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
from typing import Tuple, Iterable, Dict, Sequence, NamedTuple, Optional
from pkg_resources import resource_string
from statistics import median
from time import time
import json
import math

from aiohttp.web import Request, Response
from sqlalchemy import Table, Column, MetaData, String, Integer, BigInteger, and_
from jinja2 import Template

from mautrix.types import MessageType, RoomID, EventID, EventType, StateEvent

from maubot import Plugin, MessageEvent
from maubot.handlers import command, web, event

Pong = NamedTuple("Pong", room_id=RoomID, ping_id=EventID, pong_server=str, ping_server=str,
                  receive_diff=int, pong_timestamp=int)

SECOND = 1000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR
WEEK = 7 * DAY


# TODO make configurable?
default_disclaimer = "Please note that ping times may not be representative of homeserver performance."
disclaimers = {
  "!ping-no-synapse:maunium.net": f"{default_disclaimer} This room includes work-in-progress homeserver implementations, which may implement some parts of Matrix in way that is faster, but not spec-compliant and could cause other problems."
}


class PingStatBot(Plugin):
    stats_tpl: Template = Template(resource_string("pingstat", "stats.html.j2").decode("utf-8"))
    pong: Table

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

    # region Creating pong data

    def save_pong(self, pong: Pong) -> None:
        self.database.execute(self.pong.insert().values(
            room_id=pong.room_id, ping_id=pong.ping_id, pong_server=pong.pong_server,
            ping_server=pong.ping_server, receive_diff=pong.receive_diff,
            pong_timestamp=pong.pong_timestamp))

    @command.passive(r"^@.+:.+: Pong! \(ping (\".+\" )?took .+ to arrive\)$",
                     msgtypes=(MessageType.NOTICE,))
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

    @event.on(EventType.ROOM_TOMBSTONE)
    async def tombstone(self, evt: StateEvent) -> None:
        if not evt.content.replacement_room:
            return
        self.database.execute(self.pong.update()
                              .where(self.pong.c.room_id == evt.room_id)
                              .values(room_id=evt.content.replacement_room))

    # endregion
    # region Getting pong data

    def iter_pongs(self, room_id: RoomID, max_age: int = WEEK, min_age: int = 0,
                   max_diff: int = 10 * MINUTE, min_diff: int = 10) -> Iterable[Pong]:
        now = int(time() * 1000)
        rows = self.database.execute(self.pong.select().where(and_(
            self.pong.c.room_id == room_id,
            self.pong.c.pong_timestamp <= now - min_age,
            self.pong.c.pong_timestamp >= now - max_age,
            self.pong.c.receive_diff <= max_diff,
            self.pong.c.receive_diff >= min_diff)))
        for row in rows:
            yield Pong(*row)

    @staticmethod
    def geomean(data: Sequence[int]) -> float:
        return math.exp(math.fsum(math.log(elem) for elem in data) / len(data))

    def get_room_data(self, room_id: RoomID, min_age: int, max_age: int) -> dict:
        data = {}
        pongservers = set()
        for pong in self.iter_pongs(room_id, min_age=min_age, max_age=max_age):
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
                pong_server["gmean"] = self.geomean(server_diffs)
                diffs += server_diffs
                del pong_server["sum"]
            ping_server["mean"] = total_sum / total_len
            ping_server["median"] = median(diffs)
            ping_server["gmean"] = self.geomean(diffs)

        data = dict(sorted(data.items(), key=lambda kv: kv[1]["median"]))
        return {
            "disclaimer": disclaimers.get(room_id, default_disclaimer),
            "pings": data,
            "mean": (sum(ping_server["mean"] for ping_server in data.values()) / len(data)
                     if len(data) > 0 else 0),
            "pongservers": sorted(list(pongservers))
        }

    # endregion
    # region Rendering helpers

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

    # endregion
    # region HTTP endpoints

    @staticmethod
    def _get_min_max_age(request: Request, max_delta: int = WEEK) -> Dict[str, int]:
        try:
            max_age = int(request.query["max_age"]) * 1000
        except (KeyError, ValueError):
            max_age = WEEK
        try:
            min_age = int(request.query["min_age"]) * 1000
        except (KeyError, ValueError):
            min_age = 0
        if 0 < max_delta < max_age - min_age:
            max_age = min_age + max_delta
        return {
            "min_age": min_age,
            "max_age": max_age,
        }

    @web.get("/{room_id}/stats")
    async def stats(self, request: Request, force_json: bool = False) -> Response:
        try:
            room_id = RoomID(request.match_info["room_id"])
        except KeyError:
            return Response(status=404, text="Room ID missing")
        data = self.get_room_data(room_id, **self._get_min_max_age(request))
        if "application/json" in request.headers.get("Accept", "") or force_json:
            return Response(status=200, content_type="application/json", text=json.dumps(data))
        n = 1
        for ping in data["pings"].values():
            ping["n"] = n
            n += 1
        return Response(status=200, content_type="text/html",
                        text=self.stats_tpl.render(**data, prettify_diff=self.prettify_diff))

    @web.get("/{room_id}/stats.json")
    async def stats_json(self, request: Request) -> Response:
        return await self.stats(request, force_json=True)

    @web.get("/{room_id}/stats.raw.json")
    async def stats_raw_json(self, request: Request) -> Response:
        try:
            room_id = RoomID(request.match_info["room_id"])
        except KeyError:
            return Response(status=404, text="Room ID missing")
        return Response(status=200, content_type="application/json",
                        text=json.dumps([pong._asdict() for pong in self.iter_pongs(
                            room_id, **self._get_min_max_age(request, 0))]))

    # region Legacy endpoints

    @web.get("/stats")
    async def old_stats(self, request: Request, fmt: str = "") -> Response:
        try:
            room_id = request.query["room_id"]
        except KeyError:
            return Response(status=400, text="Room ID query param missing")
        return Response(status=302, headers={"Location": f"{room_id}/stats{fmt}"})

    @web.get("/stats.json")
    async def old_stats_json(self, request: Request) -> Response:
        return await self.old_stats(request, fmt=".json")

    @web.get("/stats.raw.json")
    async def old_stats_raw_json(self, request: Request) -> Response:
        return await self.old_stats(request, fmt=".raw.json")

    # endregion
    # endregion
