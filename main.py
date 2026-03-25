from __future__ import annotations

from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.star import Context, Star, register

@register("astrbot_plugin_meetingsecretary", "IlkerUniye", "将指定数量群聊天记录以合订本形式转发出来，并且排除指定用户的发言", "1.0.0")
class MeetingSecretary(Star):
    def __init__(self, context: Context, config: Optional[AstrBotConfig] = None):
        super().__init__(context)
        self.config = config or {}
        self._history: Dict[str, Deque[_CachedMsg]] = defaultdict(lambda: deque(maxlen=2000))

    async def initialize(self):
        return

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def _record_message(self, event: AstrMessageEvent):
        session = event.unified_msg_origin
        message_chain = self._normalize_message_chain(
            getattr(event.message_obj, "message", None),
            str(event.message_str or ""),
        )
        msg = _CachedMsg(
            message_id=str(getattr(event.message_obj, "message_id", "")),
            sender_id=str(event.get_sender_id()),
            sender_name=str(event.get_sender_name()),
            message_str=str(event.message_str or ""),
            message_chain=message_chain,
            timestamp=int(getattr(event.message_obj, "timestamp", 0) or 0),
        )
        self._history[session].append(msg)

    @filter.command("meeting")
    async def meeting(self, event: AstrMessageEvent):
        """整理最近消息并以合并转发发送：/meeting <数量> [用户ID ...]"""
        count, blocked = self._parse_meeting_args(event.message_str)
        if count is None:
            yield event.plain_result("用法：/meeting <消息数量:int> [屏蔽用户ID ...]")
            return

        if count <= 0:
            yield event.plain_result("消息数量必须为正整数。")
            return
        max_count = self._get_max_meeting_count()
        if count > max_count:
            yield event.plain_result(f"消息数量不能大于配置上限：{max_count}。")
            return

        exclude_message_id = str(getattr(event.message_obj, "message_id", ""))

        msgs: List[_CachedMsg] = []
        try:
            msgs = await self._fetch_messages_onebot_first(event, count, blocked, exclude_message_id)
        except Exception as e:
            logger.warning(f"meeting 拉取历史失败，改用本地缓存：{e!r}")
            msgs = self._fetch_messages_from_cache(event.unified_msg_origin, count, blocked, exclude_message_id)

        if not msgs:
            yield event.plain_result("未找到可转发的消息。")
            return

        from astrbot.api.message_components import Node, Plain

        nodes: List[Node] = []
        for m in msgs:
            uin = self._safe_int(m.sender_id)
            content_chain = self._normalize_message_chain(m.message_chain, m.message_str)
            if not content_chain:
                content_text = m.message_str if m.message_str else "[空消息]"
                content_chain = [Plain(content_text)]
            nodes.append(Node(uin=uin, name=m.sender_name, content=content_chain))

        for chunk in self._chunk(nodes, 100):
            yield event.chain_result(self._wrap_nodes_for_forward(chunk))

    async def terminate(self):
        return

    def _parse_meeting_args(self, text: str) -> Tuple[Optional[int], Set[str]]:
        parts = [p for p in (text or "").strip().split() if p]
        if len(parts) < 2:
            return None, set()

        try:
            count = int(parts[1])
        except Exception:
            return None, set()

        blocked: Set[str] = set()
        for token in parts[2:]:
            for piece in token.replace("，", ",").split(","):
                piece = piece.strip()
                if piece:
                    blocked.add(piece)
        return count, blocked

    async def _fetch_messages_onebot_first(
        self,
        event: AstrMessageEvent,
        count: int,
        blocked: Set[str],
        exclude_message_id: str,
    ) -> List[_CachedMsg]:
        group_id = str(getattr(event.message_obj, "group_id", "") or "")
        if not group_id:
            return self._fetch_messages_from_cache(event.unified_msg_origin, count, blocked, exclude_message_id)

        raw = getattr(event.message_obj, "raw_message", None)
        seed_seq = self._extract_onebot_message_seq(raw)
        if seed_seq is None:
            return self._fetch_messages_from_cache(event.unified_msg_origin, count, blocked, exclude_message_id)

        pulled = await self._pull_onebot_group_history(
            event=event,
            group_id=self._safe_int(group_id),
            seed_seq=seed_seq,
            need=count,
            blocked=blocked,
            exclude_message_id=exclude_message_id,
        )
        if pulled:
            return pulled

        return self._fetch_messages_from_cache(event.unified_msg_origin, count, blocked, exclude_message_id)

    async def _pull_onebot_group_history(
        self,
        event: AstrMessageEvent,
        group_id: int,
        seed_seq: int,
        need: int,
        blocked: Set[str],
        exclude_message_id: str,
    ) -> List[_CachedMsg]:
        seen: Set[str] = set()
        out: List[_CachedMsg] = []
        next_seq = int(seed_seq)

        while len(out) < need and next_seq > 0:
            resp = await event.bot.call_action(
                "get_group_msg_history",
                **{"group_id": group_id, "message_seq": next_seq},
            )
            msgs = self._extract_onebot_messages_list(resp)
            if not msgs:
                break

            min_seq = None
            for m in msgs:
                mid = str(m.get("message_id", "") or "")
                if not mid or mid == exclude_message_id or mid in seen:
                    continue
                seen.add(mid)

                sender = m.get("sender") or {}
                sender_id = str(sender.get("user_id", "") or "")
                if sender_id and sender_id in blocked:
                    continue

                sender_name = str(sender.get("card") or sender.get("nickname") or sender_id or "unknown")
                timestamp = int(m.get("time", 0) or 0)
                message_chain = self._onebot_message_to_chain(m.get("message"))
                msg_text = self._onebot_message_to_text(m.get("message"))

                out.append(
                    _CachedMsg(
                        message_id=mid,
                        sender_id=sender_id or "0",
                        sender_name=sender_name,
                        message_str=msg_text,
                        message_chain=message_chain,
                        timestamp=timestamp,
                    )
                )

                mseq = self._safe_int(m.get("message_seq"))
                if mseq > 0:
                    min_seq = mseq if min_seq is None else min(min_seq, mseq)

            if min_seq is None:
                break

            if min_seq >= next_seq:
                next_seq = min_seq - 1
            else:
                next_seq = min_seq - 1

        out.sort(key=lambda x: (x.timestamp, x.message_id))
        if len(out) > need:
            out = out[-need:]
        return out

    def _fetch_messages_from_cache(
        self,
        session: str,
        count: int,
        blocked: Set[str],
        exclude_message_id: str,
    ) -> List[_CachedMsg]:
        buf = list(self._history.get(session, []))
        out: List[_CachedMsg] = []
        for m in reversed(buf):
            if len(out) >= count:
                break
            if m.message_id == exclude_message_id:
                continue
            if m.sender_id in blocked:
                continue
            out.append(m)
        out.reverse()
        return out

    def _extract_onebot_message_seq(self, raw: object) -> Optional[int]:
        if isinstance(raw, dict):
            for k in ("message_seq", "messageSeq", "seq", "messageSequence"):
                v = raw.get(k)
                iv = self._safe_int(v)
                if iv > 0:
                    return iv
        return None

    def _extract_onebot_messages_list(self, resp: object) -> List[dict]:
        if isinstance(resp, dict):
            payload = resp.get("data", resp)
            if isinstance(payload, dict):
                msgs = payload.get("messages")
                if isinstance(msgs, list):
                    return [m for m in msgs if isinstance(m, dict)]
            if isinstance(payload, list):
                return [m for m in payload if isinstance(m, dict)]
        return []

    def _onebot_message_to_chain(self, message: object) -> List[object]:
        import astrbot.api.message_components as Comp

        if isinstance(message, str):
            return [Comp.Plain(message)] if message else []
        if not isinstance(message, list):
            text = str(message).strip()
            return [Comp.Plain(text)] if text else []

        out: List[object] = []
        for seg in message:
            if not isinstance(seg, dict):
                continue
            t = str(seg.get("type", "") or "")
            data = seg.get("data") or {}
            if not isinstance(data, dict):
                data = {}

            if t in ("text", "plain"):
                text = str(data.get("text", "") or "")
                if text:
                    out.append(Comp.Plain(text))
            elif t == "at":
                qq = str(data.get("qq", "") or "")
                if qq:
                    try:
                        out.append(Comp.At(qq=int(qq)))
                    except Exception:
                        out.append(Comp.At(qq=qq))
                else:
                    out.append(Comp.Plain("@"))
            elif t == "image":
                img = data.get("url") or data.get("file") or data.get("path")
                if img:
                    try:
                        out.append(Comp.Image.fromURL(str(img)))
                    except Exception:
                        try:
                            out.append(Comp.Image(file=str(img)))
                        except Exception:
                            out.append(Comp.Plain(str(img)))
                else:
                    out.append(Comp.Plain("[图片]"))
            elif t == "video":
                video = data.get("url") or data.get("file") or data.get("path")
                if video:
                    try:
                        out.append(Comp.Video.fromURL(str(video)))
                    except Exception:
                        try:
                            out.append(Comp.Video(file=str(video)))
                        except Exception:
                            out.append(Comp.Plain(f"[视频]{video}"))
                else:
                    out.append(Comp.Plain("[视频]"))
            elif t == "file":
                file_path = data.get("path") or data.get("file")
                file_name = str(data.get("name", "") or "")
                if file_path:
                    try:
                        out.append(Comp.File(file=str(file_path), name=file_name or str(file_path)))
                    except Exception:
                        out.append(Comp.Plain(f"[文件]{file_name or str(file_path)}"))
                else:
                    file_url = data.get("url")
                    if file_url:
                        out.append(Comp.Plain(f"[文件]{file_name or file_url}"))
                    else:
                        out.append(Comp.Plain("[文件]"))
            elif t == "record":
                out.append(Comp.Plain("[语音]"))
            elif t == "face":
                face_id = data.get("id")
                if face_id is not None:
                    try:
                        out.append(Comp.Face(id=int(face_id)))
                    except Exception:
                        out.append(Comp.Plain("[表情]"))
                else:
                    out.append(Comp.Plain("[表情]"))
            elif t == "reply":
                rid = data.get("id")
                if rid:
                    try:
                        out.append(Comp.Reply(id=int(rid)))
                    except Exception:
                        out.append(Comp.Plain("[回复]"))
                else:
                    out.append(Comp.Plain("[回复]"))
            elif t == "forward":
                fid = data.get("id")
                if fid:
                    try:
                        out.append(Comp.Forward(id=str(fid)))
                    except Exception:
                        out.append(Comp.Plain("[合并转发]"))
                else:
                    out.append(Comp.Plain("[合并转发]"))
            else:
                out.append(Comp.Plain(f"[{t}]"))
        return out

    def _onebot_message_to_text(self, message: object) -> str:
        if isinstance(message, str):
            return message.strip()
        if not isinstance(message, list):
            return str(message).strip()

        parts: List[str] = []
        for seg in message:
            if not isinstance(seg, dict):
                continue
            t = str(seg.get("type", "") or "")
            data = seg.get("data") or {}
            if t in ("text", "plain"):
                parts.append(str(data.get("text", "") or ""))
            elif t == "at":
                qq = str(data.get("qq", "") or "")
                parts.append(f"@{qq}" if qq else "@")
            elif t == "image":
                parts.append("[图片]")
            elif t == "face":
                parts.append("[表情]")
            elif t == "reply":
                parts.append("[回复]")
            elif t == "record":
                parts.append("[语音]")
            elif t == "video":
                parts.append("[视频]")
            elif t == "file":
                parts.append("[文件]")
            else:
                parts.append(f"[{t}]")
        return "".join(parts).strip()

    def _normalize_message_chain(self, message: object, fallback_text: str = "") -> List[object]:
        out: List[object] = []
        if isinstance(message, list):
            for comp in message:
                if comp is None:
                    continue
                out.append(deepcopy(comp))
        if out:
            return out

        text = str(fallback_text or "").strip()
        if text:
            import astrbot.api.message_components as Comp

            return [Comp.Plain(text)]
        return []

    def _wrap_nodes_for_forward(self, nodes: List[object]) -> List[object]:
        import astrbot.api.message_components as Comp

        builders = (
            lambda: Comp.Nodes(nodes=nodes),
            lambda: Comp.Nodes(node_list=nodes),
            lambda: Comp.Nodes(nodes),
            lambda: Comp.Forward(nodes=nodes),
            lambda: Comp.Forward(node_list=nodes),
            lambda: Comp.Forward(nodes),
        )
        for builder in builders:
            try:
                comp = builder()
                if comp is not None:
                    return [comp]
            except Exception:
                continue
        return nodes

    def _safe_int(self, v: object) -> int:
        try:
            if isinstance(v, bool):
                return 0
            if v is None:
                return 0
            s = str(v).strip()
            if not s:
                return 0
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                return int(s)
            return int(float(s))
        except Exception:
            return 0

    def _get_max_meeting_count(self) -> int:
        max_count = self._safe_int(self.config.get("max_meeting_count", 1000))
        if max_count <= 0:
            return 1000
        return max_count

    def _chunk(self, items: List, size: int) -> Iterable[List]:
        for i in range(0, len(items), size):
            yield items[i : i + size]


@dataclass(frozen=True)
class _CachedMsg:
    message_id: str
    sender_id: str
    sender_name: str
    message_str: str
    message_chain: List[object]
    timestamp: int
