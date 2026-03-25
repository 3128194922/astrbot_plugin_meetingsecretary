from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import dataclass
import time

from astrbot.api import AstrBotConfig, logger
import astrbot.api.message_components as Comp
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

@register("astrbot_plugin_meetingsecretary", "IlkerUniye", "将指定数量群聊天记录以合订本形式转发出来，并且排除指定用户的发言", "1.0.0")
class MeetingSecretary(Star):
    def __init__(self, context: Context, config: AstrBotConfig | None = None):
        super().__init__(context)
        self.config = config or {}
        self._history: dict[str, deque[_CachedMsg]] = {}
        self._session_last_seen: dict[str, int] = {}
        self._history_ttl_seconds = max(self._safe_int(self.config.get("history_ttl_seconds", 604800)), 60)
        self._history_max_sessions = max(self._safe_int(self.config.get("history_max_sessions", 1000)), 1)

    async def initialize(self):
        return

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def _record_message(self, event: AstrMessageEvent):
        self._cleanup_history()
        session = event.unified_msg_origin
        self._touch_session(session)
        message_chain = self._normalize_message_chain(
            getattr(event.message_obj, "message", None),
            str(event.message_str or ""),
        )
        message_seq = self._extract_onebot_message_seq(getattr(event.message_obj, "raw_message", None))
        if message_seq is None:
            message_seq = self._extract_onebot_message_seq(event.message_obj)
        msg = _CachedMsg(
            message_id=str(getattr(event.message_obj, "message_id", "")),
            sender_id=str(event.get_sender_id()),
            sender_name=str(event.get_sender_name()),
            message_str=str(event.message_str or ""),
            message_chain=message_chain,
            timestamp=int(getattr(event.message_obj, "timestamp", 0) or 0),
            message_seq=message_seq or 0,
        )
        if session not in self._history:
            self._history[session] = deque(maxlen=2000)
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

        msgs: list[_CachedMsg] = []
        try:
            msgs = await self._fetch_messages_onebot_first(event, count, blocked, exclude_message_id)
        except (RuntimeError, ValueError, TypeError) as e:
            logger.warning(f"meeting 拉取历史失败，改用本地缓存：{e!r}")
            msgs = self._fetch_messages_from_cache(event.unified_msg_origin, count, blocked, exclude_message_id)

        if not msgs:
            yield event.plain_result("未找到可转发的消息。")
            return

        nodes: list[object] = []
        for m in msgs:
            uin = self._safe_int(m.sender_id)
            content_chain = self._normalize_message_chain(m.message_chain, m.message_str)
            content_chain = self._sanitize_forward_content(content_chain, m.message_str)
            nodes.append(Comp.Node(uin=uin, name=m.sender_name, content=content_chain))

        for chunk in self._chunk(nodes, 100):
            yield event.chain_result([Comp.Nodes(nodes=chunk)])

    async def terminate(self):
        return

    def _parse_meeting_args(self, text: str) -> tuple[int | None, set[str]]:
        parts = [p for p in (text or "").strip().split() if p]
        if len(parts) < 2:
            return None, set()

        try:
            count = int(parts[1])
        except (ValueError, TypeError):
            return None, set()

        blocked: set[str] = set()
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
        blocked: set[str],
        exclude_message_id: str,
    ) -> list[_CachedMsg]:
        group_id = str(getattr(event.message_obj, "group_id", "") or "")
        if not group_id:
            return self._fetch_messages_from_cache(event.unified_msg_origin, count, blocked, exclude_message_id)

        seed_seq = self._extract_onebot_message_seq(getattr(event.message_obj, "raw_message", None))
        if seed_seq is None:
            seed_seq = self._extract_onebot_message_seq(event.message_obj)
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
        blocked: set[str],
        exclude_message_id: str,
    ) -> list[_CachedMsg]:
        seen: set[str] = set()
        out: list[_CachedMsg] = []
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
                mseq = self._safe_int(m.get("message_seq"))
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
                        message_seq=mseq,
                    )
                )

                if mseq > 0:
                    min_seq = mseq if min_seq is None else min(min_seq, mseq)

            if min_seq is None:
                break

            next_seq = min_seq - 1

        out.sort(key=lambda x: (x.message_seq <= 0, x.message_seq if x.message_seq > 0 else x.timestamp, x.message_id))
        if len(out) > need:
            out = out[-need:]
        return out

    def _fetch_messages_from_cache(
        self,
        session: str,
        count: int,
        blocked: set[str],
        exclude_message_id: str,
    ) -> list[_CachedMsg]:
        self._cleanup_history()
        self._touch_session(session)
        buf = list(self._history.get(session, []))
        out: list[_CachedMsg] = []
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

    def _extract_onebot_message_seq(self, raw: object) -> int | None:
        if isinstance(raw, dict):
            for k in ("message_seq", "messageSeq", "seq", "messageSequence"):
                v = raw.get(k)
                iv = self._safe_int(v)
                if iv > 0:
                    return iv
        for k in ("message_seq", "messageSeq", "seq", "messageSequence"):
            v = getattr(raw, k, None)
            iv = self._safe_int(v)
            if iv > 0:
                return iv
        return None

    def _extract_onebot_messages_list(self, resp: object) -> list[dict]:
        if isinstance(resp, dict):
            payload = resp.get("data", resp)
            if isinstance(payload, dict):
                msgs = payload.get("messages")
                if isinstance(msgs, list):
                    return [m for m in msgs if isinstance(m, dict)]
            if isinstance(payload, list):
                return [m for m in payload if isinstance(m, dict)]
        return []

    def _onebot_message_to_chain(self, message: object) -> list[object]:
        if isinstance(message, str):
            return [Comp.Plain(message)] if message else []
        if not isinstance(message, list):
            text = str(message).strip()
            return [Comp.Plain(text)] if text else []

        handlers = {
            "text": self._build_text_segment,
            "plain": self._build_text_segment,
            "at": self._build_at_segment,
            "image": self._build_image_segment,
            "video": self._build_video_segment,
            "file": self._build_file_segment,
            "record": self._build_record_segment,
            "face": self._build_face_segment,
            "reply": self._build_reply_segment,
            "forward": self._build_forward_segment,
        }
        out: list[object] = []
        for seg in message:
            if not isinstance(seg, dict):
                continue
            t = str(seg.get("type", "") or "")
            data = seg.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            handler = handlers.get(t)
            try:
                comp = handler(data) if handler else Comp.Plain(f"[{t}]")
            except (ValueError, TypeError, OSError):
                comp = Comp.Plain(f"[{t}]")
            if comp is not None:
                out.append(comp)
        return out

    def _onebot_message_to_text(self, message: object) -> str:
        if isinstance(message, str):
            return message.strip()
        if not isinstance(message, list):
            return str(message).strip()

        handlers = {
            "text": self._text_from_text_segment,
            "plain": self._text_from_text_segment,
            "at": self._text_from_at_segment,
            "poke": self._text_from_poke_segment,
            "image": self._text_fixed_image,
            "face": self._text_fixed_face,
            "reply": self._text_fixed_reply,
            "record": self._text_fixed_record,
            "video": self._text_fixed_video,
            "file": self._text_fixed_file,
        }
        parts: list[str] = []
        for seg in message:
            if not isinstance(seg, dict):
                continue
            t = str(seg.get("type", "") or "")
            data = seg.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            handler = handlers.get(t)
            parts.append(handler(data) if handler else f"[{t}]")
        return "".join(parts).strip()

    def _normalize_message_chain(self, message: object, fallback_text: str = "") -> list[object]:
        out: list[object] = []
        if isinstance(message, list):
            for comp in message:
                if comp is None:
                    continue
                out.append(deepcopy(comp))
        if out:
            return out

        text = str(fallback_text or "").strip()
        if text:
            return [Comp.Plain(text)]
        return []

    def _sanitize_forward_content(self, chain: list[object], fallback_text: str = "") -> list[object]:
        out: list[object] = []
        for comp in chain:
            safe_comp = self._sanitize_forward_component(comp)
            if safe_comp is None:
                continue
            if isinstance(safe_comp, list):
                out.extend(safe_comp)
            else:
                out.append(safe_comp)
        if out:
            return out
        text = str(fallback_text or "").strip()
        return [Comp.Plain(text if text else "[空消息]")]

    def _sanitize_forward_component(self, comp: object) -> object | list[object] | None:
        if isinstance(comp, dict):
            t = str(comp.get("type", "") or "").lower()
            data = comp.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            dict_handlers = {
                "text": self._sanitize_dict_text,
                "plain": self._sanitize_dict_text,
                "image": self._sanitize_dict_image,
                "video": self._sanitize_dict_video,
                "file": self._sanitize_dict_file,
                "record": self._sanitize_dict_record,
                "face": self._sanitize_dict_face,
                "reply": self._sanitize_dict_reply,
                "at": self._sanitize_dict_at,
                "poke": self._sanitize_dict_poke,
            }
            handler = dict_handlers.get(t)
            if handler:
                return handler(data)
            return self._sanitize_unknown_type(t)

        cname = comp.__class__.__name__
        class_handlers = {
            "Plain": self._sanitize_class_clone,
            "Image": self._sanitize_class_clone,
            "Video": self._sanitize_class_clone,
            "File": self._sanitize_class_clone,
            "Face": self._sanitize_class_clone,
            "Record": self._sanitize_class_record,
            "At": self._sanitize_class_at,
            "Reply": self._sanitize_class_reply,
            "Poke": self._sanitize_class_poke,
        }
        handler = class_handlers.get(cname)
        if handler:
            return handler(comp)
        return self._sanitize_unknown_type(cname)

    def _text_from_text_segment(self, data: dict[str, object]) -> str:
        return str(data.get("text", "") or "")

    def _text_from_at_segment(self, data: dict[str, object]) -> str:
        display_name = str(data.get("name") or data.get("nickname") or "").strip()
        if display_name:
            return f"@{display_name}"
        qq = str(data.get("qq", "") or "")
        return f"@{qq}" if qq else "@"

    def _text_fixed_image(self, data: dict[str, object]) -> str:
        return "[图片]"

    def _text_fixed_face(self, data: dict[str, object]) -> str:
        return "[表情]"

    def _text_fixed_reply(self, data: dict[str, object]) -> str:
        return "[回复]"

    def _text_fixed_record(self, data: dict[str, object]) -> str:
        return "[语音]"

    def _text_fixed_video(self, data: dict[str, object]) -> str:
        return "[视频]"

    def _text_fixed_file(self, data: dict[str, object]) -> str:
        return "[文件]"

    def _text_from_poke_segment(self, data: dict[str, object]) -> str:
        return "[戳一戳]"

    def _sanitize_dict_text(self, data: dict[str, object]) -> object:
        return Comp.Plain(str(data.get("text", "") or ""))

    def _sanitize_dict_image(self, data: dict[str, object]) -> object:
        image = self._build_image_segment(data)
        return image if image.__class__.__name__ == "Image" else Comp.Plain("[图片]")

    def _sanitize_dict_video(self, data: dict[str, object]) -> object:
        video = self._build_video_segment(data)
        return video if video.__class__.__name__ == "Video" else Comp.Plain("[视频]")

    def _sanitize_dict_file(self, data: dict[str, object]) -> object:
        file_comp = self._build_file_segment(data)
        return file_comp if file_comp.__class__.__name__ == "File" else Comp.Plain("[文件]")

    def _sanitize_dict_record(self, data: dict[str, object]) -> object:
        return Comp.Plain("[语音]")

    def _sanitize_dict_face(self, data: dict[str, object]) -> object:
        face = self._build_face_segment(data)
        return face if face.__class__.__name__ == "Face" else Comp.Plain("[表情]")

    def _sanitize_dict_reply(self, data: dict[str, object]) -> object:
        rid = self._safe_int(data.get("id"))
        return Comp.Reply(id=rid) if rid > 0 else Comp.Plain("[回复]")

    def _sanitize_dict_at(self, data: dict[str, object]) -> object:
        display_name = str(data.get("name") or data.get("nickname") or "").strip()
        if display_name:
            return Comp.Plain(f"@{display_name}")
        qq = str(data.get("qq", "") or "")
        return Comp.Plain(f"@{qq}" if qq else "@")

    def _sanitize_dict_poke(self, data: dict[str, object]) -> object:
        return Comp.Plain("[戳一戳]")

    def _sanitize_class_clone(self, comp: object) -> object:
        return deepcopy(comp)

    def _sanitize_class_record(self, comp: object) -> object:
        return Comp.Plain("[语音]")

    def _sanitize_class_at(self, comp: object) -> object:
        display_name = str(getattr(comp, "name", "") or "").strip()
        if display_name:
            return Comp.Plain(f"@{display_name}")
        qq = str(getattr(comp, "qq", "") or "")
        return Comp.Plain(f"@{qq}" if qq else "@")

    def _sanitize_class_reply(self, comp: object) -> object:
        rid = self._safe_int(getattr(comp, "id", 0))
        return Comp.Reply(id=rid) if rid > 0 else Comp.Plain("[回复]")

    def _sanitize_class_poke(self, comp: object) -> object:
        return Comp.Plain("[戳一戳]")

    def _sanitize_unknown_type(self, t: str) -> object:
        tips = {
            "forward": "[合并转发]",
            "poke": "[戳一戳]",
            "Forward": "[合并转发]",
            "Poke": "[戳一戳]",
        }
        return Comp.Plain(tips.get(t, f"[{t or '消息'}]"))

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
        except (ValueError, TypeError, OverflowError):
            return 0

    def _get_max_meeting_count(self) -> int:
        max_count = self._safe_int(self.config.get("max_meeting_count", 1000))
        if max_count <= 0:
            return 1000
        return max_count

    def _chunk(self, items: list[object], size: int) -> list[list[object]]:
        chunks: list[list[object]] = []
        for i in range(0, len(items), size):
            chunks.append(items[i : i + size])
        return chunks

    def _touch_session(self, session: str):
        if session:
            self._session_last_seen[session] = int(time.time())

    def _cleanup_history(self):
        now = int(time.time())
        expired = [s for s, ts in self._session_last_seen.items() if now - ts > self._history_ttl_seconds]
        for s in expired:
            self._session_last_seen.pop(s, None)
            self._history.pop(s, None)
        if len(self._session_last_seen) <= self._history_max_sessions:
            return
        ordered = sorted(self._session_last_seen.items(), key=lambda x: x[1])
        for s, _ in ordered[: len(self._session_last_seen) - self._history_max_sessions]:
            self._session_last_seen.pop(s, None)
            self._history.pop(s, None)

    def _build_text_segment(self, data: dict[str, object]) -> object | None:
        text = str(data.get("text", "") or "")
        return Comp.Plain(text) if text else None

    def _build_at_segment(self, data: dict[str, object]) -> object:
        qq = str(data.get("qq", "") or "")
        name = str(data.get("name") or data.get("nickname") or "").strip()
        iv = self._safe_int(qq)
        if iv <= 0:
            return Comp.Plain("@")
        if not name:
            return Comp.At(qq=iv)
        try:
            return Comp.At(qq=iv, name=name)
        except TypeError:
            return Comp.At(qq=iv)

    def _build_image_segment(self, data: dict[str, object]) -> object:
        img = data.get("url") or data.get("file") or data.get("path")
        if not img:
            return Comp.Plain("[图片]")
        return Comp.Image.fromURL(str(img))

    def _build_video_segment(self, data: dict[str, object]) -> object:
        video = data.get("url") or data.get("file") or data.get("path")
        if not video:
            return Comp.Plain("[视频]")
        return Comp.Video.fromURL(str(video))

    def _build_file_segment(self, data: dict[str, object]) -> object:
        file_path = data.get("path") or data.get("file")
        file_name = str(data.get("name", "") or "")
        if file_path:
            return Comp.File(file=str(file_path), name=file_name or str(file_path))
        file_url = data.get("url")
        if file_url:
            return Comp.Plain(f"[文件]{file_name or file_url}")
        return Comp.Plain("[文件]")

    def _build_record_segment(self, data: dict[str, object]) -> object:
        return Comp.Plain("[语音]")

    def _build_face_segment(self, data: dict[str, object]) -> object:
        face_id = self._safe_int(data.get("id"))
        return Comp.Face(id=face_id) if face_id > 0 else Comp.Plain("[表情]")

    def _build_reply_segment(self, data: dict[str, object]) -> object:
        rid = self._safe_int(data.get("id"))
        return Comp.Reply(id=rid) if rid > 0 else Comp.Plain("[回复]")

    def _build_forward_segment(self, data: dict[str, object]) -> object:
        fid = data.get("id")
        return Comp.Forward(id=str(fid)) if fid else Comp.Plain("[合并转发]")


@dataclass(frozen=True)
class _CachedMsg:
    message_id: str
    sender_id: str
    sender_name: str
    message_str: str
    message_chain: list[object]
    timestamp: int
    message_seq: int
