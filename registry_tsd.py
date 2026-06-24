#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BoxID-ТТН Windows — современный плоский дизайн без артефактов.
Логика и API не изменены.

Запуск:
    python tracking_windows_app.py
"""

from __future__ import annotations

import json
import os
import queue
import re
import sqlite3
import threading
import traceback
import tkinter as tk
import urllib.error
import urllib.request
import tkinter.font  # noqa
from datetime import datetime, timezone
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable, Optional

APP_NAME = "BoxID-ТТН"
API_BASE_URL = "https://tracking-app.dclink.ua"
REQUEST_TIMEOUT = 8
SYNC_INTERVAL_MS = 20_000
SCAN_BUFFER_MS = 180

ROLE_LABELS = {"admin": "Адмін", "operator": "Оператор", "viewer": "Перегляд"}
ROLE_LEVELS = {"admin": 1, "operator": 0, "viewer": 2}

FONT = "Segoe UI"


# ───────────────────────── Палитра ─────────────────────────
class C:
    bg = "#0E1726"          # основной тёмный фон
    bg2 = "#162236"         # панели
    card = "#FFFFFF"
    card2 = "#F6F8FC"
    border = "#E3E9F2"

    primary = "#2F6BFF"
    primary_h = "#1F57E6"
    green = "#16B981"
    green_h = "#0EA371"
    amber = "#F5A623"
    amber_h = "#DC9214"
    red = "#EF4444"
    red_h = "#D63A3A"
    slate = "#475569"
    slate_h = "#374151"

    text = "#0F1B2D"
    text_soft = "#64748B"
    on_dark = "#E8EEF8"
    on_dark_soft = "#9DB0CC"
    white = "#FFFFFF"


def app_dir() -> Path:
    base = os.environ.get("APPDATA") or str(Path.home())
    path = Path(base) / "BoxID_TTN_Windows"
    path.mkdir(parents=True, exist_ok=True)
    return path


SETTINGS_FILE = app_dir() / "settings.json"
DB_FILE = app_dir() / "offline_queue.sqlite3"


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


def only_digits(value: str) -> str:
    return re.sub(r"\D+", "", value or "")


def parse_dt(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.astimezone()
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return text


def beep_success(root: tk.Tk) -> None:
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_OK)
    except Exception:
        root.bell()


def beep_error(root: tk.Tk) -> None:
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_ICONHAND)
    except Exception:
        root.bell()


# ───────────────────────── Скруглённая кнопка (Canvas, без артефактов) ─────────────────────────
class RoundButton(tk.Canvas):
    def __init__(self, parent, text, command, *, bg, fg=C.white, hover=None,
                 font_size=15, height=46, radius=12, pad_x=22, width=None,
                 bold=True):
        self.parent_bg = parent.cget("bg")
        super().__init__(parent, highlightthickness=0, bd=0, bg=self.parent_bg)
        self._bg = bg
        self._hover = hover or bg
        self._fg = fg
        self._cmd = command
        self._radius = radius
        self._text = text
        self._font = (FONT, font_size, "bold" if bold else "normal")
        self._h = height

        tmp = tk.font.Font(family=FONT, size=font_size, weight="bold" if bold else "normal")
        text_w = tmp.measure(text)
        self._w = width if width else text_w + pad_x * 2
        self.configure(width=self._w, height=self._h)

        self._draw(self._bg)
        self.bind("<Enter>", lambda e: self._draw(self._hover))
        self.bind("<Leave>", lambda e: self._draw(self._bg))
        self.bind("<Button-1>", self._click)
        self.configure(cursor="hand2")

    def _round_rect(self, x1, y1, x2, y2, r, color):
        self.create_arc(x1, y1, x1 + 2 * r, y1 + 2 * r, start=90, extent=90, fill=color, outline=color)
        self.create_arc(x2 - 2 * r, y1, x2, y1 + 2 * r, start=0, extent=90, fill=color, outline=color)
        self.create_arc(x1, y2 - 2 * r, x1 + 2 * r, y2, start=180, extent=90, fill=color, outline=color)
        self.create_arc(x2 - 2 * r, y2 - 2 * r, x2, y2, start=270, extent=90, fill=color, outline=color)
        self.create_rectangle(x1 + r, y1, x2 - r, y2, fill=color, outline=color)
        self.create_rectangle(x1, y1 + r, x2, y2 - r, fill=color, outline=color)

    def _draw(self, color):
        self.delete("all")
        self._round_rect(1, 1, self._w - 1, self._h - 1, self._radius, color)
        self.create_text(self._w / 2, self._h / 2, text=self._text,
                         fill=self._fg, font=self._font)

    def _click(self, _):
        if self._cmd:
            self._cmd()


# ───────────────────────── Поле ввода (скруглённое, без артефактов) ─────────────────────────
class Field(tk.Frame):
    def __init__(self, parent, *, font_size=18, show="", justify="left",
                 accent=C.primary, big=False):
        self._bg_parent = parent.cget("bg")
        super().__init__(parent, bg=C.border, bd=0, highlightthickness=0)
        self._accent = accent
        pad = 16 if not big else 22
        self.inner = tk.Frame(self, bg=C.card2, bd=0)
        self.inner.pack(fill="both", expand=True, padx=2, pady=2)
        self.entry = tk.Entry(
            self.inner, show=show, font=(FONT, font_size, "bold" if big else "normal"),
            bd=0, relief="flat", justify=justify,
            bg=C.card2, fg=C.text, insertbackground=accent,
            highlightthickness=0, disabledbackground=C.card2,
        )
        self.entry.pack(fill="both", expand=True, ipady=pad, padx=16)
        self.entry.bind("<FocusIn>", lambda e: self.config(bg=self._accent))
        self.entry.bind("<FocusOut>", lambda e: self.config(bg=C.border))

    def get(self): return self.entry.get()
    def set(self, v):
        self.entry.delete(0, "end"); self.entry.insert(0, v)
    def clear(self): self.entry.delete(0, "end")
    def focus_set(self): self.entry.focus_set()
    def bind_entry(self, seq, fn): self.entry.bind(seq, fn)


class Settings:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.load()

    def load(self) -> None:
        if SETTINGS_FILE.exists():
            try:
                self.data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}

    def save(self) -> None:
        SETTINGS_FILE.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get(self, key, default=None): return self.data.get(key, default)
    def set(self, key, value):
        self.data[key] = value; self.save()

    def clear_session(self) -> None:
        for key in ("token", "access_level", "user_name", "user_role", "last_module"):
            self.data.pop(key, None)
        self.save()


class ApiClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _headers(self, token=None):
        token = token if token is not None else self.settings.get("token")
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _decode(self, raw):
        if not raw:
            return None
        text = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except Exception:
            return text

    def _request(self, method, path, *, token=None, json_body=None):
        data = json.dumps(json_body, ensure_ascii=False).encode("utf-8") if json_body is not None else None
        req = urllib.request.Request(API_BASE_URL + path, data=data,
                                     headers=self._headers(token), method=method)
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return self._decode(resp.read())
        except urllib.error.HTTPError as exc:
            body = self._decode(exc.read())
            msg = f"Помилка сервера ({exc.code})"
            if isinstance(body, dict):
                msg = str(body.get("detail") or body.get("message") or msg)
            elif body:
                msg = str(body)
            raise ApiError(msg, exc.code) from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"Немає зв'язку з сервером: {exc.reason}") from exc
        except TimeoutError as exc:
            raise ApiError("Немає зв'язку: перевищено час очікування") from exc

    def login(self, surname, password):
        return self._request("POST", "/login", json_body={"surname": surname, "password": password}) or {}
    def register(self, surname, password):
        self._request("POST", "/register", json_body={"surname": surname, "password": password})
    def admin_login(self, password):
        return self._request("POST", "/admin_login", json_body={"password": password}) or {}
    def add_record(self, record):
        return self._request("POST", "/add_record", json_body=record) or {}
    def history(self):
        return self._request("GET", "/get_history") or []
    def clear_history(self):
        self._request("DELETE", "/clear_tracking")
    def errors(self):
        return self._request("GET", "/get_errors") or []
    def clear_errors(self):
        self._request("DELETE", "/clear_errors")
    def delete_error(self, eid):
        self._request("DELETE", f"/delete_error/{eid}")
    def pending_users(self, t):
        return self._request("GET", "/admin/registration_requests", token=t) or []
    def users(self, t):
        return self._request("GET", "/admin/users", token=t) or []
    def approve_user(self, t, rid, role):
        self._request("POST", f"/admin/registration_requests/{rid}/approve", token=t, json_body={"role": role})
    def reject_user(self, t, rid):
        self._request("POST", f"/admin/registration_requests/{rid}/reject", token=t)
    def update_user(self, t, uid, payload):
        self._request("PATCH", f"/admin/users/{uid}", token=t, json_body=payload)
    def delete_user(self, t, uid):
        self._request("DELETE", f"/admin/users/{uid}", token=t)


class OfflineQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        with sqlite3.connect(DB_FILE) as db:
            db.execute("CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, created_at TEXT NOT NULL)")
            db.commit()

    def add(self, record):
        with self.lock, sqlite3.connect(DB_FILE) as db:
            db.execute("INSERT INTO records(payload, created_at) VALUES(?, ?)",
                       (json.dumps(record, ensure_ascii=False), datetime.now(timezone.utc).isoformat()))
            db.commit()

    def count(self):
        with sqlite3.connect(DB_FILE) as db:
            return int(db.execute("SELECT COUNT(*) FROM records").fetchone()[0])

    def all(self):
        with sqlite3.connect(DB_FILE) as db:
            rows = db.execute("SELECT id, payload FROM records ORDER BY id").fetchall()
        result = []
        for rid, payload in rows:
            try:
                result.append((int(rid), json.loads(payload)))
            except Exception:
                self.delete(int(rid))
        return result

    def delete(self, rid):
        with self.lock, sqlite3.connect(DB_FILE) as db:
            db.execute("DELETE FROM records WHERE id=?", (rid,))
            db.commit()


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1300x840")
        self.minsize(1080, 720)
        self.configure(bg=C.bg)
        try:
            self.state("zoomed")
        except Exception:
            pass
        self.settings = Settings()
        self.api = ApiClient(self.settings)
        self.offline = OfflineQueue()
        self.tasks: queue.Queue = queue.Queue()
        self.current_frame: Optional[tk.Frame] = None
        self.style = ttk.Style(self)
        self.style.theme_use("clam")
        self._style()
        self.after(100, self._poll_tasks)
        self.after(1000, self._sync_loop)
        self.show_scanner() if self.settings.get("token") else self.show_login()

    def _style(self):
        self.style.configure("Modern.Treeview",
                             rowheight=48, font=(FONT, 14),
                             background=C.card, fieldbackground=C.card,
                             foreground=C.text, borderwidth=0)
        self.style.configure("Modern.Treeview.Heading",
                             font=(FONT, 14, "bold"),
                             background=C.bg2, foreground=C.on_dark,
                             relief="flat", padding=(8, 14))
        self.style.map("Modern.Treeview.Heading", background=[("active", C.bg)])
        self.style.map("Modern.Treeview",
                       background=[("selected", C.primary)],
                       foreground=[("selected", C.white)])
        self.style.configure("Modern.Vertical.TScrollbar",
                             background=C.border, troughcolor=C.card2,
                             borderwidth=0, arrowsize=14)
        self.style.map("Modern.Vertical.TScrollbar", background=[("active", C.primary)])

    def run_bg(self, func, ok=None, fail=None):
        def worker():
            try:
                res = func()
                if ok:
                    self.tasks.put((ok, (res,), {}))
            except Exception as exc:
                self.tasks.put(((fail or self.show_error), (exc,), {}))
        threading.Thread(target=worker, daemon=True).start()

    def _poll_tasks(self):
        while True:
            try:
                func, args, kwargs = self.tasks.get_nowait()
            except queue.Empty:
                break
            func(*args, **kwargs)
        self.after(80, self._poll_tasks)

    def _sync_loop(self):
        self.sync_offline(silent=True)
        self.after(SYNC_INTERVAL_MS, self._sync_loop)

    def sync_offline(self, silent=False):
        if not self.settings.get("token") or self.offline.count() == 0:
            if not silent:
                messagebox.showinfo("Синхронізація", "Черга порожня — відправляти нічого.")
            return
        def job():
            sent = 0
            for rid, record in self.offline.all():
                self.api.add_record(record)
                self.offline.delete(rid)
                sent += 1
            return sent
        def ok(sent):
            if sent and not silent:
                messagebox.showinfo("Синхронізація", f"Відправлено офлайн-записів: {sent}")
            if isinstance(self.current_frame, ScannerFrame):
                self.current_frame.update_queue_badge()
        self.run_bg(job, ok=ok, fail=(lambda e: None if silent else self.show_error(e)))

    def show_error(self, exc):
        messagebox.showerror("Помилка", str(exc))

    def switch(self, frame_cls, *args):
        if self.current_frame:
            self.current_frame.destroy()
        self.current_frame = frame_cls(self, *args)
        self.current_frame.pack(fill="both", expand=True)

    def show_login(self): self.switch(LoginFrame)
    def show_scanner(self): self.switch(ScannerFrame)
    def show_history(self): self.switch(HistoryFrame)
    def show_errors_frame(self): self.switch(ErrorsFrame)
    def show_admin(self, token): self.switch(AdminFrame, token)


class BaseFrame(tk.Frame):
    def __init__(self, app, bg=C.bg):
        super().__init__(app, bg=bg)
        self.app = app

# ───────────────────────── Логин ─────────────────────────
class LoginFrame(BaseFrame):
    def __init__(self, app: App):
        super().__init__(app, bg=C.bg)

        card = tk.Frame(self, bg=C.card)
        card.place(relx=.5, rely=.5, anchor="center", width=560)
        inner = tk.Frame(card, bg=C.card, padx=52, pady=48)
        inner.pack(fill="both", expand=True)

        tk.Label(inner, text="BoxID-ТТН", bg=C.card, fg=C.text,
                 font=(FONT, 38, "bold")).pack(anchor="w")
        tk.Label(inner, text="Модуль сканування  •  DC Link", bg=C.card,
                 fg=C.text_soft, font=(FONT, 15)).pack(anchor="w", pady=(2, 32))

        self.surname = self._field(inner, "Прізвище / Фамилия")
        self.password = self._field(inner, "Пароль", show="*")

        RoundButton(inner, "УВІЙТИ", self.login, bg=C.primary, hover=C.primary_h,
                    font_size=17, height=54, width=456).pack(pady=(26, 12))
        RoundButton(inner, "Реєстрація", self.register, bg=C.green, hover=C.green_h,
                    font_size=15, height=48, width=456).pack(pady=6)
        RoundButton(inner, "Панель адміністратора", self.admin_login, bg=C.slate,
                    hover=C.slate_h, font_size=15, height=48, width=456).pack(pady=6)

        self.password.bind_entry("<Return>", lambda e: self.login())
        self.surname.bind_entry("<Return>", lambda e: self.password.focus_set())
        self.surname.focus_set()

    def _field(self, parent, label, show=""):
        tk.Label(parent, text=label, bg=C.card, fg=C.text,
                 font=(FONT, 13, "bold")).pack(anchor="w", pady=(12, 6))
        f = Field(parent, font_size=18, show=show)
        f.pack(fill="x")
        return f

    def login(self):
        surname, password = self.surname.get().strip(), self.password.get().strip()
        if not surname or not password:
            messagebox.showwarning("Вхід", "Введіть прізвище та пароль")
            return
        def ok(data):
            token = str(data.get("token") or "")
            if not token:
                raise ApiError("Сервер не повернув токен")
            role = str(data.get("role") or "viewer")
            self.app.settings.set("token", token)
            self.app.settings.set("user_name", str(data.get("surname") or surname))
            self.app.settings.set("user_role", role)
            self.app.settings.set("access_level", int(data.get("access_level") if data.get("access_level") is not None else ROLE_LEVELS.get(role, 2)))
            self.app.settings.set("last_module", "tracking")
            self.app.show_scanner()
        self.app.run_bg(lambda: self.app.api.login(surname, password), ok=ok)

    def register(self):
        RegisterDialog(self.app)

    def admin_login(self):
        pwd = InputDialog.ask(self.app, "Панель адміністратора", "Пароль адміністратора", secret=True)
        if not pwd:
            return
        def ok(data):
            token = str(data.get("token") or data.get("admin_token") or "")
            if not token:
                raise ApiError("Сервер не повернув адмін-токен")
            self.app.show_admin(token)
        self.app.run_bg(lambda: self.app.api.admin_login(pwd), ok=ok)


# ───────────────────────── Верхняя панель (общая) ─────────────────────────
def build_topbar(parent, title, app, buttons, back=None):
    bar = tk.Frame(parent, bg=C.bg2, height=80)
    bar.pack(fill="x")
    bar.pack_propagate(False)

    left = tk.Frame(bar, bg=C.bg2)
    left.pack(side="left", padx=24, fill="y")
    li = tk.Frame(left, bg=C.bg2)
    li.pack(expand=True)
    if back:
        RoundButton(li, "‹  Назад", back, bg=C.slate, hover=C.slate_h,
                    font_size=14, height=42).pack(side="left", padx=(0, 16))
    tk.Label(li, text=title, bg=C.bg2, fg=C.on_dark,
             font=(FONT, 24, "bold")).pack(side="left")

    right = tk.Frame(bar, bg=C.bg2)
    right.pack(side="right", padx=18, fill="y")
    ri = tk.Frame(right, bg=C.bg2)
    ri.pack(expand=True)
    for text, cmd, bg, hv, fg in buttons:
        RoundButton(ri, text, cmd, bg=bg, hover=hv, fg=fg,
                    font_size=14, height=42).pack(side="left", padx=5)
    return bar


# ───────────────────────── Сканер ─────────────────────────
class ScannerFrame(BaseFrame):
    def __init__(self, app: App):
        super().__init__(app, bg=C.bg)
        self.scan_buffer = ""
        self.scan_after = None

        buttons = [
            ("Історія", app.show_history, C.primary, C.primary_h, C.white),
            ("Помилки", app.show_errors_frame, C.amber, C.amber_h, C.white),
            ("Синхр.", lambda: app.sync_offline(False), C.green, C.green_h, C.white),
            ("Вихід", self.logout, C.red, C.red_h, C.white),
        ]
        build_topbar(self, "BoxID-ТТН", app, buttons)

        # Инфо-строка пользователя
        sub = tk.Frame(self, bg=C.bg2, height=46)
        sub.pack(fill="x")
        sub.pack_propagate(False)
        user = app.settings.get("user_name", "operator")
        role = ROLE_LABELS.get(app.settings.get("user_role", "viewer"), "Перегляд")
        tk.Label(sub, text=f"Користувач: {user}   •   Роль: {role}",
                 bg=C.bg2, fg=C.on_dark_soft, font=(FONT, 13, "bold")).pack(side="left", padx=24)
        self.queue_lbl = tk.Label(sub, text="", bg=C.bg2, font=(FONT, 13, "bold"))
        self.queue_lbl.pack(side="right", padx=24)

        # Карточка
        wrap = tk.Frame(self, bg=C.bg)
        wrap.pack(fill="both", expand=True, padx=40, pady=30)
        card = tk.Frame(wrap, bg=C.card)
        card.pack(fill="both", expand=True)
        inner = tk.Frame(card, bg=C.card, padx=52, pady=40)
        inner.pack(fill="both", expand=True)

        tk.Label(inner, text="Сканування", bg=C.card, fg=C.text,
                 font=(FONT, 32, "bold")).pack(anchor="w")
        tk.Label(inner, text="1) Скануйте BoxID   →   2) Скануйте ТТН.  Поля очищаються автоматично.",
                 bg=C.card, fg=C.text_soft, font=(FONT, 16)).pack(anchor="w", pady=(6, 28))

        self.box = self._big(inner, "BOXID", C.primary)
        self.ttn = self._big(inner, "ТТН", C.green)

        self.status = tk.Label(inner, text="Готово до сканування", bg=C.card2,
                               fg=C.text, font=(FONT, 26, "bold"), pady=26)
        self.status.pack(fill="x", pady=(28, 0))

        self.box.bind_entry("<Return>", lambda e: self.handle_box())
        self.ttn.bind_entry("<Return>", lambda e: self.handle_ttn())
        self.bind_all("<Key>", self.global_key)
        self.box.focus_set()
        self.update_queue_badge()

    def destroy(self):
        self.unbind_all("<Key>")
        super().destroy()

    def _big(self, parent, title, accent):
        tk.Label(parent, text=title, bg=C.card, fg=C.text,
                 font=(FONT, 18, "bold")).pack(anchor="w", pady=(14, 8))
        f = Field(parent, font_size=32, justify="center", accent=accent, big=True)
        f.pack(fill="x")
        f.bind_entry("<FocusIn>", lambda ev: f.clear())
        return f

    def global_key(self, event):
        if event.keysym in ("Return", "KP_Enter"):
            code = only_digits(self.scan_buffer)
            self.scan_buffer = ""
            if self.scan_after:
                self.after_cancel(self.scan_after); self.scan_after = None
            if code:
                self.route_code(code)
            return
        if event.char and event.char.isprintable():
            self.scan_buffer += event.char
            if self.scan_after:
                self.after_cancel(self.scan_after)
            self.scan_after = self.after(SCAN_BUFFER_MS, lambda: setattr(self, "scan_buffer", ""))

    def route_code(self, code):
        if not self.box.get().strip() or self.focus_get() == self.box.entry:
            self.box.set(code); beep_success(self.app); self.ttn.focus_set()
        else:
            self.ttn.set(code); self.handle_ttn()

    def handle_box(self):
        val = only_digits(self.box.get())
        if val:
            self.box.set(val); beep_success(self.app); self.ttn.focus_set()

    def handle_ttn(self):
        box, ttn = only_digits(self.box.get()), only_digits(self.ttn.get())
        if not box:
            self.box.focus_set(); return
        if not ttn:
            return
        record = {"user_name": self.app.settings.get("user_name", "operator"), "boxid": box, "ttn": ttn}
        self._status("Відправка...", C.primary, C.card2)
        self.box.clear(); self.ttn.clear(); self.box.focus_set()
        def ok(data):
            note = str(data.get("note") or "") if isinstance(data, dict) else ""
            if note:
                beep_error(self.app); self._status(f"Дублікат: {note}", "#8A5A00", "#FFF4DC")
            else:
                beep_success(self.app); self._status("Успішно додано", C.green_h, "#DCFBEF")
            self.update_queue_badge()
        def fail(exc):
            self.app.offline.add(record); beep_error(self.app)
            self._status("Збережено локально (офлайн)", "#8A5A00", "#FFF4DC")
            self.update_queue_badge()
        self.app.run_bg(lambda: self.app.api.add_record(record), ok=ok, fail=fail)

    def _status(self, text, fg, bg):
        self.status.config(text=text, fg=fg, bg=bg)

    def update_queue_badge(self):
        n = self.app.offline.count()
        if n:
            self.queue_lbl.config(text=f"Офлайн черга: {n}", fg=C.amber)
        else:
            self.queue_lbl.config(text="Онлайн — черга порожня", fg=C.green)

    def logout(self):
        if messagebox.askyesno("Вихід", "Вийти з акаунту?"):
            self.app.settings.clear_session(); self.app.show_login()


# ───────────────────────── Базовая таблица ─────────────────────────
class TableFrame(BaseFrame):
    columns = ()
    headings = ()
    widths = ()

    def make_table(self, title, extra_buttons=None):
        buttons = [("Оновити", self.load, C.green, C.green_h, C.white)]
        buttons = (extra_buttons or []) + buttons
        build_topbar(self, title, self.app, buttons, back=self.app.show_scanner)

        wrap = tk.Frame(self, bg=C.bg)
        wrap.pack(fill="both", expand=True, padx=24, pady=22)
        card = tk.Frame(wrap, bg=C.card)
        card.pack(fill="both", expand=True)
        holder = tk.Frame(card, bg=C.card, padx=6, pady=6)
        holder.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(holder, columns=self.columns, show="headings",
                                 style="Modern.Treeview")
        vsb = ttk.Scrollbar(holder, orient="vertical", command=self.tree.yview,
                            style="Modern.Vertical.TScrollbar")
        self.tree.configure(yscrollcommand=vsb.set)
        widths = self.widths or tuple(150 for _ in self.columns)
        for c, h, w in zip(self.columns, self.headings, widths):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="center")
        self.tree.tag_configure("odd", background=C.card)
        self.tree.tag_configure("even", background="#F2F6FD")
        vsb.pack(side="right", fill="y")
        self.tree.pack(side="left", fill="both", expand=True)

    def _ins(self, values):
        tag = "even" if len(self.tree.get_children()) % 2 == 0 else "odd"
        self.tree.insert("", "end", values=values, tags=(tag,))

    def selected_id(self):
        sel = self.tree.selection()
        return int(self.tree.item(sel[0], "values")[0]) if sel else None


# ───────────────────────── История ─────────────────────────
class HistoryFrame(TableFrame):
    columns = ("datetime", "user_name", "boxid", "ttn")
    headings = ("Дата", "Користувач", "BoxID", "ТТН")
    widths = (250, 250, 220, 220)

    def __init__(self, app):
        super().__init__(app, bg=C.bg)
        extra = []
        if app.settings.get("user_role") == "admin":
            extra = [("Очистити", self.clear, C.red, C.red_h, C.white)]
        self.make_table("Історія сканувань", extra)
        self.load()

    def load(self):
        self.app.run_bg(self.app.api.history, ok=self.fill)

    def fill(self, rows):
        self.tree.delete(*self.tree.get_children())
        rows.sort(key=lambda r: str(r.get("datetime") or ""), reverse=True)
        for r in rows:
            self._ins((parse_dt(r.get("datetime")), r.get("user_name", ""),
                       r.get("boxid", ""), r.get("ttn", "")))

    def clear(self):
        if messagebox.askyesno("Очистити", "Видалити всю історію?"):
            self.app.run_bg(self.app.api.clear_history, ok=lambda _: self.load())


# ───────────────────────── Ошибки ─────────────────────────
class ErrorsFrame(TableFrame):
    columns = ("id", "datetime", "user_name", "boxid", "ttn", "error")
    headings = ("ID", "Дата", "Користувач", "BoxID", "ТТН", "Помилка")
    widths = (70, 200, 200, 170, 170, 300)

    def __init__(self, app):
        super().__init__(app, bg=C.bg)
        extra = []
        if app.settings.get("user_role") in ("admin", "operator"):
            extra = [
                ("Видалити обрану", self.delete_selected, C.red, C.red_h, C.white),
                ("Очистити все", self.clear, C.slate, C.slate_h, C.white),
            ]
        self.make_table("Журнал помилок", extra)
        self.load()

    def load(self):
        self.app.run_bg(self.app.api.errors, ok=self.fill)

    def fill(self, rows):
        self.tree.delete(*self.tree.get_children())
        rows.sort(key=lambda r: str(r.get("datetime") or ""), reverse=True)
        for r in rows:
            self._ins((r.get("id", ""), parse_dt(r.get("datetime")), r.get("user_name", ""),
                       r.get("boxid", ""), r.get("ttn", ""), r.get("error") or r.get("message", "")))

    def delete_selected(self):
        eid = self.selected_id()
        if eid is None:
            messagebox.showinfo("Видалення", "Оберіть запис."); return
        if messagebox.askyesno("Видалити", f"Видалити помилку #{eid}?"):
            self.app.run_bg(lambda: self.app.api.delete_error(eid), ok=lambda _: self.load())

    def clear(self):
        if messagebox.askyesno("Очистити", "Видалити весь журнал помилок?"):
            self.app.run_bg(self.app.api.clear_errors, ok=lambda _: self.load())

# ───────────────────────── Админ-панель ─────────────────────────
class AdminFrame(TableFrame):
    columns = ("id", "surname", "role", "active", "created")
    headings = ("ID", "Прізвище", "Роль", "Активний", "Створено")
    widths = (70, 300, 180, 150, 240)

    def __init__(self, app, admin_token):
        self.admin_token = admin_token
        super().__init__(app, bg=C.bg)
        extra = [
            ("Заявки", self.open_pending, C.amber, C.amber_h, C.white),
            ("Роль", self.change_role, C.primary, C.primary_h, C.white),
            ("Вкл/Викл", self.toggle, C.green, C.green_h, C.white),
            ("Видалити", self.delete, C.red, C.red_h, C.white),
        ]
        self.make_table("Адмін-панель", extra)
        self.load()

    def load(self):
        self.app.run_bg(lambda: self.app.api.users(self.admin_token), ok=self.fill)

    def fill(self, rows):
        self.tree.delete(*self.tree.get_children())
        for r in rows:
            self._ins((
                r.get("id", ""), r.get("surname", ""),
                ROLE_LABELS.get(str(r.get("role")), r.get("role", "")),
                "Так" if r.get("is_active") else "Ні",
                parse_dt(r.get("created_at")),
            ))

    def open_pending(self):
        PendingDialog(self.app, self.admin_token)

    def change_role(self):
        uid = self.selected_id()
        if uid is None:
            messagebox.showinfo("Роль", "Оберіть користувача."); return
        role = ChoiceDialog.ask(self.app, "Зміна ролі", "Оберіть нову роль",
                                [("admin", "Адмін"), ("operator", "Оператор"), ("viewer", "Перегляд")])
        if role:
            self.app.run_bg(lambda: self.app.api.update_user(self.admin_token, uid, {"role": role}),
                            ok=lambda _: self.load())

    def toggle(self):
        uid = self.selected_id()
        if uid is None:
            messagebox.showinfo("Статус", "Оберіть користувача."); return
        active_now = self.tree.item(self.tree.selection()[0], "values")[3] == "Так"
        self.app.run_bg(lambda: self.app.api.update_user(self.admin_token, uid, {"is_active": not active_now}),
                        ok=lambda _: self.load())

    def delete(self):
        uid = self.selected_id()
        if uid is None:
            messagebox.showinfo("Видалення", "Оберіть користувача."); return
        if messagebox.askyesno("Видалити", f"Видалити користувача #{uid}?"):
            self.app.run_bg(lambda: self.app.api.delete_user(self.admin_token, uid),
                            ok=lambda _: self.load())


# ───────────────────────── Базовый диалог ─────────────────────────
class BaseDialog(tk.Toplevel):
    def __init__(self, master, title, width=480, height=380):
        super().__init__(master)
        self.master_win = master
        self.title(title)
        self.configure(bg=C.card)
        self.resizable(False, False)
        self.transient(master.winfo_toplevel())
        self.update_idletasks()
        root = master.winfo_toplevel()
        x = root.winfo_rootx() + (root.winfo_width() - width) // 2
        y = root.winfo_rooty() + (root.winfo_height() - height) // 2
        self.geometry(f"{width}x{height}+{max(x, 0)}+{max(y, 0)}")

        header = tk.Frame(self, bg=C.bg2, height=64)
        header.pack(fill="x")
        header.pack_propagate(False)
        tk.Label(header, text=title, bg=C.bg2, fg=C.on_dark,
                 font=(FONT, 18, "bold")).pack(side="left", padx=22)

        self.body = tk.Frame(self, bg=C.card, padx=30, pady=24)
        self.body.pack(fill="both", expand=True)

        self.after(50, self._grab)

    def _grab(self):
        try:
            self.grab_set()
            self.focus_force()
        except Exception:
            pass


# ───────────────────────── Заявки на регистрацию ─────────────────────────
class PendingDialog(BaseDialog):
    def __init__(self, app, token):
        super().__init__(app, "Заявки на реєстрацію", width=820, height=540)
        self.app = app
        self.token = token

        holder = tk.Frame(self.body, bg=C.card)
        holder.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(holder, columns=("id", "surname", "created"),
                                 show="headings", style="Modern.Treeview")
        for c, h, w in zip(("id", "surname", "created"),
                           ("ID", "Прізвище", "Створено"), (80, 360, 280)):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="both", expand=True)

        bar = tk.Frame(self.body, bg=C.card)
        bar.pack(fill="x", pady=(16, 0))
        RoundButton(bar, "Схвалити", self.approve, bg=C.green, hover=C.green_h,
                    font_size=14, height=44).pack(side="left", padx=4)
        RoundButton(bar, "Відхилити", self.reject, bg=C.red, hover=C.red_h,
                    font_size=14, height=44).pack(side="left", padx=4)
        RoundButton(bar, "Оновити", self.load, bg=C.slate, hover=C.slate_h,
                    font_size=14, height=44).pack(side="right", padx=4)
        self.load()

    def _id(self):
        sel = self.tree.selection()
        return int(self.tree.item(sel[0], "values")[0]) if sel else None

    def load(self):
        self.app.run_bg(lambda: self.app.api.pending_users(self.token), ok=self.fill)

    def fill(self, rows):
        self.tree.delete(*self.tree.get_children())
        for r in rows:
            self.tree.insert("", "end", values=(r.get("id", ""), r.get("surname", ""),
                                                parse_dt(r.get("created_at"))))

    def approve(self):
        rid = self._id()
        if rid is None:
            messagebox.showinfo("Заявки", "Оберіть заявку.", parent=self); return
        # ВАЖНО: диалог выбора роли дочерний к ЭТОМУ окну
        role = ChoiceDialog.ask(self, "Підтвердження заявки",
                                f"Оберіть роль для заявки #{rid}",
                                [("operator", "Оператор"), ("viewer", "Перегляд"), ("admin", "Адмін")])
        if not role:
            return
        def ok(_):
            messagebox.showinfo("Заявки", "Заявку схвалено.", parent=self)
            self.load()
        self.app.run_bg(lambda: self.app.api.approve_user(self.token, rid, role), ok=ok)

    def reject(self):
        rid = self._id()
        if rid is None:
            messagebox.showinfo("Заявки", "Оберіть заявку.", parent=self); return
        if messagebox.askyesno("Відхилити", f"Відхилити заявку #{rid}?", parent=self):
            self.app.run_bg(lambda: self.app.api.reject_user(self.token, rid),
                            ok=lambda _: self.load())


# ───────────────────────── Регистрация ─────────────────────────
class RegisterDialog(BaseDialog):
    def __init__(self, app):
        super().__init__(app, "Реєстрація", width=500, height=480)
        self.app = app
        self.surname = self._field("Прізвище")
        self.password = self._field("Пароль", "*")
        self.confirm = self._field("Повтор пароля", "*")
        RoundButton(self.body, "Відправити заявку", self.submit, bg=C.green,
                    hover=C.green_h, font_size=16, height=52, width=440).pack(pady=(24, 0))

    def _field(self, label, show=""):
        tk.Label(self.body, text=label, bg=C.card, fg=C.text,
                 font=(FONT, 13, "bold")).pack(anchor="w", pady=(10, 5))
        f = Field(self.body, font_size=17, show=show)
        f.pack(fill="x")
        return f

    def submit(self):
        s, p, c = self.surname.get().strip(), self.password.get().strip(), self.confirm.get().strip()
        if not s or not p or len(p) < 6 or p != c:
            messagebox.showwarning("Реєстрація",
                                   "Заповніть поля, пароль від 6 символів, паролі мають співпадати",
                                   parent=self)
            return
        def ok(_):
            messagebox.showinfo("Реєстрація",
                                "Заявку відправлено. Очікуйте підтвердження адміністратора.",
                                parent=self)
            self.destroy()
        self.app.run_bg(lambda: self.app.api.register(s, p), ok=ok)


# ───────────────────────── Ввод текста ─────────────────────────
class InputDialog:
    @staticmethod
    def ask(master, title, prompt, secret=False):
        dlg = BaseDialog(master, title, width=460, height=290)
        result = {"v": ""}
        tk.Label(dlg.body, text=prompt, bg=C.card, fg=C.text,
                 font=(FONT, 15, "bold")).pack(anchor="w", pady=(4, 10))
        field = Field(dlg.body, font_size=18, show="*" if secret else "")
        field.pack(fill="x")
        field.focus_set()

        def ok():
            result["v"] = field.get()
            dlg.destroy()

        RoundButton(dlg.body, "OK", ok, bg=C.primary, hover=C.primary_h,
                    font_size=15, height=50, width=400).pack(pady=(24, 0))
        field.bind_entry("<Return>", lambda e: ok())
        master.winfo_toplevel().wait_window(dlg)
        return result["v"]


# ───────────────────────── Выбор варианта ─────────────────────────
class ChoiceDialog:
    @staticmethod
    def ask(master, title, prompt, choices):
        height = 170 + len(choices) * 66
        dlg = BaseDialog(master, title, width=420, height=height)
        result = {"v": None}
        tk.Label(dlg.body, text=prompt, bg=C.card, fg=C.text,
                 font=(FONT, 15, "bold")).pack(anchor="w", pady=(4, 14))

        palette = [(C.primary, C.primary_h), (C.green, C.green_h), (C.amber, C.amber_h)]
        for i, (value, label) in enumerate(choices):
            bg, hv = palette[i % len(palette)]

            def choose(v=value):
                result["v"] = v
                dlg.destroy()

            RoundButton(dlg.body, label, choose, bg=bg, hover=hv,
                        font_size=15, height=52, width=356).pack(pady=6)
        master.winfo_toplevel().wait_window(dlg)
        return result["v"]


def main() -> int:
    try:
        # инициализация tkinter.font до создания виджетов
        import tkinter.font  # noqa: F401
        app = App()
        app.mainloop()
        return 0
    except Exception as exc:
        traceback.print_exc()
        try:
            messagebox.showerror(APP_NAME, str(exc))
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
