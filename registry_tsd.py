#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BoxID-ТТН Windows — переработанный дизайн (крупный, современный, профессиональный).
Логика и API не изменены, обновлён только UI/UX.

Запуск:
    python tracking_windows_app.py
"""

from __future__ import annotations

import json
import os
import queue
import re
import sqlite3
import sys
import threading
import time
import traceback
import tkinter as tk
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable, Optional

APP_NAME = "BoxID-ТТН Windows"
API_BASE_URL = "https://tracking-app.dclink.ua"
REQUEST_TIMEOUT = 8
SYNC_INTERVAL_MS = 20_000
SCAN_BUFFER_MS = 180

ROLE_LABELS = {"admin": "Адмін", "operator": "Оператор", "viewer": "Перегляд"}
ROLE_LEVELS = {"admin": 1, "operator": 0, "viewer": 2}

FONT = "Segoe UI"


# ───────────────────────── Палитра ─────────────────────────
class C:
    deep_blue = "#07153A"
    bg_dark = "#0A1B47"
    panel = "#FFFFFF"
    panel_soft = "#F2F5FB"
    surface = "#F7F8FA"
    field_bg = "#F2F5FB"
    field_border = "#D6E0F0"

    blue = "#075BFF"
    blue_hover = "#0A50DB"
    soft_blue = "#3F8CFF"
    cyan = "#04C8E8"
    emerald = "#14C9A6"
    emerald_hover = "#10AB8D"
    mint = "#5EF2D0"
    amber = "#FFB020"
    amber_hover = "#E89A0C"
    red = "#E5484D"
    red_hover = "#C93B40"
    red_dark = "#B00020"

    text_dark = "#0B1530"
    text_muted = "#60708C"
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


# ───────────────────────── Современная кнопка ─────────────────────────
class ModernButton(tk.Frame):
    """Плоская кнопка со скруглением (имитация) и hover-эффектом."""

    def __init__(self, parent, text, command, *, bg, fg="white",
                 hover=None, font_size=15, pad_x=22, pad_y=12, icon=""):
        super().__init__(parent, bg=parent.cget("bg"))
        self._bg = bg
        self._hover = hover or bg
        self._command = command
        display = f"{icon}  {text}" if icon else text
        self._lbl = tk.Label(
            self, text=display, bg=bg, fg=fg,
            font=(FONT, font_size, "bold"),
            padx=pad_x, pady=pad_y, cursor="hand2",
        )
        self._lbl.pack(fill="both", expand=True)
        for w in (self, self._lbl):
            w.bind("<Enter>", self._on_enter)
            w.bind("<Leave>", self._on_leave)
            w.bind("<Button-1>", self._on_click)

    def _on_enter(self, _):
        self._lbl.config(bg=self._hover)

    def _on_leave(self, _):
        self._lbl.config(bg=self._bg)

    def _on_click(self, _):
        if self._command:
            self._command()


# ───────────────────────── Поле ввода ─────────────────────────
class FieldEntry(tk.Frame):
    """Карточка-поле ввода с подсветкой фокуса."""

    def __init__(self, parent, *, font_size=20, justify="left",
                 show="", accent=C.blue):
        super().__init__(parent, bg=C.field_border, padx=2, pady=2)
        self._accent = accent
        self.entry = tk.Entry(
            self, show=show, font=(FONT, font_size, "bold"),
            bd=0, relief="flat", justify=justify,
            bg=C.field_bg, fg=C.text_dark,
            insertbackground=accent, highlightthickness=0,
        )
        self.entry.pack(fill="both", expand=True, ipady=10, padx=12)
        self.entry.bind("<FocusIn>", self._focus_in)
        self.entry.bind("<FocusOut>", self._focus_out)

    def _focus_in(self, _):
        self.config(bg=self._accent)

    def _focus_out(self, _):
        self.config(bg=C.field_border)

    def get(self):
        return self.entry.get()

    def set(self, value):
        self.entry.delete(0, "end")
        self.entry.insert(0, value)

    def clear(self):
        self.entry.delete(0, "end")

    def focus_set(self):
        self.entry.focus_set()

    def bind_entry(self, seq, fn):
        self.entry.bind(seq, fn)


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

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.save()

    def clear_session(self) -> None:
        for key in ("token", "access_level", "user_name", "user_role", "last_module"):
            self.data.pop(key, None)
        self.save()


class ApiClient:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _headers(self, token: Optional[str] = None) -> dict[str, str]:
        token = token if token is not None else self.settings.get("token")
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _decode_response_body(self, raw: bytes) -> Any:
        if not raw:
            return None
        text = raw.decode("utf-8", errors="replace")
        try:
            return json.loads(text)
        except Exception:
            return text

    def _request(self, method: str, path: str, *, token: Optional[str] = None, json_body: Any = None) -> Any:
        data = None
        if json_body is not None:
            data = json.dumps(json_body, ensure_ascii=False).encode("utf-8")

        request = urllib.request.Request(
            API_BASE_URL + path,
            data=data,
            headers=self._headers(token),
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return self._decode_response_body(response.read())
        except urllib.error.HTTPError as exc:
            body = self._decode_response_body(exc.read())
            msg = f"Ошибка сервера ({exc.code})"
            if isinstance(body, dict):
                msg = str(body.get("detail") or body.get("message") or msg)
            elif body:
                msg = str(body)
            raise ApiError(msg, exc.code) from exc
        except urllib.error.URLError as exc:
            raise ApiError(f"Нет связи с сервером: {exc.reason}") from exc
        except TimeoutError as exc:
            raise ApiError("Нет связи с сервером: превышено время ожидания") from exc

    def login(self, surname: str, password: str) -> dict[str, Any]:
        return self._request("POST", "/login", json_body={"surname": surname, "password": password}) or {}

    def register(self, surname: str, password: str) -> None:
        self._request("POST", "/register", json_body={"surname": surname, "password": password})

    def admin_login(self, password: str) -> dict[str, Any]:
        return self._request("POST", "/admin_login", json_body={"password": password}) or {}

    def add_record(self, record: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/add_record", json_body=record) or {}

    def history(self) -> list[dict[str, Any]]:
        return self._request("GET", "/get_history") or []

    def clear_history(self) -> None:
        self._request("DELETE", "/clear_tracking")

    def errors(self) -> list[dict[str, Any]]:
        return self._request("GET", "/get_errors") or []

    def clear_errors(self) -> None:
        self._request("DELETE", "/clear_errors")

    def delete_error(self, error_id: int) -> None:
        self._request("DELETE", f"/delete_error/{error_id}")

    def pending_users(self, admin_token: str) -> list[dict[str, Any]]:
        return self._request("GET", "/admin/registration_requests", token=admin_token) or []

    def users(self, admin_token: str) -> list[dict[str, Any]]:
        return self._request("GET", "/admin/users", token=admin_token) or []

    def approve_user(self, admin_token: str, request_id: int, role: str) -> None:
        self._request("POST", f"/admin/registration_requests/{request_id}/approve", token=admin_token, json_body={"role": role})

    def reject_user(self, admin_token: str, request_id: int) -> None:
        self._request("POST", f"/admin/registration_requests/{request_id}/reject", token=admin_token)

    def update_user(self, admin_token: str, user_id: int, payload: dict[str, Any]) -> None:
        self._request("PATCH", f"/admin/users/{user_id}", token=admin_token, json_body=payload)

    def delete_user(self, admin_token: str, user_id: int) -> None:
        self._request("DELETE", f"/admin/users/{user_id}", token=admin_token)


class OfflineQueue:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        with sqlite3.connect(DB_FILE) as db:
            db.execute("CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, created_at TEXT NOT NULL)")
            db.commit()

    def add(self, record: dict[str, Any]) -> None:
        with self.lock, sqlite3.connect(DB_FILE) as db:
            db.execute("INSERT INTO records(payload, created_at) VALUES(?, ?)", (json.dumps(record, ensure_ascii=False), datetime.now(timezone.utc).isoformat()))
            db.commit()

    def count(self) -> int:
        with sqlite3.connect(DB_FILE) as db:
            return int(db.execute("SELECT COUNT(*) FROM records").fetchone()[0])

    def all(self) -> list[tuple[int, dict[str, Any]]]:
        with sqlite3.connect(DB_FILE) as db:
            rows = db.execute("SELECT id, payload FROM records ORDER BY id").fetchall()
        result = []
        for rid, payload in rows:
            try:
                result.append((int(rid), json.loads(payload)))
            except Exception:
                self.delete(int(rid))
        return result

    def delete(self, rid: int) -> None:
        with self.lock, sqlite3.connect(DB_FILE) as db:
            db.execute("DELETE FROM records WHERE id=?", (rid,))
            db.commit()


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1280x820")
        self.minsize(1040, 700)
        self.configure(bg=C.deep_blue)
        self.settings = Settings()
        self.api = ApiClient(self.settings)
        self.offline = OfflineQueue()
        self.tasks: queue.Queue[tuple[Callable, tuple, dict]] = queue.Queue()
        self.current_frame: Optional[tk.Frame] = None
        self.style = ttk.Style(self)
        self.style.theme_use("clam")
        self._style()
        self.after(100, self._poll_tasks)
        self.after(1000, self._sync_loop)
        self.show_scanner() if self.settings.get("token") else self.show_login()

    def _style(self) -> None:
        # Современная крупная таблица
        self.style.configure(
            "Modern.Treeview",
            rowheight=46,
            font=(FONT, 15),
            background=C.white,
            fieldbackground=C.white,
            foreground=C.text_dark,
            borderwidth=0,
        )
        self.style.configure(
            "Modern.Treeview.Heading",
            font=(FONT, 15, "bold"),
            background=C.deep_blue,
            foreground=C.white,
            relief="flat",
            padding=(8, 12),
        )
        self.style.map(
            "Modern.Treeview.Heading",
            background=[("active", C.bg_dark)],
        )
        self.style.map(
            "Modern.Treeview",
            background=[("selected", C.soft_blue)],
            foreground=[("selected", C.white)],
        )
        self.style.configure(
            "Modern.Vertical.TScrollbar",
            background=C.soft_blue,
            troughcolor=C.surface,
            borderwidth=0,
            arrowsize=16,
        )

    def run_bg(self, func: Callable, ok: Callable | None = None, fail: Callable | None = None) -> None:
        def worker() -> None:
            try:
                res = func()
                if ok:
                    self.tasks.put((ok, (res,), {}))
            except Exception as exc:
                self.tasks.put(((fail or self.show_error), (exc,), {}))
        threading.Thread(target=worker, daemon=True).start()

    def _poll_tasks(self) -> None:
        while True:
            try:
                func, args, kwargs = self.tasks.get_nowait()
            except queue.Empty:
                break
            func(*args, **kwargs)
        self.after(80, self._poll_tasks)

    def _sync_loop(self) -> None:
        self.sync_offline(silent=True)
        self.after(SYNC_INTERVAL_MS, self._sync_loop)

    def sync_offline(self, silent: bool = False) -> None:
        if not self.settings.get("token") or self.offline.count() == 0:
            if not silent:
                messagebox.showinfo("Синхронизация", "Очередь пуста — отправлять нечего.")
            return
        def job() -> int:
            sent = 0
            for rid, record in self.offline.all():
                self.api.add_record(record)
                self.offline.delete(rid)
                sent += 1
            return sent
        def ok(sent: int) -> None:
            if sent and not silent:
                messagebox.showinfo("Синхронизация", f"Отправлено офлайн-записей: {sent}")
            if isinstance(self.current_frame, ScannerFrame):
                self.current_frame.update_queue_badge()
        self.run_bg(job, ok=ok, fail=(lambda e: None if silent else self.show_error(e)))

    def show_error(self, exc: Exception) -> None:
        messagebox.showerror("Ошибка", str(exc))

    def switch(self, frame_cls: type[tk.Frame], *args: Any) -> None:
        if self.current_frame:
            self.current_frame.destroy()
        self.current_frame = frame_cls(self, *args)
        self.current_frame.pack(fill="both", expand=True)

    def show_login(self) -> None: self.switch(LoginFrame)
    def show_scanner(self) -> None: self.switch(ScannerFrame)
    def show_history(self) -> None: self.switch(HistoryFrame)
    def show_errors_frame(self) -> None: self.switch(ErrorsFrame)
    def show_admin(self, token: str) -> None: self.switch(AdminFrame, token)


class BaseFrame(tk.Frame):
    def __init__(self, app: App, bg: str = C.deep_blue):
        super().__init__(app, bg=bg)
        self.app = app


# ───────────────────────── Логин ─────────────────────────
class LoginFrame(BaseFrame):
    def __init__(self, app: App):
        super().__init__(app, bg=C.deep_blue)

        # Лёгкий градиентный фон через Canvas
        self._draw_background()

        card = tk.Frame(self, bg=C.white)
        card.place(relx=.5, rely=.5, anchor="center", width=620)

        inner = tk.Frame(card, bg=C.white, padx=48, pady=44)
        inner.pack(fill="both", expand=True)

        # Логотип
        logo = tk.Label(inner, text="🔳", bg=C.white, font=(FONT, 44))
        logo.pack(pady=(0, 4))
        tk.Label(inner, text="BoxID-ТТН", bg=C.white, fg=C.deep_blue,
                 font=(FONT, 40, "bold")).pack()
        tk.Label(inner, text="Модуль сканування • DC Link", bg=C.white,
                 fg=C.text_muted, font=(FONT, 16)).pack(pady=(4, 30))

        self.surname = self._entry(inner, "Прізвище / Фамилия", "👤")
        self.password = self._entry(inner, "Пароль", "🔒", show="*")

        ModernButton(inner, "УВІЙТИ", self.login, bg=C.blue,
                     hover=C.blue_hover, font_size=18, pad_y=15, icon="➜").pack(fill="x", pady=(24, 10))
        ModernButton(inner, "Реєстрація", self.register, bg=C.emerald,
                     hover=C.emerald_hover, font_size=16, icon="✚").pack(fill="x", pady=6)
        ModernButton(inner, "Панель адміністратора", self.admin_login,
                     bg=C.amber, hover=C.amber_hover, fg=C.deep_blue,
                     font_size=16, icon="⚙").pack(fill="x", pady=6)

        self.password.bind_entry("<Return>", lambda e: self.login())
        self.surname.bind_entry("<Return>", lambda e: self.password.focus_set())
        self.surname.focus_set()

    def _draw_background(self):
        cv = tk.Canvas(self, highlightthickness=0, bd=0)
        cv.pack(fill="both", expand=True)

        def render(_=None):
            cv.delete("all")
            w = cv.winfo_width() or 1280
            h = cv.winfo_height() or 820
            steps = 60
            top = (7, 21, 58)
            bot = (4, 74, 194)
            for i in range(steps):
                t = i / steps
                r = int(top[0] + (bot[0] - top[0]) * t)
                g = int(top[1] + (bot[1] - top[1]) * t)
                b = int(top[2] + (bot[2] - top[2]) * t)
                cv.create_rectangle(0, h * t, w, h * (t + 1 / steps) + 1,
                                    fill=f"#{r:02x}{g:02x}{b:02x}", width=0)

        cv.bind("<Configure>", render)
        self._bg_canvas = cv

    def _entry(self, parent, placeholder, icon, show=""):
        tk.Label(parent, text=f"{icon}  {placeholder}", bg=C.white, fg=C.text_dark,
                 font=(FONT, 14, "bold")).pack(anchor="w", pady=(10, 6))
        field = FieldEntry(parent, font_size=20, show=show, accent=C.blue)
        field.pack(fill="x")
        return field

    def login(self) -> None:
        surname, password = self.surname.get().strip(), self.password.get().strip()
        if not surname or not password:
            messagebox.showwarning("Вход", "Введите фамилию и пароль")
            return
        def ok(data: dict[str, Any]) -> None:
            token = str(data.get("token") or "")
            if not token:
                raise ApiError("Сервер не вернул токен")
            role = str(data.get("role") or "viewer")
            self.app.settings.set("token", token)
            self.app.settings.set("user_name", str(data.get("surname") or surname))
            self.app.settings.set("user_role", role)
            self.app.settings.set("access_level", int(data.get("access_level") if data.get("access_level") is not None else ROLE_LEVELS.get(role, 2)))
            self.app.settings.set("last_module", "tracking")
            self.app.show_scanner()
        self.app.run_bg(lambda: self.app.api.login(surname, password), ok=ok)

    def register(self) -> None:
        RegisterDialog(self.app)

    def admin_login(self) -> None:
        pwd = SimpleInput.ask(self.app, "Админ-панель", "Пароль администратора", secret=True)
        if not pwd:
            return
        def ok(data: dict[str, Any]) -> None:
            token = str(data.get("token") or data.get("admin_token") or "")
            if not token:
                raise ApiError("Сервер не вернул админ-токен")
            self.app.show_admin(token)
        self.app.run_bg(lambda: self.app.api.admin_login(pwd), ok=ok)


# ───────────────────────── Сканер ─────────────────────────
class ScannerFrame(BaseFrame):
    def __init__(self, app: App):
        super().__init__(app, bg=C.surface)
        self.scan_buffer = ""
        self.scan_after: Optional[str] = None

        # ── Верхняя панель ──
        top = tk.Frame(self, bg=C.deep_blue, height=92)
        top.pack(fill="x")
        top.pack_propagate(False)

        left = tk.Frame(top, bg=C.deep_blue)
        left.pack(side="left", padx=24, fill="y")

        title_row = tk.Frame(left, bg=C.deep_blue)
        title_row.pack(anchor="w", expand=True, fill="y")
        tk.Label(title_row, text="🔳", bg=C.deep_blue, font=(FONT, 26)).pack(side="left", pady=18)
        tk.Label(title_row, text="BoxID-ТТН", bg=C.deep_blue, fg=C.white,
                 font=(FONT, 28, "bold")).pack(side="left", padx=(10, 0), pady=18)

        # Инфо о пользователе
        user = app.settings.get("user_name", "operator")
        role = ROLE_LABELS.get(app.settings.get("user_role", "viewer"), "Перегляд")
        info = tk.Frame(top, bg=C.deep_blue)
        info.pack(side="left", padx=20, fill="y")
        chip = tk.Frame(info, bg=C.bg_dark)
        chip.pack(anchor="w", expand=True)
        tk.Label(chip, text=f"👤  {user}", bg=C.bg_dark, fg=C.white,
                 font=(FONT, 15, "bold"), padx=14, pady=8).pack(side="left")
        tk.Label(chip, text=role, bg=C.soft_blue, fg=C.white,
                 font=(FONT, 13, "bold"), padx=12, pady=8).pack(side="left")

        # Кнопки справа
        actions = tk.Frame(top, bg=C.deep_blue)
        actions.pack(side="right", padx=18, fill="y")
        btns = [
            ("Історія", app.show_history, C.soft_blue, C.blue_hover, C.white, "🕓"),
            ("Помилки", app.show_errors_frame, C.amber, C.amber_hover, C.deep_blue, "⚠"),
            ("Синхр.", lambda: app.sync_offline(False), C.emerald, C.emerald_hover, C.white, "↻"),
            ("Вихід", self.logout, C.red, C.red_hover, C.white, "⏻"),
        ]
        inner_actions = tk.Frame(actions, bg=C.deep_blue)
        inner_actions.pack(expand=True)
        for text, cmd, bg, hv, fg, icon in btns:
            ModernButton(inner_actions, text, cmd, bg=bg, hover=hv, fg=fg,
                         font_size=14, pad_x=16, pad_y=11, icon=icon).pack(side="left", padx=5)

        # ── Полоса статуса очереди ──
        self.badge_bar = tk.Frame(self, bg=C.bg_dark, height=44)
        self.badge_bar.pack(fill="x")
        self.badge_bar.pack_propagate(False)
        self.queue_lbl = tk.Label(self.badge_bar, text="", bg=C.bg_dark,
                                  fg=C.mint, font=(FONT, 14, "bold"))
        self.queue_lbl.pack(side="left", padx=24)

        # ── Центральная карточка ──
        wrapper = tk.Frame(self, bg=C.surface)
        wrapper.pack(fill="both", expand=True, padx=40, pady=28)

        card = tk.Frame(wrapper, bg=C.white)
        card.pack(fill="both", expand=True)
        inner = tk.Frame(card, bg=C.white, padx=48, pady=38)
        inner.pack(fill="both", expand=True)

        tk.Label(inner, text="Сканування", bg=C.white, fg=C.deep_blue,
                 font=(FONT, 34, "bold")).pack(anchor="w")
        tk.Label(inner,
                 text="1) Скануйте BoxID   →   2) Скануйте ТТН.  Поля очищаються автоматично.",
                 bg=C.white, fg=C.text_muted, font=(FONT, 17)).pack(anchor="w", pady=(6, 26))

        self.box = self.big_entry(inner, "BOXID", "📦", C.blue)
        self.ttn = self.big_entry(inner, "ТТН", "🚚", C.emerald)

        # Статус-блок
        self.status = tk.Label(
            inner, text="Готово до сканування", bg=C.panel_soft, fg=C.text_dark,
            font=(FONT, 28, "bold"), pady=26,
        )
        self.status.pack(fill="x", pady=(28, 0))

        self.box.bind_entry("<Return>", lambda e: self.handle_box())
        self.ttn.bind_entry("<Return>", lambda e: self.handle_ttn())
        self.bind_all("<Key>", self.global_key)
        self.box.focus_set()
        self.update_queue_badge()

    def destroy(self) -> None:
        self.unbind_all("<Key>")
        super().destroy()

    def big_entry(self, parent, title, icon, accent):
        tk.Label(parent, text=f"{icon}  {title}", bg=C.white, fg=C.text_dark,
                 font=(FONT, 20, "bold")).pack(anchor="w", pady=(16, 8))
        field = FieldEntry(parent, font_size=34, justify="center", accent=accent)
        field.pack(fill="x")
        field.bind_entry("<FocusIn>", lambda ev: field.clear())
        return field

    def global_key(self, event: tk.Event) -> None:
        if event.keysym in ("Return", "KP_Enter"):
            code = only_digits(self.scan_buffer)
            self.scan_buffer = ""
            if self.scan_after:
                self.after_cancel(self.scan_after)
                self.scan_after = None
            if code:
                self.route_code(code)
            return
        if event.char and event.char.isprintable():
            self.scan_buffer += event.char
            if self.scan_after:
                self.after_cancel(self.scan_after)
            self.scan_after = self.after(SCAN_BUFFER_MS, lambda: setattr(self, "scan_buffer", ""))

    def route_code(self, code: str) -> None:
        if not self.box.get().strip() or self.focus_get() == self.box.entry:
            self.box.set(code)
            beep_success(self.app)
            self.ttn.focus_set()
        else:
            self.ttn.set(code)
            self.handle_ttn()

    def handle_box(self) -> None:
        val = only_digits(self.box.get())
        if val:
            self.box.set(val)
            beep_success(self.app)
            self.ttn.focus_set()

    def handle_ttn(self) -> None:
        box, ttn = only_digits(self.box.get()), only_digits(self.ttn.get())
        if not box:
            self.box.focus_set()
            return
        if not ttn:
            return
        record = {"user_name": self.app.settings.get("user_name", "operator"), "boxid": box, "ttn": ttn}
        self._set_status("⏳ Відправка...", C.blue, C.panel_soft)
        self.box.clear()
        self.ttn.clear()
        self.box.focus_set()

        def ok(data: dict[str, Any]) -> None:
            note = str(data.get("note") or "") if isinstance(data, dict) else ""
            if note:
                beep_error(self.app)
                self._set_status(f"⚠️ Дублікат: {note}", "#8A5A00", "#FFF3DC")
            else:
                beep_success(self.app)
                self._set_status("✅ Успішно додано", "#0A8F73", "#DEFBF1")
            self.update_queue_badge()

        def fail(exc: Exception) -> None:
            self.app.offline.add(record)
            beep_error(self.app)
            self._set_status("📦 Збережено локально (офлайн)", "#8A5A00", "#FFF3DC")
            self.update_queue_badge()

        self.app.run_bg(lambda: self.app.api.add_record(record), ok=ok, fail=fail)

    def _set_status(self, text, fg, bg):
        self.status.config(text=text, fg=fg, bg=bg)

    def update_queue_badge(self) -> None:
        n = self.app.offline.count()
        if n:
            self.queue_lbl.config(text=f"⏺ Офлайн черга: {n} запис(ів)", fg=C.amber)
        else:
            self.queue_lbl.config(text="● Онлайн — черга порожня", fg=C.mint)

    def logout(self) -> None:
        if messagebox.askyesno("Вихід", "Вийти з акаунту?"):
            self.app.settings.clear_session()
            self.app.show_login()


# ───────────────────────── Базовая таблица ─────────────────────────
class TableFrame(BaseFrame):
    columns: tuple[str, ...] = ()
    headings: tuple[str, ...] = ()
    widths: tuple[int, ...] = ()

    def make_table(self, title: str, icon: str = "") -> None:
        bar = tk.Frame(self, bg=C.deep_blue, height=84)
        bar.pack(fill="x")
        bar.pack_propagate(False)
        self.toolbar = bar

        left = tk.Frame(bar, bg=C.deep_blue)
        left.pack(side="left", padx=20, fill="y")
        inner_left = tk.Frame(left, bg=C.deep_blue)
        inner_left.pack(expand=True)
        ModernButton(inner_left, "Назад", self.app.show_scanner,
                     bg=C.soft_blue, hover=C.blue_hover, font_size=15,
                     icon="←").pack(side="left")
        tk.Label(inner_left, text=f"  {icon}  {title}", bg=C.deep_blue, fg=C.white,
                 font=(FONT, 26, "bold")).pack(side="left", padx=16)

        self.actions = tk.Frame(bar, bg=C.deep_blue)
        self.actions.pack(side="right", padx=16, fill="y")
        self.actions_inner = tk.Frame(self.actions, bg=C.deep_blue)
        self.actions_inner.pack(expand=True)
        ModernButton(self.actions_inner, "Оновити", self.load,
                     bg=C.emerald, hover=C.emerald_hover, font_size=14,
                     icon="↻").pack(side="right", padx=5)

        # Таблица в карточке
        wrapper = tk.Frame(self, bg=C.surface)
        wrapper.pack(fill="both", expand=True, padx=24, pady=20)
        card = tk.Frame(wrapper, bg=C.white)
        card.pack(fill="both", expand=True)

        table_holder = tk.Frame(card, bg=C.white, padx=6, pady=6)
        table_holder.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(table_holder, columns=self.columns,
                                 show="headings", style="Modern.Treeview")
        vsb = ttk.Scrollbar(table_holder, orient="vertical",
                            command=self.tree.yview, style="Modern.Vertical.TScrollbar")
        self.tree.configure(yscrollcommand=vsb.set)

        widths = self.widths or tuple(150 for _ in self.columns)
        for c, h, w in zip(self.columns, self.headings, widths):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="center")

        self.tree.tag_configure("odd", background=C.white)
        self.tree.tag_configure("even", background="#F4F7FC")

        vsb.pack(side="right", fill="y")
        self.tree.pack(side="left", fill="both", expand=True)

    def add_action(self, text, cmd, bg, hover, fg="white", icon=""):
        ModernButton(self.actions_inner, text, cmd, bg=bg, hover=hover,
                     fg=fg, font_size=14, icon=icon).pack(side="right", padx=5)

    def _striped_insert(self, values):
        idx = len(self.tree.get_children())
        tag = "even" if idx % 2 == 0 else "odd"
        self.tree.insert("", "end", values=values, tags=(tag,))


# ───────────────────────── История ─────────────────────────
class HistoryFrame(TableFrame):
    columns = ("datetime", "user_name", "boxid", "ttn")
    headings = ("Дата", "Користувач", "BoxID", "ТТН")
    widths = (240, 240, 220, 220)

    def __init__(self, app: App):
        super().__init__(app, bg=C.surface)
        self.make_table("Історія сканувань", "🕓")
        if app.settings.get("user_role") == "admin":
            self.add_action("Очистити", self.clear, C.red, C.red_hover, icon="🗑")
        self.load()

    def load(self) -> None:
        self.app.run_bg(self.app.api.history, ok=self.fill)

    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        rows.sort(key=lambda r: str(r.get("datetime") or ""), reverse=True)
        for r in rows:
            self._striped_insert((parse_dt(r.get("datetime")), r.get("user_name", ""),
                                  r.get("boxid", ""), r.get("ttn", "")))

    def clear(self) -> None:
        if messagebox.askyesno("Очистити", "Видалити всю історію?"):
            self.app.run_bg(self.app.api.clear_history, ok=lambda _: self.load())


# ───────────────────────── Ошибки ─────────────────────────
class ErrorsFrame(TableFrame):
    columns = ("id", "datetime", "user_name", "boxid", "ttn", "error")
    headings = ("ID", "Дата", "Користувач", "BoxID", "ТТН", "Помилка")
    widths = (70, 200, 200, 170, 170, 280)

    def __init__(self, app: App):
        super().__init__(app, bg=C.surface)
        self.make_table("Журнал помилок", "⚠")
        role = app.settings.get("user_role")
        if role in ("admin", "operator"):
            self.add_action("Видалити обрану", self.delete_selected, C.red, C.red_hover, icon="🗑")
            self.add_action("Очистити все", self.clear, C.red_dark, "#8E0019", icon="🧹")
        self.load()

    def load(self) -> None:
        self.app.run_bg(self.app.api.errors, ok=self.fill)

    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        rows.sort(key=lambda r: str(r.get("datetime") or ""), reverse=True)
        for r in rows:
            self._striped_insert((r.get("id", ""), parse_dt(r.get("datetime")),
                                  r.get("user_name", ""), r.get("boxid", ""),
                                  r.get("ttn", ""), r.get("error") or r.get("message", "")))

    def delete_selected(self) -> None:
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Видалення", "Оберіть запис у таблиці.")
            return
        eid = int(self.tree.item(sel[0], "values")[0])
        if messagebox.askyesno("Видалити", f"Видалити помилку #{eid}?"):
            self.app.run_bg(lambda: self.app.api.delete_error(eid), ok=lambda _: self.load())

    def clear(self) -> None:
        if messagebox.askyesno("Очистити", "Видалити весь журнал помилок?"):
            self.app.run_bg(self.app.api.clear_errors, ok=lambda _: self.load())


# ───────────────────────── Админ-панель ─────────────────────────
class AdminFrame(TableFrame):
    columns = ("id", "surname", "role", "active", "created")
    headings = ("ID", "Прізвище", "Роль", "Активний", "Створено")
    widths = (70, 280, 180, 150, 240)

    def __init__(self, app: App, admin_token: str):
        self.admin_token = admin_token
        super().__init__(app, bg=C.surface)
        self.make_table("Адмін-панель", "⚙")
        self.add_action("Заявки", self.pending, C.amber, C.amber_hover, fg=C.deep_blue, icon="📨")
        self.add_action("Роль", self.change_role, C.blue, C.blue_hover, icon="🎚")
        self.add_action("Вкл/Викл", self.toggle, C.emerald, C.emerald_hover, icon="⏯")
        self.add_action("Видалити", self.delete, C.red, C.red_hover, icon="🗑")
        self.load()

    def load(self) -> None:
        self.app.run_bg(lambda: self.app.api.users(self.admin_token), ok=self.fill)

    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        for r in rows:
            self._striped_insert((
                r.get("id", ""), r.get("surname", ""),
                ROLE_LABELS.get(str(r.get("role")), r.get("role", "")),
                "Так" if r.get("is_active") else "Ні",
                parse_dt(r.get("created_at")),
            ))

    def selected_id(self) -> Optional[int]:
        sel = self.tree.selection()
        return int(self.tree.item(sel[0], "values")[0]) if sel else None

    def pending(self) -> None:
        PendingDialog(self.app, self.admin_token)

    def change_role(self) -> None:
        uid = self.selected_id()
        if uid is None:
            messagebox.showinfo("Роль", "Оберіть користувача.")
            return
        role = ChoiceDialog.ask(self.app, "Роль", "Оберіть роль",
                                [("admin", "Адмін"), ("operator", "Оператор"), ("viewer", "Перегляд")])
        if role:
            self.app.run_bg(lambda: self.app.api.update_user(self.admin_token, uid, {"role": role}), ok=lambda _: self.load())

    def toggle(self) -> None:
        uid = self.selected_id()
        if uid is None:
            messagebox.showinfo("Статус", "Оберіть користувача.")
            return
        active_now = self.tree.item(self.tree.selection()[0], "values")[3] == "Так"
        self.app.run_bg(lambda: self.app.api.update_user(self.admin_token, uid, {"is_active": not active_now}), ok=lambda _: self.load())

    def delete(self) -> None:
        uid = self.selected_id()
        if uid and messagebox.askyesno("Видалити", f"Видалити користувача #{uid}?"):
            self.app.run_bg(lambda: self.app.api.delete_user(self.admin_token, uid), ok=lambda _: self.load())


# ───────────────────────── Диалоги ─────────────────────────
class StyledDialog(tk.Toplevel):
    """Базовый красивый диалог."""

    def __init__(self, app, title, width=480, height=380):
        super().__init__(app)
        self.app = app
        self.title(title)
        self.configure(bg=C.white)
        self.resizable(False, False)
        self.grab_set()
        # Центрируем относительно главного окна
        self.update_idletasks()
        x = app.winfo_rootx() + (app.winfo_width() - width) // 2
        y = app.winfo_rooty() + (app.winfo_height() - height) // 2
        self.geometry(f"{width}x{height}+{max(x,0)}+{max(y,0)}")

        header = tk.Frame(self, bg=C.deep_blue, height=64)
        header.pack(fill="x")
        header.pack_propagate(False)
        tk.Label(header, text=title, bg=C.deep_blue, fg=C.white,
                 font=(FONT, 18, "bold")).pack(side="left", padx=22, pady=16)

        self.body = tk.Frame(self, bg=C.white, padx=32, pady=26)
        self.body.pack(fill="both", expand=True)


class RegisterDialog(StyledDialog):
    def __init__(self, app: App):
        super().__init__(app, "Реєстрація", width=500, height=470)
        self.surname = self.entry("Прізвище", "👤")
        self.password = self.entry("Пароль", "🔒", "*")
        self.confirm = self.entry("Повтор пароля", "🔒", "*")
        ModernButton(self.body, "Відправити заявку", self.submit,
                     bg=C.emerald, hover=C.emerald_hover, font_size=16,
                     pad_y=13, icon="✓").pack(fill="x", pady=(22, 0))

    def entry(self, label, icon, show=""):
        tk.Label(self.body, text=f"{icon}  {label}", bg=C.white, fg=C.text_dark,
                 font=(FONT, 13, "bold")).pack(anchor="w", pady=(10, 5))
        field = FieldEntry(self.body, font_size=17, show=show, accent=C.emerald)
        field.pack(fill="x")
        return field

    def submit(self) -> None:
        s, p, c = self.surname.get().strip(), self.password.get().strip(), self.confirm.get().strip()
        if not s or not p or not c or p != c or len(p) < 6:
            messagebox.showwarning("Реєстрація", "Заповніть поля, пароль від 6 символів, паролі мають співпадати")
            return
        self.app.run_bg(
            lambda: self.app.api.register(s, p),
            ok=lambda _: (messagebox.showinfo("Реєстрація", "Заявку відправлено. Очікуйте підтвердження адміністратора."), self.destroy()),
        )


class PendingDialog(StyledDialog):
    def __init__(self, app: App, token: str):
        super().__init__(app, "Заявки на реєстрацію", width=820, height=520)
        self.token = token

        holder = tk.Frame(self.body, bg=C.white)
        holder.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(holder, columns=("id", "surname", "created"),
                                 show="headings", style="Modern.Treeview")
        for c, h, w in zip(("id", "surname", "created"),
                           ("ID", "Прізвище", "Створено"), (80, 320, 260)):
            self.tree.heading(c, text=h)
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="both", expand=True)

        bar = tk.Frame(self.body, bg=C.white)
        bar.pack(fill="x", pady=(16, 0))
        ModernButton(bar, "Схвалити", self.approve, bg=C.emerald,
                     hover=C.emerald_hover, font_size=14, icon="✓").pack(side="left", padx=4)
        ModernButton(bar, "Відхилити", self.reject, bg=C.red,
                     hover=C.red_hover, font_size=14, icon="✕").pack(side="left", padx=4)
        ModernButton(bar, "Оновити", self.load, bg=C.soft_blue,
                     hover=C.blue_hover, font_size=14, icon="↻").pack(side="right", padx=4)
        self.load()

    def selected_id(self) -> Optional[int]:
        sel = self.tree.selection()
        return int(self.tree.item(sel[0], "values")[0]) if sel else None

    def load(self) -> None:
        self.app.run_bg(lambda: self.app.api.pending_users(self.token), ok=self.fill)

    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        for r in rows:
            self.tree.insert("", "end", values=(r.get("id", ""), r.get("surname", ""),
                                                parse_dt(r.get("created_at"))))

    def approve(self) -> None:
        rid = self.selected_id()
        if rid is None:
            messagebox.showinfo("Заявки", "Оберіть заявку.")
            return
        role = ChoiceDialog.ask(self.app, "Роль", "Роль нового користувача",
                                [("operator", "Оператор"), ("viewer", "Перегляд"), ("admin", "Адмін")])
        if role:
            self.app.run_bg(lambda: self.app.api.approve_user(self.token, rid, role), ok=lambda _: self.load())

    def reject(self) -> None:
        rid = self.selected_id()
        if rid and messagebox.askyesno("Відхилити", f"Відхилити заявку #{rid}?"):
            self.app.run_bg(lambda: self.app.api.reject_user(self.token, rid), ok=lambda _: self.load())


class SimpleInput:
    @staticmethod
    def ask(root: tk.Tk, title: str, prompt: str, secret: bool = False) -> str:
        dlg = StyledDialog(root, title, width=460, height=280)
        result = {"v": ""}
        tk.Label(dlg.body, text=prompt, bg=C.white, fg=C.text_dark,
                 font=(FONT, 15, "bold")).pack(anchor="w", pady=(4, 10))
        field = FieldEntry(dlg.body, font_size=18, show="*" if secret else "", accent=C.blue)
        field.pack(fill="x")
        field.focus_set()

        def ok():
            result["v"] = field.get()
            dlg.destroy()

        ModernButton(dlg.body, "OK", ok, bg=C.blue, hover=C.blue_hover,
                     font_size=15, pad_y=12, icon="✓").pack(fill="x", pady=(22, 0))
        field.bind_entry("<Return>", lambda ev: ok())
        root.wait_window(dlg)
        return result["v"]


class ChoiceDialog:
    @staticmethod
    def ask(root: tk.Tk, title: str, prompt: str, choices: list[tuple[str, str]]) -> Optional[str]:
        height = 180 + len(choices) * 64
        dlg = StyledDialog(root, title, width=420, height=height)
        result: dict[str, Optional[str]] = {"v": None}
        tk.Label(dlg.body, text=prompt, bg=C.white, fg=C.text_dark,
                 font=(FONT, 15, "bold")).pack(anchor="w", pady=(4, 14))

        palette = [C.blue, C.emerald, C.amber]
        hovers = [C.blue_hover, C.emerald_hover, C.amber_hover]
        for i, (value, label) in enumerate(choices):
            bg = palette[i % len(palette)]
            hv = hovers[i % len(hovers)]
            fg = C.deep_blue if bg == C.amber else C.white

            def choose(v=value):
                result["v"] = v
                dlg.destroy()

            ModernButton(dlg.body, label, choose, bg=bg, hover=hv, fg=fg,
                         font_size=15, pad_y=12).pack(fill="x", pady=6)
        root.wait_window(dlg)
        return result["v"]


def main() -> int:
    try:
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
