#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BoxID-ТТН Windows
Однофайловое Python-приложение для Windows, повторяющее функционал Android-модуля
сканирования BoxID-ТТН: вход/регистрация, сканирование, офлайн-очередь,
история, журнал ошибок и админ-панель.

Запуск:
    python tracking_windows_app.py

Зависимости:
    pip install requests
Tkinter обычно входит в стандартную поставку Python для Windows.
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
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable, Optional

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore

APP_NAME = "BoxID-ТТН Windows"
API_BASE_URL = "https://tracking-app.dclink.ua"
REQUEST_TIMEOUT = 8
SYNC_INTERVAL_MS = 20_000
SCAN_BUFFER_MS = 180

ROLE_LABELS = {"admin": "Адмін", "operator": "Оператор", "viewer": "Перегляд"}
ROLE_LEVELS = {"admin": 1, "operator": 0, "viewer": 2}


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
        if requests is None:
            raise RuntimeError("Установите библиотеку requests: pip install requests")
        self.settings = settings

    def _headers(self, token: Optional[str] = None) -> dict[str, str]:
        token = token if token is not None else self.settings.get("token")
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _request(self, method: str, path: str, *, token: Optional[str] = None, json_body: Any = None) -> Any:
        try:
            r = requests.request(method, API_BASE_URL + path, headers=self._headers(token), json=json_body, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            raise ApiError(f"Нет связи с сервером: {exc}") from exc
        body: Any = None
        if r.text:
            try:
                body = r.json()
            except Exception:
                body = r.text
        if 200 <= r.status_code < 300:
            return body
        msg = f"Ошибка сервера ({r.status_code})"
        if isinstance(body, dict):
            msg = str(body.get("detail") or body.get("message") or msg)
        elif body:
            msg = str(body)
        raise ApiError(msg, r.status_code)

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
        self.geometry("1180x760")
        self.minsize(980, 650)
        self.configure(bg="#07153A")
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
        self.style.configure("Big.TButton", font=("Segoe UI", 18, "bold"), padding=12)
        self.style.configure("TEntry", font=("Segoe UI", 18), padding=8)
        self.style.configure("Treeview", rowheight=38, font=("Segoe UI", 13))
        self.style.configure("Treeview.Heading", font=("Segoe UI", 13, "bold"))

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
    def __init__(self, app: App, bg: str = "#07153A"):
        super().__init__(app, bg=bg)
        self.app = app

    def label(self, parent: tk.Misc, text: str, size: int = 18, fg: str = "white", bg: str | None = None, bold: bool = False) -> tk.Label:
        return tk.Label(parent, text=text, font=("Segoe UI", size, "bold" if bold else "normal"), fg=fg, bg=bg or parent.cget("bg"))

    def button(self, parent: tk.Misc, text: str, cmd: Callable, bg: str = "#075BFF", fg: str = "white") -> tk.Button:
        return tk.Button(parent, text=text, command=cmd, bg=bg, fg=fg, activebackground=bg, activeforeground=fg, bd=0, relief="flat", font=("Segoe UI", 16, "bold"), padx=18, pady=10, cursor="hand2")


class LoginFrame(BaseFrame):
    def __init__(self, app: App):
        super().__init__(app)
        card = tk.Frame(self, bg="white", padx=38, pady=32)
        card.place(relx=.5, rely=.5, anchor="center", width=560)
        tk.Label(card, text="BoxID-ТТН", bg="white", fg="#07153A", font=("Segoe UI", 34, "bold")).pack(pady=(0, 8))
        tk.Label(card, text="Вход в модуль сканирования", bg="white", fg="#60708C", font=("Segoe UI", 16)).pack(pady=(0, 24))
        self.surname = self._entry(card, "Прізвище / фамилия")
        self.password = self._entry(card, "Пароль", show="*")
        self.button(card, "УВІЙТИ / ВОЙТИ", self.login).pack(fill="x", pady=(18, 10))
        self.button(card, "Реєстрація", self.register, bg="#14C9A6").pack(fill="x", pady=6)
        self.button(card, "Панель адміністратора", self.admin_login, bg="#FFB020", fg="#07153A").pack(fill="x", pady=6)
        self.password.bind("<Return>", lambda e: self.login())

    def _entry(self, parent: tk.Misc, placeholder: str, show: str = "") -> tk.Entry:
        tk.Label(parent, text=placeholder, bg="white", fg="#0B1530", font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(8, 4))
        e = tk.Entry(parent, show=show, font=("Segoe UI", 20), bd=1, relief="solid")
        e.pack(fill="x", ipady=10)
        return e

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


class ScannerFrame(BaseFrame):
    def __init__(self, app: App):
        super().__init__(app)
        self.scan_buffer = ""
        self.scan_after: Optional[str] = None
        top = tk.Frame(self, bg="#07153A", padx=22, pady=18); top.pack(fill="x")
        self.label(top, "BoxID-ТТН", 30, bold=True).pack(side="left")
        user = app.settings.get("user_name", "operator")
        role = ROLE_LABELS.get(app.settings.get("user_role", "viewer"), "Перегляд")
        self.user_lbl = self.label(top, f"  {user} • {role}", 16, fg="#D7E6FF"); self.user_lbl.pack(side="left", padx=18)
        self.queue_lbl = self.label(top, "", 15, fg="#5EF2D0"); self.queue_lbl.pack(side="left", padx=12)
        for text, cmd, color in [("История", app.show_history, "#3F8CFF"), ("Ошибки", app.show_errors_frame, "#FFB020"), ("Синхр.", lambda: app.sync_offline(False), "#14C9A6"), ("Выход", self.logout, "#E5484D")]:
            self.button(top, text, cmd, bg=color, fg=("#07153A" if color == "#FFB020" else "white")).pack(side="right", padx=5)
        card = tk.Frame(self, bg="white", padx=34, pady=30); card.pack(fill="both", expand=True, padx=26, pady=(0, 24))
        tk.Label(card, text="Сканирование", bg="white", fg="#07153A", font=("Segoe UI", 32, "bold")).pack(anchor="w")
        tk.Label(card, text="1) Сканируйте BoxID  →  2) Сканируйте ТТН. Поля очищаются автоматически.", bg="white", fg="#60708C", font=("Segoe UI", 17)).pack(anchor="w", pady=(0, 22))
        self.box = self.big_entry(card, "BOXID")
        self.ttn = self.big_entry(card, "ТТН")
        self.status = tk.Label(card, text="Готово к сканированию", bg="#F2F5FB", fg="#0B1530", font=("Segoe UI", 26, "bold"), pady=22)
        self.status.pack(fill="x", pady=22)
        self.box.bind("<Return>", lambda e: self.handle_box())
        self.ttn.bind("<Return>", lambda e: self.handle_ttn())
        self.bind_all("<Key>", self.global_key)
        self.box.focus_set(); self.update_queue_badge()

    def destroy(self) -> None:
        self.unbind_all("<Key>")
        super().destroy()

    def big_entry(self, parent: tk.Misc, title: str) -> tk.Entry:
        tk.Label(parent, text=title, bg="white", fg="#0B1530", font=("Segoe UI", 20, "bold")).pack(anchor="w", pady=(18, 6))
        e = tk.Entry(parent, font=("Segoe UI", 36, "bold"), bd=2, relief="solid", justify="center")
        e.pack(fill="x", ipady=18)
        e.bind("<FocusIn>", lambda ev: e.delete(0, "end"))
        return e

    def global_key(self, event: tk.Event) -> None:
        if event.keysym in ("Return", "KP_Enter"):
            code = only_digits(self.scan_buffer)
            self.scan_buffer = ""
            if self.scan_after: self.after_cancel(self.scan_after); self.scan_after = None
            if code: self.route_code(code)
            return
        if event.char and event.char.isprintable():
            self.scan_buffer += event.char
            if self.scan_after: self.after_cancel(self.scan_after)
            self.scan_after = self.after(SCAN_BUFFER_MS, lambda: setattr(self, "scan_buffer", ""))

    def route_code(self, code: str) -> None:
        if not self.box.get().strip() or self.focus_get() == self.box:
            self.box.delete(0, "end"); self.box.insert(0, code); beep_success(self.app); self.ttn.focus_set()
        else:
            self.ttn.delete(0, "end"); self.ttn.insert(0, code); self.handle_ttn()

    def handle_box(self) -> None:
        val = only_digits(self.box.get())
        if val:
            self.box.delete(0, "end"); self.box.insert(0, val); beep_success(self.app); self.ttn.focus_set()

    def handle_ttn(self) -> None:
        box, ttn = only_digits(self.box.get()), only_digits(self.ttn.get())
        if not box:
            self.box.focus_set(); return
        if not ttn: return
        record = {"user_name": self.app.settings.get("user_name", "operator"), "boxid": box, "ttn": ttn}
        self.status.config(text="⏳ Отправка...", fg="#075BFF")
        self.box.delete(0, "end"); self.ttn.delete(0, "end"); self.box.focus_set()
        def ok(data: dict[str, Any]) -> None:
            note = str(data.get("note") or "") if isinstance(data, dict) else ""
            if note:
                beep_error(self.app); self.status.config(text=f"⚠️ Дубликат: {note}", fg="#B26A00")
            else:
                beep_success(self.app); self.status.config(text="✅ Успешно добавлено", fg="#0A8F73")
            self.update_queue_badge()
        def fail(exc: Exception) -> None:
            self.app.offline.add(record); beep_error(self.app); self.status.config(text="📦 Сохранено локально (офлайн)", fg="#B26A00"); self.update_queue_badge()
        self.app.run_bg(lambda: self.app.api.add_record(record), ok=ok, fail=fail)

    def update_queue_badge(self) -> None:
        n = self.app.offline.count()
        self.queue_lbl.config(text=(f"Офлайн очередь: {n}" if n else "Онлайн / очередь пуста"))

    def logout(self) -> None:
        if messagebox.askyesno("Выход", "Выйти из аккаунта?"):
            self.app.settings.clear_session(); self.app.show_login()


class TableFrame(BaseFrame):
    columns: tuple[str, ...] = ()
    headings: tuple[str, ...] = ()
    def make_table(self) -> None:
        bar = tk.Frame(self, bg="#07153A", padx=18, pady=14); bar.pack(fill="x")
        self.button(bar, "← Назад", self.app.show_scanner, bg="#3F8CFF").pack(side="left")
        self.title_lbl = self.label(bar, "", 28, bold=True); self.title_lbl.pack(side="left", padx=18)
        self.button(bar, "Обновить", self.load, bg="#14C9A6").pack(side="right", padx=4)
        self.tree = ttk.Treeview(self, columns=self.columns, show="headings")
        for c, h in zip(self.columns, self.headings):
            self.tree.heading(c, text=h); self.tree.column(c, width=150, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=18, pady=18)


class HistoryFrame(TableFrame):
    columns = ("datetime", "user_name", "boxid", "ttn")
    headings = ("Дата", "Пользователь", "BoxID", "ТТН")
    def __init__(self, app: App):
        super().__init__(app, bg="#F7F8FA"); self.make_table(); self.title_lbl.config(text="История сканирований")
        if app.settings.get("user_role") == "admin": self.button(self.children['!frame'], "Очистить", self.clear, bg="#E5484D").pack(side="right", padx=4)
        self.load()
    def load(self) -> None:
        self.app.run_bg(self.app.api.history, ok=self.fill)
    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        rows.sort(key=lambda r: str(r.get("datetime") or ""), reverse=True)
        for r in rows: self.tree.insert("", "end", values=(parse_dt(r.get("datetime")), r.get("user_name", ""), r.get("boxid", ""), r.get("ttn", "")))
    def clear(self) -> None:
        if messagebox.askyesno("Очистить", "Удалить всю историю?"): self.app.run_bg(self.app.api.clear_history, ok=lambda _: self.load())


class ErrorsFrame(TableFrame):
    columns = ("id", "datetime", "user_name", "boxid", "ttn", "error")
    headings = ("ID", "Дата", "Пользователь", "BoxID", "ТТН", "Ошибка")
    def __init__(self, app: App):
        super().__init__(app, bg="#F7F8FA"); self.make_table(); self.title_lbl.config(text="Журнал ошибок")
        role = app.settings.get("user_role")
        if role in ("admin", "operator"):
            self.button(self.children['!frame'], "Удалить выбранную", self.delete_selected, bg="#E5484D").pack(side="right", padx=4)
            self.button(self.children['!frame'], "Очистить все", self.clear, bg="#B00020").pack(side="right", padx=4)
        self.load()
    def load(self) -> None: self.app.run_bg(self.app.api.errors, ok=self.fill)
    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children()); rows.sort(key=lambda r: str(r.get("datetime") or ""), reverse=True)
        for r in rows: self.tree.insert("", "end", values=(r.get("id", ""), parse_dt(r.get("datetime")), r.get("user_name", ""), r.get("boxid", ""), r.get("ttn", ""), r.get("error") or r.get("message", "")))
    def delete_selected(self) -> None:
        sel = self.tree.selection()
        if not sel: return
        eid = int(self.tree.item(sel[0], "values")[0])
        if messagebox.askyesno("Удалить", f"Удалить ошибку #{eid}?"): self.app.run_bg(lambda: self.app.api.delete_error(eid), ok=lambda _: self.load())
    def clear(self) -> None:
        if messagebox.askyesno("Очистить", "Удалить весь журнал ошибок?"): self.app.run_bg(self.app.api.clear_errors, ok=lambda _: self.load())


class AdminFrame(TableFrame):
    columns = ("id", "surname", "role", "active", "created")
    headings = ("ID", "Фамилия", "Роль", "Активен", "Создан")
    def __init__(self, app: App, admin_token: str):
        self.admin_token = admin_token; super().__init__(app, bg="#F7F8FA"); self.make_table(); self.title_lbl.config(text="Админ-панель")
        bar = self.children['!frame']
        self.button(bar, "Заявки", self.pending, bg="#FFB020", fg="#07153A").pack(side="right", padx=4)
        self.button(bar, "Роль", self.change_role, bg="#075BFF").pack(side="right", padx=4)
        self.button(bar, "Вкл/Выкл", self.toggle, bg="#14C9A6").pack(side="right", padx=4)
        self.button(bar, "Удалить", self.delete, bg="#E5484D").pack(side="right", padx=4)
        self.load()
    def load(self) -> None: self.app.run_bg(lambda: self.app.api.users(self.admin_token), ok=self.fill)
    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        for r in rows: self.tree.insert("", "end", values=(r.get("id", ""), r.get("surname", ""), ROLE_LABELS.get(str(r.get("role")), r.get("role", "")), "Да" if r.get("is_active") else "Нет", parse_dt(r.get("created_at"))))
    def selected_id(self) -> Optional[int]:
        sel = self.tree.selection(); return int(self.tree.item(sel[0], "values")[0]) if sel else None
    def pending(self) -> None: PendingDialog(self.app, self.admin_token)
    def change_role(self) -> None:
        uid = self.selected_id(); role = ChoiceDialog.ask(self.app, "Роль", "Выберите роль", [("admin", "Адмін"), ("operator", "Оператор"), ("viewer", "Перегляд")])
        if uid and role: self.app.run_bg(lambda: self.app.api.update_user(self.admin_token, uid, {"role": role}), ok=lambda _: self.load())
    def toggle(self) -> None:
        uid = self.selected_id()
        if uid is None: return
        active_now = self.tree.item(self.tree.selection()[0], "values")[3] == "Да"
        self.app.run_bg(lambda: self.app.api.update_user(self.admin_token, uid, {"is_active": not active_now}), ok=lambda _: self.load())
    def delete(self) -> None:
        uid = self.selected_id()
        if uid and messagebox.askyesno("Удалить", f"Удалить пользователя #{uid}?"): self.app.run_bg(lambda: self.app.api.delete_user(self.admin_token, uid), ok=lambda _: self.load())


class RegisterDialog(tk.Toplevel):
    def __init__(self, app: App):
        super().__init__(app); self.app = app; self.title("Регистрация"); self.geometry("460x360"); self.configure(bg="white"); self.grab_set()
        tk.Label(self, text="Регистрация", bg="white", fg="#07153A", font=("Segoe UI", 24, "bold")).pack(pady=18)
        self.surname = self.entry("Фамилия"); self.password = self.entry("Пароль", "*"); self.confirm = self.entry("Повтор пароля", "*")
        tk.Button(self, text="Отправить заявку", command=self.submit, bg="#14C9A6", fg="white", bd=0, font=("Segoe UI", 15, "bold"), pady=10).pack(fill="x", padx=32, pady=18)
    def entry(self, label: str, show: str = "") -> tk.Entry:
        tk.Label(self, text=label, bg="white", fg="#0B1530", font=("Segoe UI", 12, "bold")).pack(anchor="w", padx=32)
        e = tk.Entry(self, show=show, font=("Segoe UI", 16)); e.pack(fill="x", padx=32, pady=(3, 10), ipady=6); return e
    def submit(self) -> None:
        s, p, c = self.surname.get().strip(), self.password.get().strip(), self.confirm.get().strip()
        if not s or not p or not c or p != c or len(p) < 6:
            messagebox.showwarning("Регистрация", "Заполните поля, пароль от 6 символов, повторы должны совпадать"); return
        self.app.run_bg(lambda: self.app.api.register(s, p), ok=lambda _: (messagebox.showinfo("Регистрация", "Заявка отправлена. Дождитесь подтверждения администратора."), self.destroy()))


class PendingDialog(tk.Toplevel):
    def __init__(self, app: App, token: str):
        super().__init__(app); self.app = app; self.token = token; self.title("Заявки"); self.geometry("760x440"); self.grab_set()
        self.tree = ttk.Treeview(self, columns=("id", "surname", "created"), show="headings"); self.tree.pack(fill="both", expand=True, padx=10, pady=10)
        for c, h in zip(("id", "surname", "created"), ("ID", "Фамилия", "Создан")): self.tree.heading(c, text=h)
        bar = tk.Frame(self); bar.pack(fill="x", padx=10, pady=8)
        tk.Button(bar, text="Одобрить", command=self.approve).pack(side="left", padx=4); tk.Button(bar, text="Отклонить", command=self.reject).pack(side="left", padx=4); tk.Button(bar, text="Обновить", command=self.load).pack(side="right")
        self.load()
    def selected_id(self) -> Optional[int]:
        sel = self.tree.selection(); return int(self.tree.item(sel[0], "values")[0]) if sel else None
    def load(self) -> None:
        self.app.run_bg(lambda: self.app.api.pending_users(self.token), ok=self.fill)
    def fill(self, rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        for r in rows: self.tree.insert("", "end", values=(r.get("id", ""), r.get("surname", ""), parse_dt(r.get("created_at"))))
    def approve(self) -> None:
        rid = self.selected_id(); role = ChoiceDialog.ask(self.app, "Роль", "Роль нового пользователя", [("operator", "Оператор"), ("viewer", "Перегляд"), ("admin", "Адмін")])
        if rid and role: self.app.run_bg(lambda: self.app.api.approve_user(self.token, rid, role), ok=lambda _: self.load())
    def reject(self) -> None:
        rid = self.selected_id()
        if rid and messagebox.askyesno("Отклонить", f"Отклонить заявку #{rid}?"): self.app.run_bg(lambda: self.app.api.reject_user(self.token, rid), ok=lambda _: self.load())


class SimpleInput:
    @staticmethod
    def ask(root: tk.Tk, title: str, prompt: str, secret: bool = False) -> str:
        dlg = tk.Toplevel(root); dlg.title(title); dlg.geometry("430x180"); dlg.configure(bg="white"); dlg.grab_set(); result = {"v": ""}
        tk.Label(dlg, text=prompt, bg="white", font=("Segoe UI", 14, "bold")).pack(pady=(22, 8))
        e = tk.Entry(dlg, show="*" if secret else "", font=("Segoe UI", 16)); e.pack(fill="x", padx=24, ipady=5); e.focus_set()
        def ok(): result["v"] = e.get(); dlg.destroy()
        tk.Button(dlg, text="OK", command=ok, bg="#075BFF", fg="white", bd=0, font=("Segoe UI", 13, "bold"), pady=8).pack(pady=16)
        dlg.bind("<Return>", lambda ev: ok()); root.wait_window(dlg); return result["v"]


class ChoiceDialog:
    @staticmethod
    def ask(root: tk.Tk, title: str, prompt: str, choices: list[tuple[str, str]]) -> Optional[str]:
        dlg = tk.Toplevel(root); dlg.title(title); dlg.geometry("360x260"); dlg.configure(bg="white"); dlg.grab_set(); result: dict[str, Optional[str]] = {"v": None}
        tk.Label(dlg, text=prompt, bg="white", font=("Segoe UI", 14, "bold")).pack(pady=16)
        for value, label in choices:
            tk.Button(dlg, text=label, command=lambda v=value: (result.__setitem__("v", v), dlg.destroy()), bg="#075BFF", fg="white", bd=0, font=("Segoe UI", 13, "bold"), pady=8).pack(fill="x", padx=28, pady=5)
        root.wait_window(dlg); return result["v"]


def main() -> int:
    try:
        app = App(); app.mainloop(); return 0
    except Exception as exc:
        traceback.print_exc()
        try: messagebox.showerror(APP_NAME, str(exc))
        except Exception: pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
