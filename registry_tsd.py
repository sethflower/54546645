#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Windows desktop client for the ScanPak module.

Складський клієнт для сканування номерів посилок у модулі СканПак.

Install: pip install requests
Run:     python scanpak_windows.py
"""
from __future__ import annotations

import json
import os
import queue
import re
import sqlite3
import threading
import tkinter as tk
from collections import Counter, deque
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable

import requests

# --------------------------------------------------------------------------- #
#  Constants & theme
# --------------------------------------------------------------------------- #
APP_NAME = "СканПак"
API_BASE_URL = "https://tracking-app.dclink.ua"
TIMEOUT = 12

APP_DIR = Path(os.getenv("APPDATA") or Path.home()) / "ScanPak_Windows"
APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "scanpak.sqlite3"
CONFIG_PATH = APP_DIR / "session.json"

BG = "#06122F"
SIDEBAR = "#0B1A42"
SIDEBAR_HOVER = "#123066"
SIDEBAR_ACTIVE = "#14C9A6"
CARD = "#FFFFFF"
CARD_ALT = "#F1F6F4"
TEXT = "#0B1530"
TEXT_LIGHT = "#E8EEFF"
MUTED = "#60708C"
MUTED_LIGHT = "#9FB0D9"
GREEN = "#14C9A6"
GREEN_BG = "#14C9A6"
CYAN = "#04C8E8"
BLUE = "#3F8CFF"
RED = "#EF4444"
RED_BG = "#EF4444"
AMBER = "#F59E0B"
AMBER_BG = "#F59E0B"
FIELD = "#EEF6F4"
BORDER = "#D8E0F0"


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #
def only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def parse_dt(value: Any) -> datetime:
    """Parse API datetime without adding +3 hours for ScanPak.

    Backend returns local Kyiv time without timezone, for example:
    2026-06-30T12:10:00. For this case we keep time as-is.
    """
    raw = str(value or "").strip()
    if not raw:
        return datetime.min
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            return dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return datetime.min


def fmt_dt(value: Any) -> str:
    dt = parse_dt(value)
    return str(value) if dt == datetime.min else dt.strftime("%d.%m.%Y %H:%M:%S")


def fmt_time(value: Any) -> str:
    dt = parse_dt(value)
    return str(value) if dt == datetime.min else dt.strftime("%H:%M:%S")


def play_sound(ok: bool) -> None:
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_OK if ok else winsound.MB_ICONHAND)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
#  API client
# --------------------------------------------------------------------------- #
class ApiError(Exception):
    pass


class ApiClient:
    def __init__(self, base_url: str = API_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.token = ""

    def _url(self, path: str) -> str:
        return self.base_url + path

    def _headers(self, auth: bool = True) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, *, auth: bool = True, **kwargs: Any) -> Any:
        try:
            response = self.session.request(
                method,
                self._url(path),
                headers=self._headers(auth),
                timeout=TIMEOUT,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise ApiError("Немає зв'язку з сервером") from exc

        if response.status_code < 200 or response.status_code >= 300:
            try:
                body = response.json()
                msg = body.get("detail") or body.get("message") or response.text
            except Exception:
                msg = response.text
            raise ApiError(msg or f"Помилка сервера ({response.status_code})")

        if not response.text:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ApiError("Некоректна відповідь сервера") from exc

    def login(self, surname: str, password: str) -> dict[str, Any]:
        data = self._request(
            "POST",
            "/scanpak/login",
            auth=False,
            data=json.dumps({"surname": surname, "password": password}),
        )
        token = str(data.get("token") or "")
        if not token:
            raise ApiError("Сервер не повернув токен")
        self.token = token
        return data

    def register(self, surname: str, password: str) -> None:
        self._request(
            "POST",
            "/scanpak/register",
            auth=False,
            data=json.dumps({"surname": surname, "password": password}),
        )

    def add_scan(self, parcel_number: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/scanpak/scans",
            data=json.dumps({"parcel_number": parcel_number}),
        ) or {}

    def get_history(self) -> list[dict[str, Any]]:
        return self._request("GET", "/scanpak/history") or []


# --------------------------------------------------------------------------- #
#  Offline queue
# --------------------------------------------------------------------------- #
class OfflineQueue:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init()

    def _init(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS pending_scans("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "parcel_number TEXT NOT NULL, "
                "created_at TEXT NOT NULL)"
            )

    def add(self, parcel_number: str) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO pending_scans(parcel_number, created_at) VALUES(?,?)",
                (parcel_number, datetime.now().isoformat()),
            )

    def contains(self, parcel_number: str) -> bool:
        with sqlite3.connect(self.db_path) as db:
            row = db.execute(
                "SELECT 1 FROM pending_scans WHERE parcel_number=? LIMIT 1",
                (parcel_number,),
            ).fetchone()
            return row is not None

    def count(self) -> int:
        with sqlite3.connect(self.db_path) as db:
            return int(db.execute("SELECT COUNT(*) FROM pending_scans").fetchone()[0])

    def sync(self, api: ApiClient) -> int:
        sent = 0
        with sqlite3.connect(self.db_path) as db:
            rows = db.execute(
                "SELECT id, parcel_number FROM pending_scans ORDER BY id"
            ).fetchall()
        for row_id, parcel_number in rows:
            api.add_scan(parcel_number)
            with sqlite3.connect(self.db_path) as db:
                db.execute("DELETE FROM pending_scans WHERE id=?", (row_id,))
            sent += 1
        return sent


# --------------------------------------------------------------------------- #
#  Application
# --------------------------------------------------------------------------- #
class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1280x820")
        self.minsize(1080, 720)
        self.configure(bg=BG)

        self.api = ApiClient()
        self.offline = OfflineQueue(DB_PATH)
        self.user_name = "operator"
        self.role = "viewer"
        self.access_level = 2

        self.q: queue.Queue[Callable[[], None]] = queue.Queue()
        self.nav_buttons: dict[str, tk.Label] = {}
        self.active_page = ""
        self.session_count = 0
        self.session_errors = 0
        self.local_log: deque[tuple[str, str, str, str]] = deque(maxlen=14)
        self.history_raw: list[dict[str, Any]] = []

        self._setup_style()
        self._load_session()

        if self.api.token:
            self.show_main()
        else:
            self.show_login()

        self.after(80, self._drain_queue)
        self.after(15000, self._auto_sync_tick)

    def _setup_style(self) -> None:
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TFrame", background=BG)
        style.configure("TEntry", fieldbackground=FIELD, borderwidth=0, relief="flat")
        style.configure(
            "Accent.TButton",
            font=("Segoe UI Semibold", 15, "bold"),
            padding=14,
            background=GREEN,
            foreground="white",
            borderwidth=0,
            focuscolor=GREEN,
        )
        style.map("Accent.TButton", background=[("active", CYAN)])
        style.configure(
            "Ghost.TButton",
            font=("Segoe UI", 12),
            padding=12,
            background=CARD_ALT,
            foreground=TEXT,
            borderwidth=0,
        )
        style.map("Ghost.TButton", background=[("active", BORDER)])
        style.configure(
            "Small.TButton",
            font=("Segoe UI", 11),
            padding=8,
            background=CARD_ALT,
            foreground=TEXT,
            borderwidth=0,
        )
        style.configure(
            "Treeview",
            background=CARD,
            fieldbackground=CARD,
            foreground=TEXT,
            rowheight=40,
            font=("Segoe UI", 11),
            borderwidth=0,
        )
        style.configure(
            "Treeview.Heading",
            background=SIDEBAR,
            foreground="white",
            font=("Segoe UI Semibold", 11, "bold"),
            padding=8,
            relief="flat",
        )
        style.map("Treeview", background=[("selected", "#CEF7EF")], foreground=[("selected", TEXT)])
        style.configure("Vertical.TScrollbar", background=CARD_ALT, troughcolor=CARD, borderwidth=0, arrowcolor=MUTED)

    def _drain_queue(self) -> None:
        while True:
            try:
                self.q.get_nowait()()
            except queue.Empty:
                break
        self.after(80, self._drain_queue)

    def bg_task(self, work: Callable[[], Any], done: Callable[[Any, Exception | None], None]) -> None:
        def run() -> None:
            try:
                result, error = work(), None
            except Exception as exc:
                result, error = None, exc
            self.q.put(lambda: done(result, error))
        threading.Thread(target=run, daemon=True).start()

    def clear(self) -> None:
        for widget in self.winfo_children():
            widget.destroy()

    def _load_session(self) -> None:
        if CONFIG_PATH.exists():
            try:
                data = json.loads(CONFIG_PATH.read_text("utf-8"))
                self.api.token = data.get("token", "")
                self.user_name = data.get("user_name", "operator")
                self.role = data.get("role", "viewer")
                self.access_level = int(data.get("access_level", 2))
            except Exception:
                pass

    def _save_session(self) -> None:
        CONFIG_PATH.write_text(
            json.dumps(
                {
                    "token": self.api.token,
                    "user_name": self.user_name,
                    "role": self.role,
                    "access_level": self.access_level,
                },
                ensure_ascii=False,
            ),
            "utf-8",
        )

    # ===================================================================== #
    #  LOGIN / REGISTER
    # ===================================================================== #
    def show_login(self) -> None:
        self.clear()
        outer = tk.Frame(self, bg=BG)
        outer.pack(expand=True, fill="both")
        center = tk.Frame(outer, bg=BG)
        center.place(relx=0.5, rely=0.5, anchor="center")

        tk.Label(center, text="📦", bg=BG, fg="white", font=("Segoe UI Emoji", 42)).pack()
        tk.Label(center, text="СканПак", bg=BG, fg="white", font=("Segoe UI Semibold", 36, "bold")).pack()
        tk.Label(center, text="Складський модуль сканування посилок", bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 14)).pack(pady=(4, 22))

        card = tk.Frame(center, bg=CARD)
        card.pack(ipadx=40, ipady=10)
        inner = tk.Frame(card, bg=CARD)
        inner.pack(padx=44, pady=36)

        tk.Label(inner, text="Вхід оператора", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 22, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 20))
        surname = self._form_entry(inner, 1, "Прізвище", False)
        password = self._form_entry(inner, 2, "Пароль", True)
        msg = tk.Label(inner, text="", bg=CARD, fg=RED, font=("Segoe UI", 12))
        msg.grid(row=3, column=0, columnspan=2, sticky="w", pady=(6, 8))

        def login() -> None:
            if not surname.get().strip() or not password.get().strip():
                msg.config(text="Введіть прізвище та пароль", fg=RED)
                return
            msg.config(text="Перевірка даних...", fg=MUTED)
            self.bg_task(lambda: self.api.login(surname.get().strip(), password.get().strip()), after_login)

        def after_login(result: Any, error: Exception | None) -> None:
            if error:
                msg.config(text=str(error), fg=RED)
                play_sound(False)
                return
            self.user_name = str(result.get("surname") or surname.get().strip())
            self.role = str(result.get("role") or "viewer")
            level = result.get("access_level")
            self.access_level = int(level) if level is not None else 1 if self.role == "admin" else 0 if self.role == "operator" else 2
            self._save_session()
            self.show_main()

        ttk.Button(inner, text="Увійти", style="Accent.TButton", command=login).grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 8))
        ttk.Button(inner, text="Реєстрація нового оператора", style="Ghost.TButton", command=self.show_register).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(2, 0))
        inner.columnconfigure(1, weight=1)
        surname.focus_set()
        surname.bind("<Return>", lambda _e: password.focus_set())
        password.bind("<Return>", lambda _e: login())

    def show_register(self) -> None:
        self.clear()
        outer = tk.Frame(self, bg=BG)
        outer.pack(expand=True, fill="both")
        card = tk.Frame(outer, bg=CARD)
        card.place(relx=0.5, rely=0.5, anchor="center")
        inner = tk.Frame(card, bg=CARD)
        inner.pack(padx=46, pady=40)

        tk.Label(inner, text="Заявка на реєстрацію", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 22, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 16))
        surname = self._form_entry(inner, 1, "Прізвище", False)
        p1 = self._form_entry(inner, 2, "Пароль", True)
        p2 = self._form_entry(inner, 3, "Повтор пароля", True)
        msg = tk.Label(inner, text="", bg=CARD, fg=RED, font=("Segoe UI", 12))
        msg.grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 8))

        def register() -> None:
            if not surname.get().strip() or not p1.get() or not p2.get():
                msg.config(text="Заповніть усі поля", fg=RED)
                return
            if len(p1.get()) < 6:
                msg.config(text="Пароль має містити щонайменше 6 символів", fg=RED)
                return
            if p1.get() != p2.get():
                msg.config(text="Паролі не співпадають", fg=RED)
                return
            msg.config(text="Відправлення...", fg=MUTED)
            self.bg_task(
                lambda: self.api.register(surname.get().strip(), p1.get().strip()),
                lambda _r, err: msg.config(
                    text=str(err) if err else "Заявку відправлено. Дочекайтесь підтвердження адміністратора.",
                    fg=RED if err else GREEN,
                ),
            )

        ttk.Button(inner, text="Відправити заявку", style="Accent.TButton", command=register).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(8, 8))
        ttk.Button(inner, text="Назад до входу", style="Ghost.TButton", command=self.show_login).grid(row=6, column=0, columnspan=2, sticky="ew")
        inner.columnconfigure(1, weight=1)

    def _form_entry(self, parent: tk.Widget, row: int, label: str, secret: bool) -> tk.Entry:
        tk.Label(parent, text=label, bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 12, "bold")).grid(row=row, column=0, columnspan=2, sticky="w", pady=(12, 2))
        wrapper = tk.Frame(parent, bg=FIELD, highlightthickness=1, highlightbackground=BORDER, highlightcolor=GREEN)
        wrapper.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(34, 4))
        parent.grid_rowconfigure(row, minsize=70)
        entry = tk.Entry(wrapper, font=("Segoe UI", 16), bd=0, relief="flat", bg=FIELD, fg=TEXT, insertbackground=TEXT, show="●" if secret else "", width=26)
        entry.pack(fill="x", padx=12, ipady=9)
        return entry

    # ===================================================================== #
    #  MAIN LAYOUT
    # ===================================================================== #
    def show_main(self) -> None:
        self.clear()
        self.session_count = 0
        self.session_errors = 0
        self.local_log.clear()

        root = tk.Frame(self, bg=BG)
        root.pack(fill="both", expand=True)
        sidebar = tk.Frame(root, bg=SIDEBAR, width=260)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        brand = tk.Frame(sidebar, bg=SIDEBAR)
        brand.pack(fill="x", pady=(26, 18), padx=22)
        tk.Label(brand, text="📦  СканПак", bg=SIDEBAR, fg="white", font=("Segoe UI Semibold", 20, "bold")).pack(anchor="w")
        tk.Label(brand, text="Сканування посилок", bg=SIDEBAR, fg=MUTED_LIGHT, font=("Segoe UI", 11)).pack(anchor="w", pady=(2, 0))

        user_card = tk.Frame(sidebar, bg=SIDEBAR_HOVER)
        user_card.pack(fill="x", padx=14, pady=(0, 18))
        tk.Label(user_card, text=self.user_name[:1].upper() or "?", bg=GREEN, fg="white", width=3, font=("Segoe UI Semibold", 16, "bold")).pack(side="left", padx=(10, 10), pady=10, ipady=4)
        ucol = tk.Frame(user_card, bg=SIDEBAR_HOVER)
        ucol.pack(side="left", fill="both", expand=True, pady=10)
        tk.Label(ucol, text=self.user_name, bg=SIDEBAR_HOVER, fg="white", font=("Segoe UI Semibold", 13, "bold"), anchor="w").pack(anchor="w", fill="x")
        role_text = {"admin": "Адміністратор", "operator": "Оператор"}.get(self.role, "Перегляд")
        tk.Label(ucol, text=role_text, bg=SIDEBAR_HOVER, fg=MUTED_LIGHT, font=("Segoe UI", 11), anchor="w").pack(anchor="w", fill="x")

        for key, label, command in [
            ("scan", "🔍  Сканування", self.scan_page),
            ("history", "🗂  Історія", self.history_page),
            ("stats", "📊  Статистика", self.stats_page),
        ]:
            self._make_nav(sidebar, key, label, command)

        bottom = tk.Frame(sidebar, bg=SIDEBAR)
        bottom.pack(side="bottom", fill="x", pady=18, padx=14)
        self.queue_label = tk.Label(bottom, text="", bg=SIDEBAR, fg=AMBER, font=("Segoe UI Semibold", 11, "bold"), anchor="w")
        self.queue_label.pack(fill="x", pady=(0, 10))
        logout_btn = tk.Label(bottom, text="⏻  Вийти", bg=SIDEBAR_HOVER, fg="white", font=("Segoe UI Semibold", 12, "bold"), padx=14, pady=12, anchor="w", cursor="hand2")
        logout_btn.pack(fill="x")
        logout_btn.bind("<Button-1>", lambda _e: self.logout())
        logout_btn.bind("<Enter>", lambda _e: logout_btn.config(bg=RED))
        logout_btn.bind("<Leave>", lambda _e: logout_btn.config(bg=SIDEBAR_HOVER))

        self.content = tk.Frame(root, bg=BG)
        self.content.pack(side="left", fill="both", expand=True)
        self.body = tk.Frame(self.content, bg=BG)
        self.body.pack(fill="both", expand=True, padx=28, pady=24)

        self.scan_page()
        self._update_queue_label()
        self.try_sync()
        self.refresh_history_silent()

    def _make_nav(self, parent: tk.Widget, key: str, label: str, command: Callable[[], None]) -> None:
        lbl = tk.Label(parent, text=label, bg=SIDEBAR, fg=TEXT_LIGHT, font=("Segoe UI", 14), anchor="w", padx=22, pady=15, cursor="hand2")
        lbl.pack(fill="x", padx=8, pady=2)
        self.nav_buttons[key] = lbl

        def on_click(_event=None) -> None:
            self._set_active_nav(key)
            command()

        lbl.bind("<Button-1>", on_click)
        lbl.bind("<Enter>", lambda _e: lbl.config(bg=SIDEBAR_HOVER) if self.active_page != key else None)
        lbl.bind("<Leave>", lambda _e: lbl.config(bg=SIDEBAR) if self.active_page != key else None)

    def _set_active_nav(self, key: str) -> None:
        self.active_page = key
        for nav_key, label in self.nav_buttons.items():
            if nav_key == key:
                label.config(bg=SIDEBAR_ACTIVE, fg="white", font=("Segoe UI Semibold", 14, "bold"))
            else:
                label.config(bg=SIDEBAR, fg=TEXT_LIGHT, font=("Segoe UI", 14))

    def body_clear(self) -> None:
        for widget in self.body.winfo_children():
            widget.destroy()

    def _page_header(self, title: str, subtitle: str = "") -> None:
        header = tk.Frame(self.body, bg=BG)
        header.pack(fill="x", pady=(0, 18))
        tk.Label(header, text=title, bg=BG, fg="white", font=("Segoe UI Semibold", 24, "bold")).pack(anchor="w")
        if subtitle:
            tk.Label(header, text=subtitle, bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 12)).pack(anchor="w", pady=(2, 0))

    def _update_queue_label(self) -> None:
        count = self.offline.count()
        if count:
            self.queue_label.config(text=f"📦 Офлайн-черга: {count}", fg=AMBER)
        else:
            self.queue_label.config(text="✅ Дані синхронізовано", fg=GREEN)

    # ===================================================================== #
    #  SCAN PAGE
    # ===================================================================== #
    def scan_page(self) -> None:
        self._set_active_nav("scan")
        self.body_clear()
        self._page_header("СканПак", "Скануйте номер посилки. Після Enter запис одразу відправляється на сервер.")

        wrap = tk.Frame(self.body, bg=BG)
        wrap.pack(fill="both", expand=True)
        wrap.columnconfigure(0, weight=3)
        wrap.columnconfigure(1, weight=2)
        wrap.rowconfigure(0, weight=1)

        card = tk.Frame(wrap, bg=CARD)
        card.grid(row=0, column=0, sticky="nsew", padx=(0, 16))
        inner = tk.Frame(card, bg=CARD)
        inner.pack(fill="both", expand=True, padx=34, pady=30)

        counters = tk.Frame(inner, bg=CARD)
        counters.pack(fill="x", pady=(0, 14))
        self.lbl_cnt_ok = self._counter_chip(counters, "Успішно за сесію", "0", GREEN)
        self.lbl_cnt_err = self._counter_chip(counters, "Помилки / дублі", "0", RED)

        tk.Label(inner, text="Номер посилки", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 17, "bold")).pack(anchor="w", pady=(12, 6))
        field_wrap = tk.Frame(inner, bg=FIELD, highlightthickness=2, highlightbackground=GREEN, highlightcolor=GREEN)
        field_wrap.pack(fill="x")
        parcel_entry = tk.Entry(field_wrap, font=("Segoe UI", 34), bd=0, relief="flat", bg=FIELD, fg=TEXT, insertbackground=GREEN, justify="center")
        parcel_entry.pack(fill="x", padx=14, ipady=16)

        ttk.Button(inner, text="Зберегти скан", style="Accent.TButton", command=lambda: on_scan()).pack(fill="x", pady=(22, 0))
        self.status = tk.Label(inner, text="Готово — відскануйте номер посилки", bg=GREEN, fg="white", font=("Segoe UI Semibold", 24, "bold"), pady=26)
        self.status.pack(fill="x", pady=(22, 0))

        log_card = tk.Frame(wrap, bg=CARD)
        log_card.grid(row=0, column=1, sticky="nsew")
        log_inner = tk.Frame(log_card, bg=CARD)
        log_inner.pack(fill="both", expand=True, padx=20, pady=20)
        tk.Label(log_inner, text="Останні сканування", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w", pady=(0, 10))
        self.log_frame = tk.Frame(log_inner, bg=CARD)
        self.log_frame.pack(fill="both", expand=True)
        self._render_local_log()

        def set_status(text: str, bg: str) -> None:
            self.status.config(text=text, bg=bg)

        def on_scan(_event=None) -> None:
            number = only_digits(parcel_entry.get())
            parcel_entry.delete(0, "end")
            if number:
                parcel_entry.insert(0, number)
            if not number:
                set_status("Порожній номер — спробуйте ще раз", RED_BG)
                play_sound(False)
                parcel_entry.focus_set()
                return
            if self._is_duplicate_number(number):
                parcel_entry.delete(0, "end")
                set_status("⚠ Дублікат — цей номер вже був у списку", AMBER_BG)
                play_sound(False)
                self._add_local_log("dup", number, "Дублікат — не збережено")
                self.session_errors += 1
                self._update_counters()
                parcel_entry.focus_set()
                return

            parcel_entry.delete(0, "end")
            parcel_entry.focus_set()
            set_status("⏳ Надсилання на сервер...", AMBER_BG)

            def done(result: Any, error: Exception | None) -> None:
                if error:
                    self.offline.add(number)
                    self._update_queue_label()
                    play_sound(False)
                    set_status(f"📦 Збережено офлайн — черга: {self.offline.count()}", AMBER_BG)
                    self._add_local_log("offline", number, "Немає зв'язку")
                    self.session_errors += 1
                else:
                    play_sound(True)
                    scanned_at = (result or {}).get("scanned_at") or datetime.now().isoformat()
                    set_status(f"✅ Успішно додано о {fmt_time(scanned_at)}", GREEN_BG)
                    self._add_local_log("ok", number, f"Додано о {fmt_time(scanned_at)}")
                    self.session_count += 1
                    self.history_raw.insert(0, {
                        "username": (result or {}).get("username") or self.user_name,
                        "parcel_number": number,
                        "scanned_at": scanned_at,
                    })
                self._update_counters()

            self.bg_task(lambda: self.api.add_scan(number), done)

        parcel_entry.bind("<Return>", on_scan)
        parcel_entry.focus_set()

    def _is_duplicate_number(self, number: str) -> bool:
        if self.offline.contains(number):
            return True
        if any(str(row.get("parcel_number") or "").strip() == number for row in self.history_raw):
            return True
        if any(pair.strip() == number for _kind, _ts, pair, _note in self.local_log):
            return True
        return False

    def _counter_chip(self, parent: tk.Widget, label: str, value: str, color: str) -> tk.Label:
        chip = tk.Frame(parent, bg=CARD_ALT)
        chip.pack(side="left", expand=True, fill="x", padx=4)
        val = tk.Label(chip, text=value, bg=CARD_ALT, fg=color, font=("Segoe UI Semibold", 24, "bold"))
        val.pack(pady=(10, 0))
        tk.Label(chip, text=label, bg=CARD_ALT, fg=MUTED, font=("Segoe UI", 11)).pack(pady=(0, 10))
        return val

    def _update_counters(self) -> None:
        try:
            self.lbl_cnt_ok.config(text=str(self.session_count))
            self.lbl_cnt_err.config(text=str(self.session_errors))
        except Exception:
            pass

    def _add_local_log(self, kind: str, number: str, note: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.local_log.appendleft((kind, ts, number, note))
        self._render_local_log()

    def _render_local_log(self) -> None:
        for widget in self.log_frame.winfo_children():
            widget.destroy()
        if not self.local_log:
            tk.Label(self.log_frame, text="Поки що немає сканувань", bg=CARD, fg=MUTED, font=("Segoe UI", 12)).pack(anchor="w", pady=8)
            return
        colors = {"ok": GREEN, "dup": AMBER, "offline": AMBER, "err": RED}
        icons = {"ok": "✅", "dup": "⚠", "offline": "📦", "err": "✖"}
        for kind, ts, number, note in self.local_log:
            row = tk.Frame(self.log_frame, bg=CARD_ALT)
            row.pack(fill="x", pady=3)
            tk.Label(row, text=icons.get(kind, "•"), bg=CARD_ALT, fg=colors.get(kind, MUTED), font=("Segoe UI Emoji", 14)).pack(side="left", padx=(10, 8), pady=8)
            col = tk.Frame(row, bg=CARD_ALT)
            col.pack(side="left", fill="x", expand=True, pady=6)
            tk.Label(col, text=number, bg=CARD_ALT, fg=TEXT, font=("Segoe UI Semibold", 14, "bold"), anchor="w").pack(anchor="w", fill="x")
            tk.Label(col, text=f"{ts} • {note}", bg=CARD_ALT, fg=MUTED, font=("Segoe UI", 10), anchor="w").pack(anchor="w", fill="x")

    # ===================================================================== #
    #  HISTORY
    # ===================================================================== #
    def _make_table(self, parent: tk.Widget, columns: list[str], headings: list[str], widths: list[int]) -> ttk.Treeview:
        container = tk.Frame(parent, bg=CARD)
        container.pack(fill="both", expand=True)
        tree = ttk.Treeview(container, columns=columns, show="headings")
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        for column, heading, width in zip(columns, headings, widths):
            tree.heading(column, text=heading)
            tree.column(column, width=width, anchor="w")
        tree.tag_configure("odd", background=CARD)
        tree.tag_configure("even", background=CARD_ALT)
        scrollbar.pack(side="right", fill="y")
        tree.pack(side="left", fill="both", expand=True)
        return tree

    def refresh_history_silent(self) -> None:
        if not self.api.token:
            return

        def loaded(data: Any, error: Exception | None) -> None:
            if not error:
                self.history_raw = data or []

        self.bg_task(self.api.get_history, loaded)

    def history_page(self) -> None:
        self._set_active_nav("history")
        self.body_clear()
        self._page_header("Історія СканПак", "Дані завантажуються з /scanpak/history.")

        bar = tk.Frame(self.body, bg=BG)
        bar.pack(fill="x", pady=(0, 12))
        search_var = tk.StringVar()
        search = tk.Entry(bar, textvariable=search_var, font=("Segoe UI", 13), bd=0, relief="flat", bg=CARD, fg=TEXT, insertbackground=GREEN, width=34)
        search.pack(side="left", ipady=8, padx=(0, 8))
        tk.Label(bar, text="🔎 пошук номера посилки / користувача", bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 11)).pack(side="left")
        ttk.Button(bar, text="↻ Оновити", style="Small.TButton", command=self.history_page).pack(side="right")

        tree = self._make_table(
            self.body,
            ["dt", "user", "number"],
            ["Дата і час", "Користувач", "Номер посилки"],
            [210, 240, 360],
        )

        def apply_filter(*_args: Any) -> None:
            self._fill_scanpak_table(tree, self.history_raw, search_var.get())

        search_var.trace_add("write", apply_filter)

        def loaded(data: Any, error: Exception | None) -> None:
            if error:
                messagebox.showerror(APP_NAME, str(error))
                return
            self.history_raw = data or []
            self._fill_scanpak_table(tree, self.history_raw, "")

        self.bg_task(self.api.get_history, loaded)

    def _fill_scanpak_table(self, tree: ttk.Treeview, data: list[dict[str, Any]], query: str) -> None:
        for item in tree.get_children():
            tree.delete(item)
        q = (query or "").strip().lower()
        rows = sorted(data or [], key=lambda row: parse_dt(row.get("scanned_at")), reverse=True)
        index = 0
        for row in rows:
            user = str(row.get("username") or "")
            number = str(row.get("parcel_number") or "")
            dt_value = fmt_dt(row.get("scanned_at"))
            if q and q not in f"{user} {number} {dt_value}".lower():
                continue
            tag = "even" if index % 2 else "odd"
            tree.insert("", "end", values=(dt_value, user, number), tags=(tag,))
            index += 1
        if index == 0:
            tree.insert("", "end", values=("Немає записів", "—", "—"), tags=("odd",))

    # ===================================================================== #
    #  STATS
    # ===================================================================== #
    def stats_page(self) -> None:
        self._set_active_nav("stats")
        self.body_clear()
        self._page_header("Статистика", "Підсумки по завантаженій історії СканПак.")
        loading = tk.Frame(self.body, bg=CARD)
        loading.pack(fill="both", expand=True)
        tk.Label(loading, text="Завантаження статистики...", bg=CARD, fg=MUTED, font=("Segoe UI", 16)).pack(expand=True)

        def loaded(data: Any, error: Exception | None) -> None:
            if error:
                messagebox.showerror(APP_NAME, str(error))
                return
            self.history_raw = data or []
            self._render_stats()

        self.bg_task(self.api.get_history, loaded)

    def _render_stats(self) -> None:
        self.body_clear()
        self._page_header("Статистика", "Підсумки по завантаженій історії СканПак.")
        rows = self.history_raw or []
        user_counter = Counter(str(row.get("username") or "—") for row in rows)
        daily_counter: Counter[str] = Counter()
        latest_dt = datetime.min
        latest_number = "—"
        for row in rows:
            dt = parse_dt(row.get("scanned_at"))
            if dt != datetime.min:
                daily_counter[dt.strftime("%d.%m.%Y")] += 1
                if dt > latest_dt:
                    latest_dt = dt
                    latest_number = str(row.get("parcel_number") or "—")

        top_user, top_count = ("—", 0)
        if user_counter:
            top_user, top_count = user_counter.most_common(1)[0]
        latest_text = "—" if latest_dt == datetime.min else f"{latest_number} • {latest_dt.strftime('%d.%m.%Y %H:%M')}"

        cards = tk.Frame(self.body, bg=BG)
        cards.pack(fill="x", pady=(0, 16))
        self._stat_card(cards, "Всього сканів", str(len(rows)), "📦", GREEN).pack(side="left", fill="x", expand=True, padx=(0, 10))
        self._stat_card(cards, "Користувачів", str(len(user_counter)), "👥", CYAN).pack(side="left", fill="x", expand=True, padx=10)
        self._stat_card(cards, "Лідер", f"{top_user} ({top_count})" if top_count else "—", "🏆", AMBER).pack(side="left", fill="x", expand=True, padx=10)
        self._stat_card(cards, "Останній скан", latest_text, "⏱", BLUE).pack(side="left", fill="x", expand=True, padx=(10, 0))

        lower = tk.Frame(self.body, bg=BG)
        lower.pack(fill="both", expand=True)
        lower.columnconfigure(0, weight=1)
        lower.columnconfigure(1, weight=1)
        left = self._list_panel(lower, "ТОП користувачів")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        right = self._list_panel(lower, "Активність по днях")
        right.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        self._fill_rank_list(left, [(u, f"{c} скан.") for u, c in user_counter.most_common(10)])
        self._fill_rank_list(right, [(d, f"{c} скан.") for d, c in sorted(daily_counter.items(), reverse=True)[:10]])

    def _stat_card(self, parent: tk.Widget, title: str, value: str, icon: str, color: str) -> tk.Frame:
        card = tk.Frame(parent, bg=CARD)
        inner = tk.Frame(card, bg=CARD)
        inner.pack(fill="both", expand=True, padx=16, pady=14)
        tk.Label(inner, text=icon, bg=CARD, fg=color, font=("Segoe UI Emoji", 24)).pack(anchor="w")
        tk.Label(inner, text=title, bg=CARD, fg=MUTED, font=("Segoe UI", 11)).pack(anchor="w", pady=(6, 0))
        tk.Label(inner, text=value, bg=CARD, fg=color, font=("Segoe UI Semibold", 18, "bold"), wraplength=230, justify="left").pack(anchor="w", pady=(5, 0))
        return card

    def _list_panel(self, parent: tk.Widget, title: str) -> tk.Frame:
        panel = tk.Frame(parent, bg=CARD)
        tk.Label(panel, text=title, bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w", padx=16, pady=(16, 8))
        body = tk.Frame(panel, bg=CARD)
        body.pack(fill="both", expand=True, padx=16, pady=(0, 16))
        return body

    def _fill_rank_list(self, parent: tk.Widget, rows: list[tuple[str, str]]) -> None:
        for widget in parent.winfo_children():
            widget.destroy()
        if not rows:
            tk.Label(parent, text="Немає даних", bg=CARD, fg=MUTED, font=("Segoe UI", 12)).pack(anchor="w", pady=8)
            return
        for index, (title, trailing) in enumerate(rows, start=1):
            row = tk.Frame(parent, bg=CARD_ALT)
            row.pack(fill="x", pady=3)
            tk.Label(row, text=str(index), bg=GREEN, fg="white", width=3, font=("Segoe UI Semibold", 11, "bold")).pack(side="left", padx=(8, 10), pady=8, ipady=3)
            tk.Label(row, text=title, bg=CARD_ALT, fg=TEXT, font=("Segoe UI Semibold", 12, "bold"), anchor="w").pack(side="left", fill="x", expand=True, pady=8)
            tk.Label(row, text=trailing, bg=CARD_ALT, fg=MUTED, font=("Segoe UI", 11), anchor="e").pack(side="right", padx=10, pady=8)

    # ===================================================================== #
    #  SYNC / LOGOUT
    # ===================================================================== #
    def try_sync(self) -> None:
        if self.offline.count():
            def done(sent: Any, error: Exception | None) -> None:
                self._update_queue_label()
                if not error and sent:
                    if self.active_page == "scan":
                        try:
                            self.status.config(text=f"☁ Синхронізовано {sent} запис(ів)", bg=GREEN_BG)
                        except Exception:
                            pass
                    self.refresh_history_silent()
            self.bg_task(lambda: self.offline.sync(self.api), done)

    def _auto_sync_tick(self) -> None:
        if getattr(self, "queue_label", None) is not None:
            try:
                self._update_queue_label()
            except Exception:
                pass
            if self.api.token and self.offline.count():
                self.bg_task(lambda: self.offline.sync(self.api), lambda _s, _e: self._safe_after_sync())
        self.after(15000, self._auto_sync_tick)

    def _safe_after_sync(self) -> None:
        try:
            self._update_queue_label()
            self.refresh_history_silent()
        except Exception:
            pass

    def logout(self) -> None:
        if messagebox.askyesno(APP_NAME, "Вийти з акаунту?"):
            self.api.token = ""
            CONFIG_PATH.unlink(missing_ok=True)
            self.nav_buttons.clear()
            self.active_page = ""
            self.show_login()


if __name__ == "__main__":
    app = App()
    app.mainloop()
