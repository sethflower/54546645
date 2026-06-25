#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Windows desktop client for the BoxID-ТТН module.

Складський клієнт для сканування пар BoxID + ТТН.

Install: pip install requests
Run:     python boxid_ttn_windows.py
"""
from __future__ import annotations

import json
import os
import queue
import re
import sqlite3
import threading
import tkinter as tk
from collections import deque
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable

import requests

# --------------------------------------------------------------------------- #
#  Constants & theme
# --------------------------------------------------------------------------- #
APP_NAME = "BoxID-ТТН"
API_BASE_URL = "https://tracking-app.dclink.ua"
TIMEOUT = 12

APP_DIR = Path(os.getenv("APPDATA") or Path.home()) / "BoxID_TTN_Windows"
APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "boxid_ttn.sqlite3"
CONFIG_PATH = APP_DIR / "session.json"

# Palette ------------------------------------------------------------------- #
BG = "#0A1330"          # main background (dark navy)
SIDEBAR = "#0E1A45"     # navigation panel
SIDEBAR_HOVER = "#16266B"
SIDEBAR_ACTIVE = "#1E5BFF"
CARD = "#FFFFFF"        # white card
CARD_ALT = "#F4F7FE"    # zebra / inner card
TEXT = "#0B1530"        # dark text on white
TEXT_LIGHT = "#E8EEFF"  # light text on dark
MUTED = "#7B89A8"       # secondary text
MUTED_LIGHT = "#9FB0D9"
BLUE = "#1E5BFF"
SOFT = "#3F8CFF"
GREEN = "#0FB981"
GREEN_BG = "#0FB981"
RED = "#EF4444"
RED_BG = "#EF4444"
AMBER = "#F59E0B"
AMBER_BG = "#F59E0B"
FIELD = "#EEF3FC"
BORDER = "#D8E0F0"


# --------------------------------------------------------------------------- #
#  Helpers
# --------------------------------------------------------------------------- #
def only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def parse_dt(value: Any) -> datetime:
    try:
        return (
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            .astimezone()
            .replace(tzinfo=None)
        )
    except Exception:
        return datetime.min


def fmt_dt(value: Any) -> str:
    dt = parse_dt(value)
    return str(value) if dt == datetime.min else dt.strftime("%d.%m.%Y %H:%M:%S")


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
            r = self.session.request(
                method,
                self._url(path),
                headers=self._headers(auth),
                timeout=TIMEOUT,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise ApiError("Немає зв'язку з сервером") from exc

        if r.status_code < 200 or r.status_code >= 300:
            try:
                body = r.json()
                msg = body.get("detail") or body.get("message") or r.text
            except Exception:
                msg = r.text
            raise ApiError(msg or f"Помилка сервера ({r.status_code})")

        if not r.text:
            return None
        try:
            return r.json()
        except ValueError as exc:
            raise ApiError("Некоректна відповідь сервера") from exc

    # -- endpoints --------------------------------------------------------- #
    def login(self, surname: str, password: str) -> dict[str, Any]:
        data = self._request(
            "POST", "/login", auth=False,
            data=json.dumps({"surname": surname, "password": password}),
        )
        token = str(data.get("token") or "")
        if not token:
            raise ApiError("Сервер не повернув токен")
        self.token = token
        return data

    def register(self, surname: str, password: str) -> None:
        self._request(
            "POST", "/register", auth=False,
            data=json.dumps({"surname": surname, "password": password}),
        )

    def add_record(self, user_name: str, boxid: str, ttn: str) -> dict[str, Any]:
        return self._request(
            "POST", "/add_record",
            data=json.dumps({"user_name": user_name, "boxid": boxid, "ttn": ttn}),
        ) or {}

    def get_history(self) -> list[dict[str, Any]]:
        return self._request("GET", "/get_history") or []


    def get_errors(self) -> list[dict[str, Any]]:
        return self._request("GET", "/get_errors") or []

    def clear_history(self) -> None:
        self._request("DELETE", "/clear_tracking")


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
                "CREATE TABLE IF NOT EXISTS pending_records("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "user_name TEXT, boxid TEXT, ttn TEXT, created_at TEXT)"
            )

    def add(self, user_name: str, boxid: str, ttn: str) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "INSERT INTO pending_records(user_name, boxid, ttn, created_at) "
                "VALUES(?,?,?,?)",
                (user_name, boxid, ttn, datetime.now().isoformat()),
            )

    def count(self) -> int:
        with sqlite3.connect(self.db_path) as db:
            return int(db.execute("SELECT COUNT(*) FROM pending_records").fetchone()[0])

    def sync(self, api: ApiClient) -> int:
        sent = 0
        with sqlite3.connect(self.db_path) as db:
            rows = db.execute(
                "SELECT id,user_name,boxid,ttn FROM pending_records ORDER BY id"
            ).fetchall()
        for row_id, user_name, boxid, ttn in rows:
            api.add_record(user_name, boxid, ttn)
            with sqlite3.connect(self.db_path) as db:
                db.execute("DELETE FROM pending_records WHERE id=?", (row_id,))
            sent += 1
        return sent


# --------------------------------------------------------------------------- #
#  Sound helper
# --------------------------------------------------------------------------- #
def play_sound(ok: bool) -> None:
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_OK if ok else winsound.MB_ICONHAND)
    except Exception:
        pass


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
        self.local_log: deque[tuple[str, str, str, str]] = deque(maxlen=12)

        self._setup_style()
        self._load_session()

        if self.api.token:
            self.show_main()
        else:
            self.show_login()

        self.after(80, self._drain_queue)
        self.after(15000, self._auto_sync_tick)

    # -- ttk styles -------------------------------------------------------- #
    def _setup_style(self) -> None:
        s = ttk.Style(self)
        s.theme_use("clam")

        s.configure("TFrame", background=BG)
        s.configure("Card.TFrame", background=CARD)
        s.configure("CardAlt.TFrame", background=CARD_ALT)

        s.configure("TLabel", background=BG, foreground=TEXT_LIGHT, font=("Segoe UI", 12))
        s.configure("Card.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 12))
        s.configure("Muted.TLabel", background=CARD, foreground=MUTED, font=("Segoe UI", 11))
        s.configure("Title.TLabel", background=BG, foreground="white",
                    font=("Segoe UI Semibold", 30, "bold"))

        # Entries
        s.configure("TEntry", fieldbackground=FIELD, borderwidth=0, relief="flat")
        s.configure("Big.TEntry", fieldbackground=FIELD, borderwidth=0,
                    relief="flat", padding=10)

        # Buttons
        s.configure("Accent.TButton", font=("Segoe UI Semibold", 15, "bold"),
                    padding=14, background=BLUE, foreground="white",
                    borderwidth=0, focuscolor=BLUE)
        s.map("Accent.TButton", background=[("active", SOFT)])

        s.configure("Ghost.TButton", font=("Segoe UI", 12), padding=12,
                    background=CARD_ALT, foreground=TEXT, borderwidth=0)
        s.map("Ghost.TButton", background=[("active", BORDER)])

        s.configure("Small.TButton", font=("Segoe UI", 11), padding=8,
                    background=CARD_ALT, foreground=TEXT, borderwidth=0)
        s.map("Small.TButton", background=[("active", BORDER)])

        # Treeview
        s.configure("Treeview",
                    background=CARD, fieldbackground=CARD, foreground=TEXT,
                    rowheight=40, font=("Segoe UI", 11), borderwidth=0)
        s.configure("Treeview.Heading",
                    background=SIDEBAR, foreground="white",
                    font=("Segoe UI Semibold", 11, "bold"), padding=8, relief="flat")
        s.map("Treeview.Heading", background=[("active", SIDEBAR_HOVER)])
        s.map("Treeview", background=[("selected", "#CFE0FF")],
              foreground=[("selected", TEXT)])

        s.configure("Vertical.TScrollbar", background=CARD_ALT,
                    troughcolor=CARD, borderwidth=0, arrowcolor=MUTED)

    # -- main loop helpers ------------------------------------------------- #
    def _drain_queue(self) -> None:
        while True:
            try:
                self.q.get_nowait()()
            except queue.Empty:
                break
        self.after(80, self._drain_queue)

    def bg_task(self, work: Callable[[], Any],
                done: Callable[[Any, Exception | None], None]) -> None:
        def run() -> None:
            try:
                res, err = work(), None
            except Exception as exc:
                res, err = None, exc
            self.q.put(lambda: done(res, err))
        threading.Thread(target=run, daemon=True).start()

    def clear(self) -> None:
        for w in self.winfo_children():
            w.destroy()

    # -- session persistence ---------------------------------------------- #
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
    #  LOGIN SCREEN
    # ===================================================================== #
    def show_login(self) -> None:
        self.clear()
        outer = tk.Frame(self, bg=BG)
        outer.pack(expand=True, fill="both")

        center = tk.Frame(outer, bg=BG)
        center.place(relx=0.5, rely=0.5, anchor="center")

        # Brand
        brand = tk.Frame(center, bg=BG)
        brand.pack(pady=(0, 22))
        tk.Label(brand, text="📦", bg=BG, fg="white",
                 font=("Segoe UI Emoji", 40)).pack()
        tk.Label(brand, text="BoxID-ТТН", bg=BG, fg="white",
                 font=("Segoe UI Semibold", 34, "bold")).pack()
        tk.Label(brand, text="Складський модуль сканування",
                 bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 14)).pack(pady=(4, 0))

        # Card
        card = tk.Frame(center, bg=CARD)
        card.pack(ipadx=40, ipady=10)
        inner = tk.Frame(card, bg=CARD)
        inner.pack(padx=44, pady=36)

        tk.Label(inner, text="Вхід оператора", bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 22, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 20))

        surname = self._login_entry(inner, 1, "Прізвище", secret=False)
        password = self._login_entry(inner, 2, "Пароль", secret=True)

        msg = tk.Label(inner, text="", bg=CARD, fg=RED, font=("Segoe UI", 12))
        msg.grid(row=3, column=0, columnspan=2, sticky="w", pady=(6, 8))

        def login() -> None:
            if not surname.get().strip() or not password.get().strip():
                msg.config(text="Введіть прізвище та пароль", fg=RED)
                return
            msg.config(text="Перевірка даних...", fg=MUTED)
            self.bg_task(
                lambda: self.api.login(surname.get().strip(), password.get().strip()),
                after_login,
            )

        def after_login(res: Any, err: Exception | None) -> None:
            if err:
                msg.config(text=str(err), fg=RED)
                play_sound(False)
                return
            self.user_name = str(res.get("surname") or surname.get().strip())
            self.role = str(res.get("role") or "viewer")
            lvl = res.get("access_level")
            if lvl is not None:
                self.access_level = int(lvl)
            else:
                self.access_level = 1 if self.role == "admin" else 0 if self.role == "operator" else 2
            self._save_session()
            self.show_main()

        btn = ttk.Button(inner, text="Увійти", style="Accent.TButton", command=login)
        btn.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 8))

        reg = ttk.Button(inner, text="Реєстрація нового оператора",
                         style="Ghost.TButton", command=self.show_register)
        reg.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(2, 0))

        inner.columnconfigure(1, weight=1)
        surname.focus_set()
        surname.bind("<Return>", lambda e: password.focus_set())
        password.bind("<Return>", lambda e: login())

    def _login_entry(self, parent: tk.Widget, row: int, label: str,
                     secret: bool = False) -> tk.Entry:
        tk.Label(parent, text=label, bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 12, "bold")).grid(
            row=row, column=0, sticky="w", pady=(10, 2), columnspan=2)
        wrapper = tk.Frame(parent, bg=FIELD, highlightthickness=1,
                           highlightbackground=BORDER, highlightcolor=BLUE)
        wrapper.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(20, 6))
        e = tk.Entry(wrapper, font=("Segoe UI", 17), bd=0, relief="flat",
                     bg=FIELD, fg=TEXT, insertbackground=TEXT,
                     show="●" if secret else "", width=26)
        e.pack(fill="x", padx=12, ipady=10)
        # fix row offset (label above the field)
        wrapper.grid_configure(row=row, pady=(34, 6))
        parent.grid_rowconfigure(row, minsize=70)
        return e

    def show_register(self) -> None:
        self.clear()
        outer = tk.Frame(self, bg=BG)
        outer.pack(expand=True, fill="both")
        card = tk.Frame(outer, bg=CARD)
        card.place(relx=0.5, rely=0.5, anchor="center")
        inner = tk.Frame(card, bg=CARD)
        inner.pack(padx=46, pady=40)

        tk.Label(inner, text="Заявка на реєстрацію", bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 22, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(0, 16))

        surname = self._register_entry(inner, 1, "Прізвище", False)
        p1 = self._register_entry(inner, 2, "Пароль", True)
        p2 = self._register_entry(inner, 3, "Повтор пароля", True)

        msg = tk.Label(inner, text="", bg=CARD, fg=RED, font=("Segoe UI", 12))
        msg.grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 8))

        def reg() -> None:
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
                    text=str(err) if err
                    else "Заявку відправлено. Дочекайтесь підтвердження адміністратора.",
                    fg=RED if err else GREEN,
                ),
            )

        ttk.Button(inner, text="Відправити заявку", style="Accent.TButton",
                   command=reg).grid(row=5, column=0, columnspan=2,
                                     sticky="ew", pady=(8, 8))
        ttk.Button(inner, text="Назад до входу", style="Ghost.TButton",
                   command=self.show_login).grid(row=6, column=0, columnspan=2,
                                                 sticky="ew")
        inner.columnconfigure(1, weight=1)

    def _register_entry(self, parent: tk.Widget, row: int, label: str,
                        secret: bool) -> tk.Entry:
        tk.Label(parent, text=label, bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 12, "bold")).grid(
            row=row, column=0, columnspan=2, sticky="w", pady=(12, 2))
        wrapper = tk.Frame(parent, bg=FIELD, highlightthickness=1,
                           highlightbackground=BORDER, highlightcolor=BLUE)
        wrapper.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(34, 4))
        parent.grid_rowconfigure(row, minsize=70)
        e = tk.Entry(wrapper, font=("Segoe UI", 16), bd=0, relief="flat",
                     bg=FIELD, fg=TEXT, insertbackground=TEXT,
                     show="●" if secret else "", width=26)
        e.pack(fill="x", padx=12, ipady=9)
        return e

    # ===================================================================== #
    #  MAIN LAYOUT (sidebar + content)
    # ===================================================================== #
    def show_main(self) -> None:
        self.clear()
        self.session_count = 0
        self.session_errors = 0
        self.local_log.clear()

        root = tk.Frame(self, bg=BG)
        root.pack(fill="both", expand=True)

        # ---- Sidebar ---- #
        sidebar = tk.Frame(root, bg=SIDEBAR, width=260)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        # Brand block
        brand = tk.Frame(sidebar, bg=SIDEBAR)
        brand.pack(fill="x", pady=(26, 18), padx=22)
        tk.Label(brand, text="📦  BoxID-ТТН", bg=SIDEBAR, fg="white",
                 font=("Segoe UI Semibold", 20, "bold")).pack(anchor="w")
        tk.Label(brand, text="Складський модуль", bg=SIDEBAR, fg=MUTED_LIGHT,
                 font=("Segoe UI", 11)).pack(anchor="w", pady=(2, 0))

        sep = tk.Frame(sidebar, bg=SIDEBAR_HOVER, height=1)
        sep.pack(fill="x", padx=18, pady=(4, 14))

        # User card
        user_card = tk.Frame(sidebar, bg=SIDEBAR_HOVER)
        user_card.pack(fill="x", padx=14, pady=(0, 18))
        avatar = tk.Label(user_card, text=self.user_name[:1].upper() or "?",
                          bg=BLUE, fg="white", width=3,
                          font=("Segoe UI Semibold", 16, "bold"))
        avatar.pack(side="left", padx=(10, 10), pady=10, ipady=4)
        ucol = tk.Frame(user_card, bg=SIDEBAR_HOVER)
        ucol.pack(side="left", fill="both", expand=True, pady=10)
        tk.Label(ucol, text=self.user_name, bg=SIDEBAR_HOVER, fg="white",
                 font=("Segoe UI Semibold", 13, "bold"), anchor="w").pack(
            anchor="w", fill="x")
        role_text = {"admin": "Адміністратор",
                     "operator": "Оператор"}.get(self.role, "Перегляд")
        tk.Label(ucol, text=role_text, bg=SIDEBAR_HOVER, fg=MUTED_LIGHT,
                 font=("Segoe UI", 11), anchor="w").pack(anchor="w", fill="x")

        # Nav items
        nav_items = [
            ("scan", "🔍  Сканування", self.scan_page),
            ("history", "🗂  Історія", self.history_page),
        ]
        for key, label, cmd in nav_items:
            self._make_nav(sidebar, key, label, cmd)

        # bottom area: connection + logout
        bottom = tk.Frame(sidebar, bg=SIDEBAR)
        bottom.pack(side="bottom", fill="x", pady=18, padx=14)

        self.queue_label = tk.Label(
            bottom, text="", bg=SIDEBAR, fg=AMBER,
            font=("Segoe UI Semibold", 11, "bold"), anchor="w")
        self.queue_label.pack(fill="x", pady=(0, 10))

        logout_btn = tk.Label(bottom, text="⏻  Вийти", bg=SIDEBAR_HOVER,
                              fg="white", font=("Segoe UI Semibold", 12, "bold"),
                              padx=14, pady=12, anchor="w", cursor="hand2")
        logout_btn.pack(fill="x")
        logout_btn.bind("<Button-1>", lambda e: self.logout())
        logout_btn.bind("<Enter>", lambda e: logout_btn.config(bg=RED))
        logout_btn.bind("<Leave>", lambda e: logout_btn.config(bg=SIDEBAR_HOVER))

        # ---- Content area ---- #
        self.content = tk.Frame(root, bg=BG)
        self.content.pack(side="left", fill="both", expand=True)

        self.body = tk.Frame(self.content, bg=BG)
        self.body.pack(fill="both", expand=True, padx=28, pady=24)

        self.scan_page()
        self._update_queue_label()
        self.try_sync()

    def _make_nav(self, parent: tk.Widget, key: str, label: str,
                  cmd: Callable[[], None]) -> None:
        lbl = tk.Label(parent, text=label, bg=SIDEBAR, fg=TEXT_LIGHT,
                       font=("Segoe UI", 14), anchor="w",
                       padx=22, pady=15, cursor="hand2")
        lbl.pack(fill="x", padx=8, pady=2)
        self.nav_buttons[key] = lbl

        def on_click(_e=None) -> None:
            self._set_active_nav(key)
            cmd()

        def on_enter(_e=None) -> None:
            if self.active_page != key:
                lbl.config(bg=SIDEBAR_HOVER)

        def on_leave(_e=None) -> None:
            if self.active_page != key:
                lbl.config(bg=SIDEBAR)

        lbl.bind("<Button-1>", on_click)
        lbl.bind("<Enter>", on_enter)
        lbl.bind("<Leave>", on_leave)

    def _set_active_nav(self, key: str) -> None:
        self.active_page = key
        for k, lbl in self.nav_buttons.items():
            if k == key:
                lbl.config(bg=SIDEBAR_ACTIVE, fg="white",
                           font=("Segoe UI Semibold", 14, "bold"))
            else:
                lbl.config(bg=SIDEBAR, fg=TEXT_LIGHT,
                           font=("Segoe UI", 14))

    def body_clear(self) -> None:
        for w in self.body.winfo_children():
            w.destroy()

    def _page_header(self, title: str, subtitle: str = "") -> tk.Frame:
        header = tk.Frame(self.body, bg=BG)
        header.pack(fill="x", pady=(0, 18))
        tk.Label(header, text=title, bg=BG, fg="white",
                 font=("Segoe UI Semibold", 24, "bold")).pack(anchor="w")
        if subtitle:
            tk.Label(header, text=subtitle, bg=BG, fg=MUTED_LIGHT,
                     font=("Segoe UI", 12)).pack(anchor="w", pady=(2, 0))
        return header

    def _update_queue_label(self) -> None:
        cnt = self.offline.count()
        if cnt:
            self.queue_label.config(
                text=f"📦 Офлайн-черга: {cnt}", fg=AMBER)
        else:
            self.queue_label.config(text="✅ Дані синхронізовано", fg=GREEN)

    # ===================================================================== #
    #  SCAN PAGE
    # ===================================================================== #
    def scan_page(self) -> None:
        self.active_page = "scan"
        self._set_active_nav("scan")
        self.body_clear()

        self._page_header("BoxID-ТТН",
                          "Скануйте BoxID, потім ТТН.")

        wrap = tk.Frame(self.body, bg=BG)
        wrap.pack(fill="both", expand=True)
        wrap.columnconfigure(0, weight=3)
        wrap.columnconfigure(1, weight=2)
        wrap.rowconfigure(0, weight=1)

        # ---- Left: scan card ---- #
        card = tk.Frame(wrap, bg=CARD)
        card.grid(row=0, column=0, sticky="nsew", padx=(0, 16))
        inner = tk.Frame(card, bg=CARD)
        inner.pack(fill="both", expand=True, padx=34, pady=30)

        # Session counters
        counters = tk.Frame(inner, bg=CARD)
        counters.pack(fill="x", pady=(0, 14))
        self.lbl_cnt_ok = self._counter_chip(counters, "Успішно за сесію", "0", GREEN)
        self.lbl_cnt_err = self._counter_chip(counters, "Помилки / дублі", "0", RED)

        # BoxID field
        tk.Label(inner, text="1. BoxID", bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w",
                                                              pady=(8, 4))
        self.box_wrap = tk.Frame(inner, bg=FIELD, highlightthickness=2,
                                 highlightbackground=BLUE, highlightcolor=BLUE)
        self.box_wrap.pack(fill="x")
        box = tk.Entry(self.box_wrap, font=("Segoe UI", 30), bd=0, relief="flat",
                       bg=FIELD, fg=TEXT, insertbackground=BLUE,
                       justify="center")
        box.pack(fill="x", padx=14, ipady=12)

        # TTN field
        tk.Label(inner, text="2. ТТН", bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w",
                                                              pady=(18, 4))
        self.ttn_wrap = tk.Frame(inner, bg=FIELD, highlightthickness=2,
                                 highlightbackground=BORDER, highlightcolor=BLUE)
        self.ttn_wrap.pack(fill="x")
        ttn = tk.Entry(self.ttn_wrap, font=("Segoe UI", 30), bd=0, relief="flat",
                       bg=FIELD, fg=TEXT, insertbackground=BLUE,
                       justify="center")
        ttn.pack(fill="x", padx=14, ipady=12)

        # Big status banner
        self.status = tk.Label(inner, text="Готово — відскануйте BoxID",
                               bg=BLUE, fg="white",
                               font=("Segoe UI Semibold", 24, "bold"),
                               pady=26)
        self.status.pack(fill="x", pady=(22, 0))

        # ---- Right: live log ---- #
        log_card = tk.Frame(wrap, bg=CARD)
        log_card.grid(row=0, column=1, sticky="nsew")
        log_inner = tk.Frame(log_card, bg=CARD)
        log_inner.pack(fill="both", expand=True, padx=20, pady=20)
        tk.Label(log_inner, text="Останні сканування", bg=CARD, fg=TEXT,
                 font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w",
                                                             pady=(0, 10))
        self.log_frame = tk.Frame(log_inner, bg=CARD)
        self.log_frame.pack(fill="both", expand=True)
        self._render_local_log()

        # ---- field logic ---- #
        def focus_box(active: bool) -> None:
            self.box_wrap.config(highlightbackground=BLUE if active else BORDER)
            self.ttn_wrap.config(highlightbackground=BLUE if not active else BORDER)

        def set_status(text: str, bg: str) -> None:
            self.status.config(text=text, bg=bg)

        def on_box(_=None) -> None:
            val = only_digits(box.get())
            box.delete(0, "end")
            box.insert(0, val)
            if val:
                play_sound(True)
                set_status("BoxID прийнято — скануйте ТТН", SOFT)
                focus_box(False)
                ttn.focus_set()
            else:
                set_status("Порожній BoxID — спробуйте ще раз", RED_BG)

        def on_ttn(_=None) -> None:
            b = only_digits(box.get())
            t = only_digits(ttn.get())
            if not b:
                set_status("Спочатку відскануйте BoxID", RED_BG)
                play_sound(False)
                focus_box(True)
                box.focus_set()
                return
            if not t:
                return
            box.delete(0, "end")
            ttn.delete(0, "end")
            focus_box(True)
            box.focus_set()
            set_status("⏳ Надсилання на сервер...", AMBER_BG)

            def done(res: Any, err: Exception | None) -> None:
                if err:
                    self.offline.add(self.user_name, b, t)
                    self._update_queue_label()
                    play_sound(False)
                    set_status(
                        f"📦 Збережено офлайн — черга: {self.offline.count()}",
                        AMBER_BG)
                    self._add_local_log("offline", b, t, "Немає зв'язку")
                    self.session_errors += 1
                else:
                    note = str((res or {}).get("note") or "")
                    if note:
                        play_sound(False)
                        set_status(f"⚠ Дублікат: {note}", AMBER_BG)
                        self._add_local_log("dup", b, t, note)
                        self.session_errors += 1
                    else:
                        play_sound(True)
                        set_status("✅ Успішно додано", GREEN_BG)
                        self._add_local_log("ok", b, t, "Додано")
                        self.session_count += 1
                self._update_counters()

            self.bg_task(lambda: self.api.add_record(self.user_name, b, t), done)

        box.bind("<Return>", on_box)
        ttn.bind("<Return>", on_ttn)
        box.bind("<FocusIn>", lambda e: focus_box(True))
        ttn.bind("<FocusIn>", lambda e: focus_box(False))
        box.focus_set()
        focus_box(True)

    def _counter_chip(self, parent: tk.Widget, label: str, value: str,
                      color: str) -> tk.Label:
        chip = tk.Frame(parent, bg=CARD_ALT)
        chip.pack(side="left", expand=True, fill="x", padx=4)
        val = tk.Label(chip, text=value, bg=CARD_ALT, fg=color,
                       font=("Segoe UI Semibold", 24, "bold"))
        val.pack(pady=(10, 0))
        tk.Label(chip, text=label, bg=CARD_ALT, fg=MUTED,
                 font=("Segoe UI", 11)).pack(pady=(0, 10))
        return val

    def _update_counters(self) -> None:
        try:
            self.lbl_cnt_ok.config(text=str(self.session_count))
            self.lbl_cnt_err.config(text=str(self.session_errors))
        except Exception:
            pass

    def _add_local_log(self, kind: str, box: str, ttn: str, note: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.local_log.appendleft((kind, ts, f"{box} → {ttn}", note))
        self._render_local_log()

    def _render_local_log(self) -> None:
        for w in self.log_frame.winfo_children():
            w.destroy()
        if not self.local_log:
            tk.Label(self.log_frame, text="Поки що немає сканувань",
                     bg=CARD, fg=MUTED, font=("Segoe UI", 12)).pack(
                anchor="w", pady=8)
            return
        colors = {"ok": GREEN, "dup": AMBER, "offline": AMBER, "err": RED}
        icons = {"ok": "✅", "dup": "⚠", "offline": "📦", "err": "✖"}
        for kind, ts, pair, note in self.local_log:
            row = tk.Frame(self.log_frame, bg=CARD_ALT)
            row.pack(fill="x", pady=3)
            tk.Label(row, text=icons.get(kind, "•"), bg=CARD_ALT,
                     fg=colors.get(kind, MUTED),
                     font=("Segoe UI Emoji", 14)).pack(side="left", padx=(10, 8),
                                                       pady=8)
            col = tk.Frame(row, bg=CARD_ALT)
            col.pack(side="left", fill="x", expand=True, pady=6)
            tk.Label(col, text=pair, bg=CARD_ALT, fg=TEXT,
                     font=("Segoe UI Semibold", 12, "bold"),
                     anchor="w").pack(anchor="w", fill="x")
            tk.Label(col, text=f"{ts} • {note}", bg=CARD_ALT, fg=MUTED,
                     font=("Segoe UI", 10), anchor="w").pack(anchor="w", fill="x")

    # ===================================================================== #
    #  TABLE PAGES (history)
    # ===================================================================== #
    def _make_table(self, parent: tk.Widget, columns: list[str],
                    headings: list[str], widths: list[int]) -> ttk.Treeview:
        container = tk.Frame(parent, bg=CARD)
        container.pack(fill="both", expand=True)

        tree = ttk.Treeview(container, columns=columns, show="headings")
        vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)

        for c, h, w in zip(columns, headings, widths):
            tree.heading(c, text=h)
            tree.column(c, width=w, anchor="w")

        tree.tag_configure("odd", background=CARD)
        tree.tag_configure("even", background=CARD_ALT)
        tree.tag_configure("error", background="#FDECEC", foreground=RED)

        vsb.pack(side="right", fill="y")
        tree.pack(side="left", fill="both", expand=True)
        return tree

    def history_page(self) -> None:
        self.active_page = "history"
        self._set_active_nav("history")
        self.body_clear()
        self._page_header("Історія сканувань",
                          "Усі пари BoxID — ТТН та помилки в одному списку.")

        bar = tk.Frame(self.body, bg=BG)
        bar.pack(fill="x", pady=(0, 12))

        search_var = tk.StringVar()
        search = tk.Entry(bar, textvariable=search_var, font=("Segoe UI", 13),
                          bd=0, relief="flat", bg=CARD, fg=TEXT,
                          insertbackground=BLUE, width=30)
        search.pack(side="left", ipady=8, padx=(0, 8))
        search.insert(0, "")
        tk.Label(bar, text="🔎 пошук BoxID / ТТН / користувача / опису помилки",
                 bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 11)).pack(
            side="left")

        ttk.Button(bar, text="↻ Оновити", style="Small.TButton",
                   command=self.history_page).pack(side="right")

        tree = self._make_table(
            self.body,
            ["dt", "user", "box", "ttn", "msg"],
            ["Дата і час", "Користувач", "BoxID", "ТТН", "Опис помилки"],
            [190, 170, 180, 190, 330],
        )

        self._table_raw: list[dict[str, Any]] = []

        def apply_filter(*_a) -> None:
            self._fill_table(tree, self._table_raw, errors=False,
                             query=search_var.get())

        search_var.trace_add("write", apply_filter)

        def work() -> list[dict[str, Any]]:
            history = self.api.get_history()
            error_rows = self.api.get_errors()

            combined: list[dict[str, Any]] = []
            for row in history:
                item = dict(row)
                item["_row_type"] = "history"
                combined.append(item)

            for row in error_rows:
                item = dict(row)
                item["_row_type"] = "error"
                combined.append(item)

            return combined

        def loaded(data: Any, err: Exception | None) -> None:
            if err:
                messagebox.showerror(APP_NAME, str(err))
                return
            self._table_raw = data or []
            self._fill_table(tree, self._table_raw, errors=False, query="")

        self.bg_task(work, loaded)

    def _fill_table(self, tree: ttk.Treeview, data: list[dict[str, Any]],
                    errors: bool, query: str) -> None:
        for item in tree.get_children():
            tree.delete(item)

        q = (query or "").strip().lower()
        rows = sorted(data or [],
                      key=lambda x: parse_dt(x.get("datetime")), reverse=True)

        index = 0
        for r in rows:
            user = r.get("user_name") or r.get("operator") or ""
            box = str(r.get("boxid", ""))
            ttn = str(r.get("ttn", ""))
            msg = (r.get("error_message") or r.get("reason")
                   or r.get("note") or r.get("message") or "")

            if q:
                haystack = f"{user} {box} {ttn} {msg}".lower()
                if q not in haystack:
                    continue

            if errors:
                vals = (r.get("id", ""), fmt_dt(r.get("datetime")),
                        user, box, ttn, msg)
                tag = "error"
            else:
                vals = (fmt_dt(r.get("datetime")), user, box, ttn, msg)
                is_error_row = r.get("_row_type") == "error"
                tag = "error" if (msg or is_error_row) else ("even" if index % 2 else "odd")

            tree.insert("", "end", values=vals, tags=(tag,))
            index += 1

        if index == 0:
            empty_cols = len(tree["columns"])
            placeholder = ["—"] * empty_cols
            placeholder[1 if errors else 0] = "Немає записів"
            tree.insert("", "end", values=placeholder, tags=("odd",))

    # ===================================================================== #
    #  SYNC / LOGOUT
    # ===================================================================== #
    def try_sync(self) -> None:
        if self.offline.count():
            def done(sent: Any, err: Exception | None) -> None:
                self._update_queue_label()
                if not err and sent:
                    if self.active_page == "scan":
                        try:
                            self.status.config(
                                text=f"☁ Синхронізовано {sent} запис(ів)",
                                bg=GREEN_BG)
                        except Exception:
                            pass
            self.bg_task(lambda: self.offline.sync(self.api), done)

    def _auto_sync_tick(self) -> None:
        # periodic background sync attempt
        if getattr(self, "queue_label", None) is not None:
            try:
                self._update_queue_label()
            except Exception:
                pass
            if self.api.token and self.offline.count():
                self.bg_task(
                    lambda: self.offline.sync(self.api),
                    lambda _s, _e: self._safe_update_queue(),
                )
        self.after(15000, self._auto_sync_tick)

    def _safe_update_queue(self) -> None:
        try:
            self._update_queue_label()
        except Exception:
            pass

    def logout(self) -> None:
        if messagebox.askyesno(APP_NAME, "Вийти з акаунту?"):
            self.api.token = ""
            CONFIG_PATH.unlink(missing_ok=True)
            self.nav_buttons.clear()
            self.active_page = ""
            self.show_login()


# --------------------------------------------------------------------------- #
#  Entry point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    app = App()
    app.mainloop()
