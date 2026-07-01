#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Windows desktop client for the СканПак module.

Складський клієнт для сканування номерів посилок СканПак.

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
from collections import deque
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable

import requests

APP_NAME = "СканПак"
API_BASE_URL = "https://tracking-app.dclink.ua"
API_BASE_PATH = "/scanpak"
TIMEOUT = 12

APP_DIR = Path(os.getenv("APPDATA") or Path.home()) / "ScanPak_Windows"
APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "scanpak.sqlite3"
CONFIG_PATH = APP_DIR / "session.json"

BG = "#0A1330"
SIDEBAR = "#0E1A45"
SIDEBAR_HOVER = "#16266B"
SIDEBAR_ACTIVE = "#1E5BFF"
CARD = "#FFFFFF"
CARD_ALT = "#F4F7FE"
TEXT = "#0B1530"
TEXT_LIGHT = "#E8EEFF"
MUTED = "#7B89A8"
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


def only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def parse_dt(value: Any) -> datetime:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone().replace(tzinfo=None)
    except Exception:
        return datetime.min


def fmt_dt(value: Any) -> str:
    dt = parse_dt(value)
    return str(value or "") if dt == datetime.min else dt.strftime("%d.%m.%Y %H:%M:%S")


class ApiError(Exception):
    pass


class ApiClient:
    def __init__(self, base_url: str = API_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.token = ""

    def _url(self, path: str) -> str:
        return f"{self.base_url}{API_BASE_PATH}{path}"

    def _headers(self, auth: bool = True) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, *, auth: bool = True, **kwargs: Any) -> Any:
        try:
            r = self.session.request(method, self._url(path), headers=self._headers(auth), timeout=TIMEOUT, **kwargs)
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

    def login(self, surname: str, password: str) -> dict[str, Any]:
        data = self._request("POST", "/login", auth=False, data=json.dumps({"surname": surname, "password": password}))
        token = str(data.get("token") or "")
        if not token:
            raise ApiError("Сервер не повернув токен")
        self.token = token
        return data

    def register(self, surname: str, password: str) -> None:
        self._request("POST", "/register", auth=False, data=json.dumps({"surname": surname, "password": password}))

    def add_scan(self, parcel_number: str) -> dict[str, Any]:
        return self._request("POST", "/scans", data=json.dumps({"parcel_number": parcel_number})) or {}

    def get_history(self) -> list[dict[str, Any]]:
        return self._request("GET", "/history") or []


class OfflineQueue:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init()

    def _init(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS pending_scans("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, parcel_number TEXT, created_at TEXT)"
            )

    def add(self, parcel_number: str) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute("INSERT INTO pending_scans(parcel_number, created_at) VALUES(?,?)", (parcel_number, datetime.now().isoformat()))

    def contains(self, parcel_number: str) -> bool:
        with sqlite3.connect(self.db_path) as db:
            row = db.execute("SELECT 1 FROM pending_scans WHERE parcel_number=? LIMIT 1", (parcel_number,)).fetchone()
        return row is not None

    def count(self) -> int:
        with sqlite3.connect(self.db_path) as db:
            return int(db.execute("SELECT COUNT(*) FROM pending_scans").fetchone()[0])

    def sync(self, api: ApiClient) -> int:
        sent = 0
        with sqlite3.connect(self.db_path) as db:
            rows = db.execute("SELECT id,parcel_number FROM pending_scans ORDER BY id").fetchall()
        for row_id, parcel_number in rows:
            api.add_scan(parcel_number)
            with sqlite3.connect(self.db_path) as db:
                db.execute("DELETE FROM pending_scans WHERE id=?", (row_id,))
            sent += 1
        return sent


def play_sound(ok: bool) -> None:
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_OK if ok else winsound.MB_ICONHAND)
    except Exception:
        pass


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
        self.q: queue.Queue[Callable[[], None]] = queue.Queue()
        self.nav_buttons: dict[str, tk.Label] = {}
        self.active_page = ""
        self.session_count = 0
        self.session_errors = 0
        self.local_log: deque[tuple[str, str, str, str]] = deque(maxlen=12)
        self.history_numbers: set[str] = set()
        self._setup_style()
        self._load_session()
        self.show_main() if self.api.token else self.show_login()
        self.after(80, self._drain_queue)
        self.after(15000, self._auto_sync_tick)

    def _setup_style(self) -> None:
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=BG)
        s.configure("Card.TFrame", background=CARD)
        s.configure("TLabel", background=BG, foreground=TEXT_LIGHT, font=("Segoe UI", 12))
        s.configure("Accent.TButton", font=("Segoe UI Semibold", 15, "bold"), padding=14, background=BLUE, foreground="white", borderwidth=0, focuscolor=BLUE)
        s.map("Accent.TButton", background=[("active", SOFT)])
        s.configure("Ghost.TButton", font=("Segoe UI", 12), padding=12, background=CARD_ALT, foreground=TEXT, borderwidth=0)
        s.map("Ghost.TButton", background=[("active", BORDER)])
        s.configure("Small.TButton", font=("Segoe UI", 11), padding=8, background=CARD_ALT, foreground=TEXT, borderwidth=0)
        s.map("Small.TButton", background=[("active", BORDER)])
        s.configure("Treeview", background=CARD, fieldbackground=CARD, foreground=TEXT, rowheight=40, font=("Segoe UI", 11), borderwidth=0)
        s.configure("Treeview.Heading", background=SIDEBAR, foreground="white", font=("Segoe UI Semibold", 11, "bold"), padding=8, relief="flat")
        s.map("Treeview.Heading", background=[("active", SIDEBAR_HOVER)])
        s.map("Treeview", background=[("selected", "#CFE0FF")], foreground=[("selected", TEXT)])
        s.configure("Vertical.TScrollbar", background=CARD_ALT, troughcolor=CARD, borderwidth=0, arrowcolor=MUTED)

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
                res, err = work(), None
            except Exception as exc:
                res, err = None, exc
            self.q.put(lambda: done(res, err))
        threading.Thread(target=run, daemon=True).start()

    def clear(self) -> None:
        for w in self.winfo_children():
            w.destroy()

    def _load_session(self) -> None:
        if CONFIG_PATH.exists():
            try:
                data = json.loads(CONFIG_PATH.read_text("utf-8"))
                self.api.token = data.get("token", "")
                self.user_name = data.get("user_name", "operator")
                self.role = data.get("role", "viewer")
            except Exception:
                pass

    def _save_session(self) -> None:
        CONFIG_PATH.write_text(json.dumps({"token": self.api.token, "user_name": self.user_name, "role": self.role}, ensure_ascii=False), "utf-8")

    def show_login(self) -> None:
        self.clear()
        outer = tk.Frame(self, bg=BG); outer.pack(expand=True, fill="both")
        center = tk.Frame(outer, bg=BG); center.place(relx=0.5, rely=0.5, anchor="center")
        brand = tk.Frame(center, bg=BG); brand.pack(pady=(0, 22))
        tk.Label(brand, text="📦", bg=BG, fg="white", font=("Segoe UI Emoji", 40)).pack()
        tk.Label(brand, text="СканПак", bg=BG, fg="white", font=("Segoe UI Semibold", 34, "bold")).pack()
        tk.Label(brand, text="Складський модуль сканування", bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 14)).pack(pady=(4, 0))
        card = tk.Frame(center, bg=CARD); card.pack(ipadx=40, ipady=10)
        inner = tk.Frame(card, bg=CARD); inner.pack(padx=44, pady=36)
        tk.Label(inner, text="Вхід оператора", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 22, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 20))
        surname = self._form_entry(inner, 1, "Прізвище", False)
        password = self._form_entry(inner, 2, "Пароль", True)
        msg = tk.Label(inner, text="", bg=CARD, fg=RED, font=("Segoe UI", 12)); msg.grid(row=3, column=0, columnspan=2, sticky="w", pady=(6, 8))
        def login() -> None:
            if not surname.get().strip() or not password.get().strip():
                msg.config(text="Введіть прізвище та пароль", fg=RED); return
            msg.config(text="Перевірка даних...", fg=MUTED)
            self.bg_task(lambda: self.api.login(surname.get().strip(), password.get().strip()), after_login)
        def after_login(res: Any, err: Exception | None) -> None:
            if err:
                msg.config(text=str(err), fg=RED); play_sound(False); return
            self.user_name = str(res.get("surname") or surname.get().strip())
            self.role = str(res.get("role") or "viewer")
            self._save_session(); self.show_main()
        ttk.Button(inner, text="Увійти", style="Accent.TButton", command=login).grid(row=4, column=0, columnspan=2, sticky="ew", pady=(8, 8))
        ttk.Button(inner, text="Реєстрація нового оператора", style="Ghost.TButton", command=self.show_register).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(2, 0))
        inner.columnconfigure(1, weight=1); surname.focus_set(); surname.bind("<Return>", lambda e: password.focus_set()); password.bind("<Return>", lambda e: login())

    def _form_entry(self, parent: tk.Widget, row: int, label: str, secret: bool) -> tk.Entry:
        tk.Label(parent, text=label, bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 12, "bold")).grid(row=row, column=0, sticky="w", pady=(10, 2), columnspan=2)
        wrapper = tk.Frame(parent, bg=FIELD, highlightthickness=1, highlightbackground=BORDER, highlightcolor=BLUE)
        wrapper.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(34, 6))
        parent.grid_rowconfigure(row, minsize=70)
        e = tk.Entry(wrapper, font=("Segoe UI", 17), bd=0, relief="flat", bg=FIELD, fg=TEXT, insertbackground=TEXT, show="●" if secret else "", width=26)
        e.pack(fill="x", padx=12, ipady=10)
        return e

    def show_register(self) -> None:
        self.clear(); outer = tk.Frame(self, bg=BG); outer.pack(expand=True, fill="both")
        card = tk.Frame(outer, bg=CARD); card.place(relx=0.5, rely=0.5, anchor="center")
        inner = tk.Frame(card, bg=CARD); inner.pack(padx=46, pady=40)
        tk.Label(inner, text="Заявка на реєстрацію", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 22, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 16))
        surname = self._form_entry(inner, 1, "Прізвище", False); p1 = self._form_entry(inner, 2, "Пароль", True); p2 = self._form_entry(inner, 3, "Повтор пароля", True)
        msg = tk.Label(inner, text="", bg=CARD, fg=RED, font=("Segoe UI", 12)); msg.grid(row=4, column=0, columnspan=2, sticky="w", pady=(4, 8))
        def reg() -> None:
            if not surname.get().strip() or not p1.get() or not p2.get():
                msg.config(text="Заповніть усі поля", fg=RED); return
            if len(p1.get()) < 6:
                msg.config(text="Пароль має містити щонайменше 6 символів", fg=RED); return
            if p1.get() != p2.get():
                msg.config(text="Паролі не співпадають", fg=RED); return
            msg.config(text="Відправлення...", fg=MUTED)
            self.bg_task(lambda: self.api.register(surname.get().strip(), p1.get().strip()), lambda _r, err: msg.config(text=str(err) if err else "Заявку відправлено. Дочекайтесь підтвердження адміністратора.", fg=RED if err else GREEN))
        ttk.Button(inner, text="Відправити заявку", style="Accent.TButton", command=reg).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(8, 8))
        ttk.Button(inner, text="Назад до входу", style="Ghost.TButton", command=self.show_login).grid(row=6, column=0, columnspan=2, sticky="ew")
        inner.columnconfigure(1, weight=1)

    def show_main(self) -> None:
        self.clear(); self.session_count = 0; self.session_errors = 0; self.local_log.clear()
        root = tk.Frame(self, bg=BG); root.pack(fill="both", expand=True)
        sidebar = tk.Frame(root, bg=SIDEBAR, width=260); sidebar.pack(side="left", fill="y"); sidebar.pack_propagate(False)
        brand = tk.Frame(sidebar, bg=SIDEBAR); brand.pack(fill="x", pady=(26, 18), padx=22)
        tk.Label(brand, text="📦  СканПак", bg=SIDEBAR, fg="white", font=("Segoe UI Semibold", 20, "bold")).pack(anchor="w")
        tk.Label(brand, text="Складський модуль", bg=SIDEBAR, fg=MUTED_LIGHT, font=("Segoe UI", 11)).pack(anchor="w", pady=(2, 0))
        tk.Frame(sidebar, bg=SIDEBAR_HOVER, height=1).pack(fill="x", padx=18, pady=(4, 14))
        user_card = tk.Frame(sidebar, bg=SIDEBAR_HOVER); user_card.pack(fill="x", padx=14, pady=(0, 18))
        tk.Label(user_card, text=self.user_name[:1].upper() or "?", bg=BLUE, fg="white", width=3, font=("Segoe UI Semibold", 16, "bold")).pack(side="left", padx=(10, 10), pady=10, ipady=4)
        ucol = tk.Frame(user_card, bg=SIDEBAR_HOVER); ucol.pack(side="left", fill="both", expand=True, pady=10)
        tk.Label(ucol, text=self.user_name, bg=SIDEBAR_HOVER, fg="white", font=("Segoe UI Semibold", 13, "bold"), anchor="w").pack(anchor="w", fill="x")
        tk.Label(ucol, text={"admin": "Адміністратор", "operator": "Оператор"}.get(self.role, "Перегляд"), bg=SIDEBAR_HOVER, fg=MUTED_LIGHT, font=("Segoe UI", 11), anchor="w").pack(anchor="w", fill="x")
        for key, label, cmd in [("scan", "🔍  Сканування", self.scan_page), ("history", "🗂  Історія", self.history_page)]:
            self._make_nav(sidebar, key, label, cmd)
        bottom = tk.Frame(sidebar, bg=SIDEBAR); bottom.pack(side="bottom", fill="x", pady=18, padx=14)
        self.queue_label = tk.Label(bottom, text="", bg=SIDEBAR, fg=AMBER, font=("Segoe UI Semibold", 11, "bold"), anchor="w"); self.queue_label.pack(fill="x", pady=(0, 10))
        logout_btn = tk.Label(bottom, text="⏻  Вийти", bg=SIDEBAR_HOVER, fg="white", font=("Segoe UI Semibold", 12, "bold"), padx=14, pady=12, anchor="w", cursor="hand2")
        logout_btn.pack(fill="x"); logout_btn.bind("<Button-1>", lambda e: self.logout()); logout_btn.bind("<Enter>", lambda e: logout_btn.config(bg=RED)); logout_btn.bind("<Leave>", lambda e: logout_btn.config(bg=SIDEBAR_HOVER))
        self.content = tk.Frame(root, bg=BG); self.content.pack(side="left", fill="both", expand=True)
        self.body = tk.Frame(self.content, bg=BG); self.body.pack(fill="both", expand=True, padx=28, pady=24)
        self.scan_page(); self._update_queue_label(); self.try_sync(); self._refresh_history_cache()

    def _make_nav(self, parent: tk.Widget, key: str, label: str, cmd: Callable[[], None]) -> None:
        lbl = tk.Label(parent, text=label, bg=SIDEBAR, fg=TEXT_LIGHT, font=("Segoe UI", 14), anchor="w", padx=22, pady=15, cursor="hand2")
        lbl.pack(fill="x", padx=8, pady=2); self.nav_buttons[key] = lbl
        lbl.bind("<Button-1>", lambda _e=None: (self._set_active_nav(key), cmd()))
        lbl.bind("<Enter>", lambda _e=None: lbl.config(bg=SIDEBAR_HOVER) if self.active_page != key else None)
        lbl.bind("<Leave>", lambda _e=None: lbl.config(bg=SIDEBAR) if self.active_page != key else None)

    def _set_active_nav(self, key: str) -> None:
        self.active_page = key
        for k, lbl in self.nav_buttons.items():
            lbl.config(bg=SIDEBAR_ACTIVE if k == key else SIDEBAR, fg="white" if k == key else TEXT_LIGHT, font=("Segoe UI Semibold", 14, "bold") if k == key else ("Segoe UI", 14))

    def body_clear(self) -> None:
        for w in self.body.winfo_children():
            w.destroy()

    def _page_header(self, title: str, subtitle: str = "") -> None:
        header = tk.Frame(self.body, bg=BG); header.pack(fill="x", pady=(0, 18))
        tk.Label(header, text=title, bg=BG, fg="white", font=("Segoe UI Semibold", 24, "bold")).pack(anchor="w")
        if subtitle:
            tk.Label(header, text=subtitle, bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 12)).pack(anchor="w", pady=(2, 0))

    def _update_queue_label(self) -> None:
        cnt = self.offline.count()
        self.queue_label.config(text=f"📦 Офлайн-черга: {cnt}" if cnt else "✅ Дані синхронізовано", fg=AMBER if cnt else GREEN)

    def scan_page(self) -> None:
        self._set_active_nav("scan"); self.body_clear(); self._page_header("СканПак", "Скануйте номер посилки.")
        wrap = tk.Frame(self.body, bg=BG); wrap.pack(fill="both", expand=True); wrap.columnconfigure(0, weight=3); wrap.columnconfigure(1, weight=2); wrap.rowconfigure(0, weight=1)
        card = tk.Frame(wrap, bg=CARD); card.grid(row=0, column=0, sticky="nsew", padx=(0, 16)); inner = tk.Frame(card, bg=CARD); inner.pack(fill="both", expand=True, padx=34, pady=30)
        counters = tk.Frame(inner, bg=CARD); counters.pack(fill="x", pady=(0, 14)); self.lbl_cnt_ok = self._counter_chip(counters, "Успішно за сесію", "0", GREEN); self.lbl_cnt_err = self._counter_chip(counters, "Помилки / дублі", "0", RED)
        tk.Label(inner, text="1. Номер посилки", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w", pady=(8, 4))
        self.number_wrap = tk.Frame(inner, bg=FIELD, highlightthickness=2, highlightbackground=BLUE, highlightcolor=BLUE); self.number_wrap.pack(fill="x")
        number = tk.Entry(self.number_wrap, font=("Segoe UI", 34), bd=0, relief="flat", bg=FIELD, fg=TEXT, insertbackground=BLUE, justify="center")
        number.pack(fill="x", padx=14, ipady=16)
        self.status = tk.Label(inner, text="Готово — відскануйте номер посилки", bg=BLUE, fg="white", font=("Segoe UI Semibold", 24, "bold"), pady=26); self.status.pack(fill="x", pady=(22, 0))
        log_card = tk.Frame(wrap, bg=CARD); log_card.grid(row=0, column=1, sticky="nsew"); log_inner = tk.Frame(log_card, bg=CARD); log_inner.pack(fill="both", expand=True, padx=20, pady=20)
        tk.Label(log_inner, text="Останні сканування", bg=CARD, fg=TEXT, font=("Segoe UI Semibold", 16, "bold")).pack(anchor="w", pady=(0, 10))
        self.log_frame = tk.Frame(log_inner, bg=CARD); self.log_frame.pack(fill="both", expand=True); self._render_local_log()
        def submit(_=None) -> None:
            digits = only_digits(number.get()); number.delete(0, "end")
            if not digits:
                self.status.config(text="Не знайшли цифр у введенні", bg=RED_BG); play_sound(False); number.focus_set(); return
            if digits in self.history_numbers or self.offline.contains(digits):
                self.status.config(text="⚠ Дублікат: не збережено", bg=AMBER_BG); self._add_local_log("dup", digits, "Дублікат"); self.session_errors += 1; self._update_counters(); play_sound(False); number.focus_set(); return
            self.status.config(text="⏳ Надсилання на сервер...", bg=AMBER_BG); number.focus_set()
            def done(res: Any, err: Exception | None) -> None:
                if err:
                    self.offline.add(digits); self._update_queue_label(); self.status.config(text=f"📦 Збережено офлайн — черга: {self.offline.count()}", bg=AMBER_BG); self._add_local_log("offline", digits, "Немає зв'язку"); self.session_errors += 1; play_sound(False)
                else:
                    user = str((res or {}).get("username") or self.user_name); ts = fmt_dt((res or {}).get("scanned_at") or datetime.now().isoformat())
                    self.history_numbers.add(digits); self.status.config(text=f"✅ Збережено для {user}", bg=GREEN_BG); self._add_local_log("ok", digits, ts); self.session_count += 1; play_sound(True)
                self._update_counters()
            self.bg_task(lambda: self.api.add_scan(digits), done)
        number.bind("<Return>", submit); number.focus_set()

    def _counter_chip(self, parent: tk.Widget, label: str, value: str, color: str) -> tk.Label:
        chip = tk.Frame(parent, bg=CARD_ALT); chip.pack(side="left", expand=True, fill="x", padx=4)
        val = tk.Label(chip, text=value, bg=CARD_ALT, fg=color, font=("Segoe UI Semibold", 24, "bold")); val.pack(pady=(10, 0))
        tk.Label(chip, text=label, bg=CARD_ALT, fg=MUTED, font=("Segoe UI", 11)).pack(pady=(0, 10)); return val

    def _update_counters(self) -> None:
        try:
            self.lbl_cnt_ok.config(text=str(self.session_count)); self.lbl_cnt_err.config(text=str(self.session_errors))
        except Exception:
            pass

    def _add_local_log(self, kind: str, number: str, note: str) -> None:
        self.local_log.appendleft((kind, datetime.now().strftime("%H:%M:%S"), number, note)); self._render_local_log()

    def _render_local_log(self) -> None:
        for w in self.log_frame.winfo_children():
            w.destroy()
        if not self.local_log:
            tk.Label(self.log_frame, text="Поки що немає сканувань", bg=CARD, fg=MUTED, font=("Segoe UI", 12)).pack(anchor="w", pady=8); return
        colors = {"ok": GREEN, "dup": AMBER, "offline": AMBER, "err": RED}; icons = {"ok": "✅", "dup": "⚠", "offline": "📦", "err": "✖"}
        for kind, ts, number, note in self.local_log:
            row = tk.Frame(self.log_frame, bg=CARD_ALT); row.pack(fill="x", pady=3)
            tk.Label(row, text=icons.get(kind, "•"), bg=CARD_ALT, fg=colors.get(kind, MUTED), font=("Segoe UI Emoji", 14)).pack(side="left", padx=(10, 8), pady=8)
            col = tk.Frame(row, bg=CARD_ALT); col.pack(side="left", fill="x", expand=True, pady=6)
            tk.Label(col, text=number, bg=CARD_ALT, fg=TEXT, font=("Segoe UI Semibold", 12, "bold"), anchor="w").pack(anchor="w", fill="x")
            tk.Label(col, text=f"{ts} • {note}", bg=CARD_ALT, fg=MUTED, font=("Segoe UI", 10), anchor="w").pack(anchor="w", fill="x")

    def _make_table(self, parent: tk.Widget, columns: list[str], headings: list[str], widths: list[int]) -> ttk.Treeview:
        container = tk.Frame(parent, bg=CARD); container.pack(fill="both", expand=True)
        tree = ttk.Treeview(container, columns=columns, show="headings"); vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview); tree.configure(yscrollcommand=vsb.set)
        for c, h, w in zip(columns, headings, widths):
            tree.heading(c, text=h); tree.column(c, width=w, anchor="w")
        tree.tag_configure("odd", background=CARD); tree.tag_configure("even", background=CARD_ALT); tree.tag_configure("error", background="#FDECEC", foreground=RED)
        vsb.pack(side="right", fill="y"); tree.pack(side="left", fill="both", expand=True); return tree

    def history_page(self) -> None:
        self._set_active_nav("history"); self.body_clear(); self._page_header("Історія сканувань", "Дані завантажуються з /scanpak/history.")
        bar = tk.Frame(self.body, bg=BG); bar.pack(fill="x", pady=(0, 12)); search_var = tk.StringVar()
        search = tk.Entry(bar, textvariable=search_var, font=("Segoe UI", 13), bd=0, relief="flat", bg=CARD, fg=TEXT, insertbackground=BLUE, width=30); search.pack(side="left", ipady=8, padx=(0, 8))
        tk.Label(bar, text="🔎 пошук номера посилки / користувача", bg=BG, fg=MUTED_LIGHT, font=("Segoe UI", 11)).pack(side="left")
        ttk.Button(bar, text="↻ Оновити", style="Small.TButton", command=self.history_page).pack(side="right")
        tree = self._make_table(self.body, ["dt", "user", "number"], ["Дата і час", "Користувач", "Номер посилки"], [220, 220, 420])
        self._table_raw: list[dict[str, Any]] = []
        search_var.trace_add("write", lambda *_a: self._fill_table(tree, self._table_raw, search_var.get()))
        def loaded(data: Any, err: Exception | None) -> None:
            if err:
                messagebox.showerror(APP_NAME, str(err)); return
            self._table_raw = data or []; self.history_numbers = {str(r.get("parcel_number") or "").strip() for r in self._table_raw if str(r.get("parcel_number") or "").strip()}; self._fill_table(tree, self._table_raw, "")
        self.bg_task(self.api.get_history, loaded)

    def _fill_table(self, tree: ttk.Treeview, data: list[dict[str, Any]], query: str) -> None:
        for item in tree.get_children(): tree.delete(item)
        q = (query or "").strip().lower(); rows = sorted(data or [], key=lambda x: parse_dt(x.get("scanned_at")), reverse=True); index = 0
        for r in rows:
            user = str(r.get("username") or ""); number = str(r.get("parcel_number") or "")
            if q and q not in f"{user} {number}".lower(): continue
            tree.insert("", "end", values=(fmt_dt(r.get("scanned_at")), user, number), tags=("even" if index % 2 else "odd",)); index += 1
        if index == 0: tree.insert("", "end", values=("Немає записів", "—", "—"), tags=("odd",))

    def _refresh_history_cache(self) -> None:
        def loaded(data: Any, err: Exception | None) -> None:
            if not err:
                self.history_numbers = {str(r.get("parcel_number") or "").strip() for r in (data or []) if str(r.get("parcel_number") or "").strip()}
        self.bg_task(self.api.get_history, loaded)

    def try_sync(self) -> None:
        if self.offline.count():
            def done(sent: Any, err: Exception | None) -> None:
                self._update_queue_label()
                if not err and sent and self.active_page == "scan":
                    try: self.status.config(text=f"☁ Синхронізовано {sent} запис(ів)", bg=GREEN_BG)
                    except Exception: pass
            self.bg_task(lambda: self.offline.sync(self.api), done)

    def _auto_sync_tick(self) -> None:
        if getattr(self, "queue_label", None) is not None:
            try: self._update_queue_label()
            except Exception: pass
            if self.api.token and self.offline.count():
                self.bg_task(lambda: self.offline.sync(self.api), lambda _s, _e: self._safe_update_queue())
        self.after(15000, self._auto_sync_tick)

    def _safe_update_queue(self) -> None:
        try: self._update_queue_label()
        except Exception: pass

    def logout(self) -> None:
        if messagebox.askyesno(APP_NAME, "Вийти з акаунту?"):
            self.api.token = ""; CONFIG_PATH.unlink(missing_ok=True); self.nav_buttons.clear(); self.active_page = ""; self.show_login()


if __name__ == "__main__":
    app = App()
    app.mainloop()
