#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Windows desktop client for the BoxID-ТТН module.

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
import time
import tkinter as tk
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable
from urllib.parse import urljoin

import requests

APP_NAME = "BoxID-ТТН"
API_BASE_URL = "https://tracking-app.dclink.ua"
TIMEOUT = 12
APP_DIR = Path(os.getenv("APPDATA") or Path.home()) / "BoxID_TTN_Windows"
APP_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = APP_DIR / "boxid_ttn.sqlite3"
CONFIG_PATH = APP_DIR / "session.json"

BG = "#07153A"
CARD = "#FFFFFF"
TEXT = "#0B1530"
MUTED = "#60708C"
BLUE = "#075BFF"
SOFT = "#3F8CFF"
GREEN = "#14C9A6"
RED = "#EF4444"
AMBER = "#F59E0B"
FIELD = "#F2F5FB"


def only_digits(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def parse_dt(value: Any) -> datetime:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone().replace(tzinfo=None)
    except Exception:
        return datetime.min


def fmt_dt(value: Any) -> str:
    dt = parse_dt(value)
    return str(value) if dt == datetime.min else dt.strftime("%d.%m.%Y %H:%M:%S")


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

    def add_record(self, user_name: str, boxid: str, ttn: str) -> dict[str, Any]:
        return self._request("POST", "/add_record", data=json.dumps({"user_name": user_name, "boxid": boxid, "ttn": ttn})) or {}

    def get_history(self) -> list[dict[str, Any]]:
        return self._request("GET", "/get_history") or []

    def get_errors(self) -> list[dict[str, Any]]:
        return self._request("GET", "/get_errors") or []

    def delete_error(self, error_id: int) -> None:
        self._request("DELETE", f"/delete_error/{error_id}")

    def clear_errors(self) -> None:
        self._request("DELETE", "/clear_errors")

    def clear_history(self) -> None:
        self._request("DELETE", "/clear_tracking")


class OfflineQueue:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init()

    def _init(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute("CREATE TABLE IF NOT EXISTS pending_records(id INTEGER PRIMARY KEY AUTOINCREMENT, user_name TEXT, boxid TEXT, ttn TEXT, created_at TEXT)")

    def add(self, user_name: str, boxid: str, ttn: str) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute("INSERT INTO pending_records(user_name, boxid, ttn, created_at) VALUES(?,?,?,?)", (user_name, boxid, ttn, datetime.now().isoformat()))

    def count(self) -> int:
        with sqlite3.connect(self.db_path) as db:
            return int(db.execute("SELECT COUNT(*) FROM pending_records").fetchone()[0])

    def sync(self, api: ApiClient) -> int:
        sent = 0
        with sqlite3.connect(self.db_path) as db:
            rows = db.execute("SELECT id,user_name,boxid,ttn FROM pending_records ORDER BY id").fetchall()
        for row_id, user_name, boxid, ttn in rows:
            api.add_record(user_name, boxid, ttn)
            with sqlite3.connect(self.db_path) as db:
                db.execute("DELETE FROM pending_records WHERE id=?", (row_id,))
            sent += 1
        return sent


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("1180x760")
        self.minsize(980, 650)
        self.configure(bg=BG)
        self.api = ApiClient()
        self.offline = OfflineQueue(DB_PATH)
        self.user_name = "operator"
        self.role = "viewer"
        self.access_level = 2
        self.q: queue.Queue[Callable[[], None]] = queue.Queue()
        self._setup_style()
        self._load_session()
        self.show_login() if not self.api.token else self.show_main()
        self.after(80, self._drain_queue)

    def _setup_style(self) -> None:
        s = ttk.Style(self)
        s.theme_use("clam")
        s.configure("TFrame", background=BG)
        s.configure("Card.TFrame", background=CARD)
        s.configure("TLabel", background=BG, foreground="white", font=("Segoe UI", 12))
        s.configure("Card.TLabel", background=CARD, foreground=TEXT, font=("Segoe UI", 12))
        s.configure("Title.TLabel", background=BG, foreground="white", font=("Segoe UI", 28, "bold"))
        s.configure("Big.TButton", font=("Segoe UI", 15, "bold"), padding=14)
        s.configure("TButton", font=("Segoe UI", 11), padding=8)
        s.configure("Treeview", rowheight=34, font=("Segoe UI", 11))
        s.configure("Treeview.Heading", font=("Segoe UI", 11, "bold"))

    def _drain_queue(self) -> None:
        while True:
            try: self.q.get_nowait()()
            except queue.Empty: break
        self.after(80, self._drain_queue)

    def bg_task(self, work: Callable[[], Any], done: Callable[[Any, Exception | None], None]) -> None:
        def run() -> None:
            try: res, err = work(), None
            except Exception as exc: res, err = None, exc
            self.q.put(lambda: done(res, err))
        threading.Thread(target=run, daemon=True).start()

    def clear(self) -> None:
        for w in self.winfo_children(): w.destroy()

    def _load_session(self) -> None:
        if CONFIG_PATH.exists():
            try:
                data = json.loads(CONFIG_PATH.read_text("utf-8")); self.api.token = data.get("token", ""); self.user_name = data.get("user_name", "operator"); self.role = data.get("role", "viewer"); self.access_level = int(data.get("access_level", 2))
            except Exception: pass

    def _save_session(self) -> None:
        CONFIG_PATH.write_text(json.dumps({"token": self.api.token, "user_name": self.user_name, "role": self.role, "access_level": self.access_level}, ensure_ascii=False), "utf-8")

    def show_login(self) -> None:
        self.clear(); wrap = ttk.Frame(self); wrap.pack(expand=True, fill="both", padx=60, pady=50)
        ttk.Label(wrap, text="BoxID-ТТН", style="Title.TLabel").pack(anchor="w")
        ttk.Label(wrap, text="Windows-клієнт для складу: сканування BoxID і ТТН", font=("Segoe UI", 15), foreground="#C7D2FE", background=BG).pack(anchor="w", pady=(0, 25))
        card = ttk.Frame(wrap, style="Card.TFrame", padding=30); card.pack(anchor="center")
        ttk.Label(card, text="Вхід оператора", style="Card.TLabel", font=("Segoe UI", 22, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 18))
        surname = self.entry(card, 1, "Прізвище", False); password = self.entry(card, 2, "Пароль", True)
        msg = ttk.Label(card, text="", style="Card.TLabel", foreground=RED); msg.grid(row=3, column=0, columnspan=2, sticky="w", pady=8)
        def login() -> None:
            if not surname.get().strip() or not password.get().strip(): msg.config(text="Введіть прізвище та пароль"); return
            msg.config(text="Вхід...", foreground=MUTED)
            self.bg_task(lambda: self.api.login(surname.get().strip(), password.get().strip()), lambda res, err: after_login(res, err))
        def after_login(res: Any, err: Exception | None) -> None:
            if err: msg.config(text=str(err), foreground=RED); return
            self.user_name = str(res.get("surname") or surname.get().strip()); self.role = str(res.get("role") or "viewer"); self.access_level = int(res.get("access_level") if res.get("access_level") is not None else (1 if self.role == "admin" else 0 if self.role == "operator" else 2)); self._save_session(); self.show_main()
        ttk.Button(card, text="Увійти", style="Big.TButton", command=login).grid(row=4, column=0, sticky="ew", pady=8)
        ttk.Button(card, text="Реєстрація", command=lambda: self.show_register()).grid(row=4, column=1, sticky="ew", padx=(12,0), pady=8)
        card.columnconfigure((0,1), weight=1)
        password.bind("<Return>", lambda e: login())

    def entry(self, parent: tk.Widget, row: int, label: str, secret: bool=False) -> ttk.Entry:
        ttk.Label(parent, text=label, style="Card.TLabel", font=("Segoe UI", 12, "bold")).grid(row=row, column=0, sticky="w", pady=8)
        e = ttk.Entry(parent, font=("Segoe UI", 18), show="●" if secret else "", width=28); e.grid(row=row, column=1, sticky="ew", pady=8, padx=(15,0)); return e

    def show_register(self) -> None:
        self.clear(); card = ttk.Frame(self, style="Card.TFrame", padding=30); card.pack(expand=True)
        ttk.Label(card, text="Заявка на реєстрацію", style="Card.TLabel", font=("Segoe UI", 22, "bold")).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 18))
        surname = self.entry(card, 1, "Прізвище"); p1 = self.entry(card, 2, "Пароль", True); p2 = self.entry(card, 3, "Повтор пароля", True)
        msg = ttk.Label(card, text="", style="Card.TLabel"); msg.grid(row=4, column=0, columnspan=2, sticky="w", pady=8)
        def reg() -> None:
            if not surname.get().strip() or not p1.get() or not p2.get(): msg.config(text="Заповніть усі поля", foreground=RED); return
            if len(p1.get()) < 6: msg.config(text="Пароль має містити щонайменше 6 символів", foreground=RED); return
            if p1.get() != p2.get(): msg.config(text="Паролі не співпадають", foreground=RED); return
            self.bg_task(lambda: self.api.register(surname.get().strip(), p1.get().strip()), lambda _r, err: msg.config(text=str(err) if err else "Заявку відправлено. Дочекайтесь підтвердження адміністратора.", foreground=RED if err else GREEN))
        ttk.Button(card, text="Відправити заявку", style="Big.TButton", command=reg).grid(row=5, column=0, sticky="ew", pady=8)
        ttk.Button(card, text="Назад", command=self.show_login).grid(row=5, column=1, sticky="ew", padx=(12,0), pady=8)

    def show_main(self) -> None:
        self.clear(); top = ttk.Frame(self, padding=(24,16)); top.pack(fill="x")
        ttk.Label(top, text="BoxID-ТТН", style="Title.TLabel").pack(side="left")
        ttk.Label(top, text=f"  {self.user_name} • {self.role}  ", font=("Segoe UI", 14, "bold"), foreground="white", background=BG).pack(side="left", padx=20)
        for txt, cmd in [("Сканування", self.scan_page), ("Історія", self.history_page), ("Помилки", self.errors_page), ("Статистика", self.stats_page), ("Вийти", self.logout)]: ttk.Button(top, text=txt, command=cmd).pack(side="right", padx=4)
        self.body = ttk.Frame(self, padding=24); self.body.pack(fill="both", expand=True); self.scan_page(); self.try_sync()

    def body_clear(self) -> None:
        for w in self.body.winfo_children(): w.destroy()

    def scan_page(self) -> None:
        self.body_clear(); card = ttk.Frame(self.body, style="Card.TFrame", padding=34); card.pack(expand=True, fill="both")
        ttk.Label(card, text="Сканування пари", style="Card.TLabel", font=("Segoe UI", 26, "bold")).pack(anchor="w")
        box = ttk.Entry(card, font=("Segoe UI", 32), width=24); ttn = ttk.Entry(card, font=("Segoe UI", 32), width=24)
        status = tk.Label(card, text="Готово: відскануйте BoxID", bg=CARD, fg=BLUE, font=("Segoe UI", 28, "bold"), pady=20)
        for label, entry in [("1. BoxID", box), ("2. ТТН", ttn)]: ttk.Label(card, text=label, style="Card.TLabel", font=("Segoe UI", 18, "bold")).pack(anchor="w", pady=(24,5)); entry.pack(fill="x", ipady=12)
        status.pack(fill="x", pady=24)
        def ok_sound(ok=True):
            try: import winsound; winsound.MessageBeep(winsound.MB_OK if ok else winsound.MB_ICONHAND)
            except Exception: self.bell()
        def on_box(_=None):
            val = only_digits(box.get()); box.delete(0,"end"); box.insert(0,val)
            if val: ok_sound(True); status.config(text="BoxID прийнято. Скануйте ТТН", fg=BLUE); ttn.focus_set()
        def on_ttn(_=None):
            b, t = only_digits(box.get()), only_digits(ttn.get())
            if not b: status.config(text="Спочатку відскануйте BoxID", fg=RED); box.focus_set(); return
            if not t: return
            box.delete(0,"end"); ttn.delete(0,"end"); box.focus_set(); status.config(text="⏳ Надсилання...", fg=AMBER)
            def done(res: Any, err: Exception | None) -> None:
                if err:
                    self.offline.add(self.user_name, b, t); ok_sound(False); status.config(text=f"📦 Збережено офлайн. Черга: {self.offline.count()}", fg=AMBER)
                else:
                    note = str((res or {}).get("note") or "")
                    ok_sound(not bool(note)); status.config(text=("⚠️ Дублікат: " + note if note else "✅ Успішно додано"), fg=AMBER if note else GREEN)
            self.bg_task(lambda: self.api.add_record(self.user_name, b, t), done)
        box.bind("<Return>", on_box); ttn.bind("<Return>", on_ttn); box.focus_set()

    def make_tree(self, parent: tk.Widget, columns: list[str], headings: list[str]) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=columns, show="headings")
        for c,h in zip(columns, headings): tree.heading(c, text=h); tree.column(c, width=150)
        tree.pack(fill="both", expand=True); return tree

    def history_page(self) -> None:
        self.body_clear(); bar=ttk.Frame(self.body); bar.pack(fill="x", pady=(0,10)); ttk.Button(bar,text="Оновити",command=self.history_page).pack(side="right"); tree=self.make_tree(self.body,["dt","user","box","ttn"],["Дата","Користувач","BoxID","ТТН"])
        self.bg_task(self.api.get_history, lambda data, err: self.fill_records(tree, data, err, False))

    def errors_page(self) -> None:
        self.body_clear(); bar=ttk.Frame(self.body); bar.pack(fill="x", pady=(0,10)); ttk.Button(bar,text="Оновити",command=self.errors_page).pack(side="right"); tree=self.make_tree(self.body,["id","dt","user","box","ttn","msg"],["ID","Дата","Користувач","BoxID","ТТН","Помилка"])
        self.bg_task(self.api.get_errors, lambda data, err: self.fill_records(tree, data, err, True))

    def fill_records(self, tree: ttk.Treeview, data: Any, err: Exception | None, errors: bool) -> None:
        if err: messagebox.showerror(APP_NAME, str(err)); return
        for r in sorted(data or [], key=lambda x: parse_dt(x.get("datetime")), reverse=True):
            vals = (r.get("id",""), fmt_dt(r.get("datetime")), r.get("user_name") or r.get("operator",""), r.get("boxid",""), r.get("ttn",""), r.get("error_message") or r.get("reason") or r.get("note") or r.get("message") or "") if errors else (fmt_dt(r.get("datetime")), r.get("user_name") or r.get("operator",""), r.get("boxid",""), r.get("ttn",""))
            tree.insert("", "end", values=vals)

    def stats_page(self) -> None:
        self.body_clear(); label=tk.Label(self.body,text="Завантаження статистики...",bg=BG,fg="white",font=("Segoe UI",20,"bold")); label.pack(pady=40)
        def work(): return self.api.get_history(), self.api.get_errors()
        def done(res, err):
            if err: label.config(text=str(err), fg=RED); return
            hist, errs = res; c=Counter((r.get("user_name") or r.get("operator") or "Невідомий") for r in hist); ec=Counter((r.get("user_name") or r.get("operator") or "Невідомий") for r in errs)
            top=c.most_common(1)[0] if c else ("—",0); etop=ec.most_common(1)[0] if ec else ("—",0)
            label.config(text=f"Сканувань: {len(hist)}\nПомилок: {len(errs)}\nОператорів: {len(c)}\nЛідер сканувань: {top[0]} ({top[1]})\nЛідер помилок: {etop[0]} ({etop[1]})", fg="white", justify="left")
        self.bg_task(work, done)

    def try_sync(self) -> None:
        if self.offline.count(): self.bg_task(lambda: self.offline.sync(self.api), lambda _r,_e: None)

    def logout(self) -> None:
        if messagebox.askyesno(APP_NAME, "Вийти з акаунту?"):
            self.api.token=""; CONFIG_PATH.unlink(missing_ok=True); self.show_login()

if __name__ == "__main__":
    App().mainloop()
