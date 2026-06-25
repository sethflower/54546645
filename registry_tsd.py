#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Windows desktop admin panel for the BoxID-ТТН project.

Install: pip install requests
Run:     python admin_panel_windows.py
"""
from __future__ import annotations

import json
import queue
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any, Callable
import os
import requests

APP_NAME = "Адмін-панель BoxID-ТТН"
API_BASE_URL = "https://tracking-app.dclink.ua"
TIMEOUT = 12
APP_DIR = Path(os.getenv("APPDATA") or Path.home()) / "BoxID_TTN_Windows"
APP_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_PATH = APP_DIR / "admin_session.json"
BG="#07153A"; CARD="#FFFFFF"; TEXT="#0B1530"; MUTED="#60708C"; BLUE="#075BFF"; GREEN="#14C9A6"; RED="#EF4444"; AMBER="#F59E0B"
ROLES = {"admin": ("Адмін", "Повний доступ"), "operator": ("Оператор", "Сканування та базові дії"), "viewer": ("Перегляд", "Перегляд без змін")}


def parse_dt(v: Any) -> datetime:
    try: return datetime.fromisoformat(str(v).replace("Z","+00:00")).astimezone().replace(tzinfo=None)
    except Exception: return datetime.min

def fmt_dt(v: Any) -> str:
    d=parse_dt(v); return "—" if d == datetime.min else d.strftime("%d.%m.%Y %H:%M")

class ApiError(Exception): pass

class ApiClient:
    def __init__(self) -> None:
        self.session=requests.Session(); self.token=""; self.base=API_BASE_URL.rstrip("/")
    def h(self, auth=True):
        out={"Accept":"application/json","Content-Type":"application/json"}
        if auth and self.token: out["Authorization"]="Bearer "+self.token
        return out
    def req(self, method: str, path: str, auth=True, **kw: Any) -> Any:
        try: r=self.session.request(method, self.base+path, headers=self.h(auth), timeout=TIMEOUT, **kw)
        except requests.RequestException as e: raise ApiError("Немає зв'язку з сервером") from e
        if not (200 <= r.status_code < 300):
            try: b=r.json(); msg=b.get("detail") or b.get("message") or r.text
            except Exception: msg=r.text
            raise ApiError(msg or f"Помилка сервера ({r.status_code})")
        if not r.text: return None
        try: return r.json()
        except ValueError: return None
    def admin_login(self, password: str) -> str:
        data=self.req("POST","/admin_login",auth=False,data=json.dumps({"password":password})) or {}
        token=str(data.get("token") or "")
        if not token: raise ApiError("Сервер не повернув токен адміністратора")
        self.token=token; return token
    def fetch_pending(self): return self.req("GET","/admin/registration_requests") or []
    def approve(self, request_id: int, role: str): self.req("POST",f"/admin/registration_requests/{request_id}/approve",data=json.dumps({"role":role}))
    def reject(self, request_id: int): self.req("POST",f"/admin/registration_requests/{request_id}/reject")
    def fetch_users(self): return self.req("GET","/admin/users") or []
    def update_user(self, user_id: int, **payload: Any): return self.req("PATCH",f"/admin/users/{user_id}",data=json.dumps(payload))
    def delete_user(self, user_id: int): self.req("DELETE",f"/admin/users/{user_id}")
    def fetch_role_passwords(self): return self.req("GET","/admin/role-passwords") or {}
    def update_role_password(self, role: str, password: str): self.req("POST",f"/admin/role-passwords/{role}",data=json.dumps({"password":password}))

class AdminApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__(); self.title(APP_NAME); self.geometry("1220x760"); self.minsize(1050,650); self.configure(bg=BG)
        self.api=ApiClient(); self.q: queue.Queue[Callable[[],None]]=queue.Queue(); self.pending=[]; self.users=[]; self.passwords={}; self.filter_var=tk.StringVar()
        self.style(); self.load_session(); self.show_login() if not self.api.token else self.show_panel(); self.after(80,self.drain)
    def style(self):
        s=ttk.Style(self); s.theme_use("clam"); s.configure("TFrame",background=BG); s.configure("Card.TFrame",background=CARD); s.configure("TLabel",background=BG,foreground="white",font=("Segoe UI",12)); s.configure("Card.TLabel",background=CARD,foreground=TEXT,font=("Segoe UI",12)); s.configure("Title.TLabel",background=BG,foreground="white",font=("Segoe UI",28,"bold")); s.configure("Big.TButton",font=("Segoe UI",14,"bold"),padding=12); s.configure("Treeview",rowheight=36,font=("Segoe UI",11)); s.configure("Treeview.Heading",font=("Segoe UI",11,"bold"))
    def drain(self):
        while True:
            try: self.q.get_nowait()()
            except queue.Empty: break
        self.after(80,self.drain)
    def bg(self, work, done):
        def run():
            try: res,err=work(),None
            except Exception as e: res,err=None,e
            self.q.put(lambda: done(res,err))
        threading.Thread(target=run,daemon=True).start()
    def clear(self):
        for w in self.winfo_children(): w.destroy()
    def load_session(self):
        if CONFIG_PATH.exists():
            try: self.api.token=json.loads(CONFIG_PATH.read_text("utf-8")).get("token","")
            except Exception: pass
    def save_session(self): CONFIG_PATH.write_text(json.dumps({"token":self.api.token}),"utf-8")
    def show_login(self):
        self.clear(); wrap=ttk.Frame(self,padding=50); wrap.pack(expand=True,fill="both"); ttk.Label(wrap,text="Адмін-панель BoxID-ТТН",style="Title.TLabel").pack(anchor="w"); ttk.Label(wrap,text="Окремий Windows-додаток тільки для адміністрування користувачів",background=BG,foreground="#C7D2FE",font=("Segoe UI",15)).pack(anchor="w",pady=(0,30))
        card=ttk.Frame(wrap,style="Card.TFrame",padding=32); card.pack(anchor="center"); ttk.Label(card,text="Вхід адміністратора",style="Card.TLabel",font=("Segoe UI",22,"bold")).grid(row=0,column=0,columnspan=2,sticky="w",pady=(0,20)); ttk.Label(card,text="Пароль адміністратора",style="Card.TLabel",font=("Segoe UI",13,"bold")).grid(row=1,column=0,sticky="w")
        pwd=ttk.Entry(card,show="●",font=("Segoe UI",18),width=30); pwd.grid(row=1,column=1,padx=(14,0),ipady=6); msg=ttk.Label(card,text="",style="Card.TLabel",foreground=RED); msg.grid(row=2,column=0,columnspan=2,sticky="w",pady=10)
        def login():
            if not pwd.get().strip(): msg.config(text="Введіть пароль"); return
            msg.config(text="Перевірка...",foreground=MUTED); self.bg(lambda:self.api.admin_login(pwd.get().strip()), lambda _r,e: (msg.config(text=str(e),foreground=RED) if e else (self.save_session(), self.show_panel())))
        ttk.Button(card,text="Увійти",style="Big.TButton",command=login).grid(row=3,column=0,columnspan=2,sticky="ew",pady=8); pwd.bind("<Return>",lambda e:login()); pwd.focus_set()
    def show_panel(self):
        self.clear(); top=ttk.Frame(self,padding=(24,16)); top.pack(fill="x"); ttk.Label(top,text="Адмін-панель",style="Title.TLabel").pack(side="left"); ttk.Button(top,text="Оновити",command=self.load_data).pack(side="right",padx=4); ttk.Button(top,text="Вийти",command=self.logout).pack(side="right",padx=4)
        self.nb=ttk.Notebook(self); self.nb.pack(fill="both",expand=True,padx=20,pady=(0,20)); self.tab_pending=ttk.Frame(self.nb,padding=16); self.tab_users=ttk.Frame(self.nb,padding=16); self.tab_pass=ttk.Frame(self.nb,padding=16); self.nb.add(self.tab_pending,text="Нові заявки"); self.nb.add(self.tab_users,text="Користувачі"); self.nb.add(self.tab_pass,text="API паролі ролей"); self.load_data()
    def load_data(self):
        for tab in [self.tab_pending,self.tab_users,self.tab_pass]:
            for w in tab.winfo_children(): w.destroy()
        ttk.Label(self.tab_pending,text="Завантаження...",background=BG,foreground="white",font=("Segoe UI",18,"bold")).pack(pady=30)
        def work(): return self.api.fetch_pending(), self.api.fetch_users(), self.api.fetch_role_passwords()
        def done(res,err):
            if err: messagebox.showerror(APP_NAME,str(err)); return
            self.pending,self.users,self.passwords=res; self.render_pending(); self.render_users(); self.render_passwords()
        self.bg(work,done)
    def tree(self,parent,cols,heads):
        t=ttk.Treeview(parent,columns=cols,show="headings",selectmode="browse")
        for c,h in zip(cols,heads): t.heading(c,text=h); t.column(c,width=140)
        t.pack(fill="both",expand=True); return t
    def render_pending(self):
        for w in self.tab_pending.winfo_children(): w.destroy()
        bar=ttk.Frame(self.tab_pending); bar.pack(fill="x",pady=(0,10)); ttk.Label(bar,text=f"Нові заявки: {len(self.pending)}",font=("Segoe UI",18,"bold"),background=BG,foreground="white").pack(side="left")
        tree=self.tree(self.tab_pending,["id","surname","created"],["ID","Прізвище","Дата заявки"])
        for u in sorted(self.pending,key=lambda x:parse_dt(x.get("created_at")),reverse=True): tree.insert("","end",iid=str(u.get("id")),values=(u.get("id"),u.get("surname"),fmt_dt(u.get("created_at"))))
        actions=ttk.Frame(self.tab_pending); actions.pack(fill="x",pady=10); role=tk.StringVar(value="viewer"); ttk.Combobox(actions,textvariable=role,values=list(ROLES),state="readonly",width=14).pack(side="left",padx=6)
        def selected_id():
            sel=tree.selection(); return int(sel[0]) if sel else None
        ttk.Button(actions,text="Підтвердити з роллю",style="Big.TButton",command=lambda:self.pending_action(selected_id(),"approve",role.get())).pack(side="left",padx=6); ttk.Button(actions,text="Відхилити",command=lambda:self.pending_action(selected_id(),"reject",role.get())).pack(side="left",padx=6)
    def pending_action(self, rid, action, role):
        if not rid: messagebox.showinfo(APP_NAME,"Оберіть заявку в таблиці"); return
        if action=="reject" and not messagebox.askyesno(APP_NAME,"Відхилити заявку?"): return
        self.bg(lambda: self.api.approve(rid,role) if action=="approve" else self.api.reject(rid), lambda _r,e: (messagebox.showerror(APP_NAME,str(e)) if e else (messagebox.showinfo(APP_NAME,"Готово"), self.load_data())))
    def render_users(self):
        for w in self.tab_users.winfo_children(): w.destroy()
        bar=ttk.Frame(self.tab_users); bar.pack(fill="x",pady=(0,10)); ttk.Label(bar,text=f"Користувачі: {len(self.users)}",font=("Segoe UI",18,"bold"),background=BG,foreground="white").pack(side="left"); ttk.Label(bar,text="Пошук:",background=BG,foreground="white").pack(side="left",padx=(30,6)); ent=ttk.Entry(bar,textvariable=self.filter_var,font=("Segoe UI",12)); ent.pack(side="left",ipady=4); ent.bind("<KeyRelease>",lambda e:self.render_users())
        tree=self.tree(self.tab_users,["id","surname","role","active","created","updated"],["ID","Прізвище","Роль","Статус","Створено","Оновлено"])
        needle=self.filter_var.get().lower().strip()
        for u in sorted(self.users,key=lambda x:str(x.get("surname",""))):
            if needle and needle not in str(u.get("surname","")).lower() and needle not in str(u.get("role","")).lower(): continue
            tree.insert("","end",iid=str(u.get("id")),values=(u.get("id"),u.get("surname"),u.get("role"),"Активний" if u.get("is_active") else "Заблокований",fmt_dt(u.get("created_at")),fmt_dt(u.get("updated_at"))))
        actions=ttk.Frame(self.tab_users); actions.pack(fill="x",pady=10); role=tk.StringVar(value="operator"); ttk.Combobox(actions,textvariable=role,values=list(ROLES),state="readonly",width=14).pack(side="left",padx=6)
        sid=lambda: int(tree.selection()[0]) if tree.selection() else None
        ttk.Button(actions,text="Змінити роль",command=lambda:self.user_update(sid(),role=role.get())).pack(side="left",padx=5); ttk.Button(actions,text="Активувати",command=lambda:self.user_update(sid(),is_active=True)).pack(side="left",padx=5); ttk.Button(actions,text="Заблокувати",command=lambda:self.user_update(sid(),is_active=False)).pack(side="left",padx=5); ttk.Button(actions,text="Видалити",command=lambda:self.delete_user(sid())).pack(side="left",padx=5)
    def user_update(self, uid, **payload):
        if not uid: messagebox.showinfo(APP_NAME,"Оберіть користувача"); return
        self.bg(lambda:self.api.update_user(uid,**payload), lambda _r,e: (messagebox.showerror(APP_NAME,str(e)) if e else self.load_data()))
    def delete_user(self, uid):
        if not uid: messagebox.showinfo(APP_NAME,"Оберіть користувача"); return
        if not messagebox.askyesno(APP_NAME,"Повністю видалити користувача?"): return
        self.bg(lambda:self.api.delete_user(uid), lambda _r,e: (messagebox.showerror(APP_NAME,str(e)) if e else self.load_data()))
    def render_passwords(self):
        for w in self.tab_pass.winfo_children(): w.destroy()
        ttk.Label(self.tab_pass,text="Системні API-паролі ролей",font=("Segoe UI",18,"bold"),background=BG,foreground="white").pack(anchor="w",pady=(0,15))
        for role,(label,desc) in ROLES.items():
            row=ttk.Frame(self.tab_pass,style="Card.TFrame",padding=16); row.pack(fill="x",pady=6); ttk.Label(row,text=f"{label} — {desc}",style="Card.TLabel",font=("Segoe UI",13,"bold")).pack(side="left"); ttk.Label(row,text="Поточний пароль: "+("•••••••" if self.passwords.get(role) else "не вказано"),style="Card.TLabel",foreground=MUTED).pack(side="left",padx=20); ttk.Button(row,text="Змінити",command=lambda r=role:self.edit_password(r)).pack(side="right")
    def edit_password(self, role):
        win=tk.Toplevel(self); win.title("Пароль ролі"); win.geometry("430x170"); win.configure(bg=CARD); ttk.Label(win,text=f"Новий пароль для ролі {role}",style="Card.TLabel",font=("Segoe UI",13,"bold")).pack(padx=20,pady=(20,8),anchor="w"); ent=ttk.Entry(win,show="●",font=("Segoe UI",14)); ent.pack(fill="x",padx=20,ipady=5)
        ttk.Button(win,text="Зберегти",style="Big.TButton",command=lambda:self.bg(lambda:self.api.update_role_password(role,ent.get().strip()), lambda _r,e:(messagebox.showerror(APP_NAME,str(e)) if e else (win.destroy(), self.load_data())))).pack(pady=15)
    def logout(self): self.api.token=""; CONFIG_PATH.unlink(missing_ok=True); self.show_login()

if __name__ == "__main__": AdminApp().mainloop()
