import json
import os
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
import winsound
from platformdirs import user_data_dir
import tkinter as tk
from tkinter import messagebox, ttk

BASE_URL = "http://173.242.53.38:10000"
APP_NAME = "TrackingApp"


    
# ═══════════════════════════════════════════════════════════════════════════════
# ЦВЕТОВАЯ СХЕМА И КОНСТАНТЫ ДИЗАЙНА
# ═══════════════════════════════════════════════════════════════════════════════

class Colors:
    PRIMARY = "#6366F1"
    PRIMARY_HOVER = "#4F46E5"
    PRIMARY_LIGHT = "#EEF2FF"
    SECONDARY = "#8B5CF6"
    SECONDARY_HOVER = "#7C3AED"
    SUCCESS = "#10B981"
    SUCCESS_LIGHT = "#D1FAE5"
    WARNING = "#F59E0B"
    WARNING_LIGHT = "#FEF3C7"
    ERROR = "#EF4444"
    ERROR_LIGHT = "#FEE2E2"
    INFO = "#3B82F6"
    INFO_LIGHT = "#DBEAFE"
    BG_PRIMARY = "#0F172A"
    BG_SECONDARY = "#1E293B"
    BG_TERTIARY = "#334155"
    BG_CARD = "#1E293B"
    BG_INPUT = "#0F172A"
    TEXT_PRIMARY = "#F8FAFC"
    TEXT_SECONDARY = "#94A3B8"
    TEXT_MUTED = "#64748B"
    BORDER = "#334155"
    BORDER_LIGHT = "#475569"


class Fonts:
    FAMILY = "Segoe UI"
    TITLE_SIZE = 28
    HEADER_SIZE = 18
    SUBHEADER_SIZE = 14
    BODY_SIZE = 11
    SMALL_SIZE = 10
    BUTTON_SIZE = 11


class Spacing:
    XS = 4
    SM = 8
    MD = 16
    LG = 24
    XL = 32
    XXL = 48


# ═══════════════════════════════════════════════════════════════════════════════
# БАЗОВЫЕ КЛАССЫ
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ApiError(Exception):
    message: str
    status_code: int


class LocalStore:
    def __init__(self) -> None:
        self.base_dir = user_data_dir(APP_NAME, "Tracking")
        os.makedirs(self.base_dir, exist_ok=True)
        self.state_path = os.path.join(self.base_dir, "state.json")
        self.tracking_offline_path = os.path.join(self.base_dir, "offline_records.json")

    def load_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.state_path):
            return {}
        try:
            with open(self.state_path, "r", encoding="utf-8") as handle:
                return json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {}

    def save_state(self, data: Dict[str, Any]) -> None:
        try:
            with open(self.state_path, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, indent=2)
        except OSError:
            messagebox.showwarning("Помилка", "Не вдалося зберегти локальні налаштування.")

    def load_offline_records(self, path: str) -> List[Dict[str, Any]]:
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, list):
                return [record for record in data if isinstance(record, dict)]
        except (OSError, json.JSONDecodeError):
            pass
        return []

    def save_offline_records(self, path: str, records: List[Dict[str, Any]]) -> None:
        try:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(records, handle, ensure_ascii=False, indent=2)
        except OSError:
            messagebox.showwarning("Помилка", "Не вдалося зберегти офлайн-чергу.")


class ApiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        token: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = requests.request(method, url, headers=headers, json=payload, timeout=12)
        return response

    @staticmethod
    def _extract_message(response: requests.Response) -> str:
        try:
            body = response.json()
            if isinstance(body, dict):
                detail = body.get("detail") or body.get("message")
                if isinstance(detail, str) and detail:
                    return detail
        except ValueError:
            pass
        return f"Помилка сервера ({response.status_code})"

    def request_json(
        self,
        method: str,
        path: str,
        token: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Any:
        response = self._request(method, path, token=token, payload=payload)
        if response.status_code != 200:
            raise ApiError(self._extract_message(response), response.status_code)
        if response.text:
            try:
                return response.json()
            except ValueError:
                raise ApiError("Некоректна відповідь сервера", response.status_code)
        return None


class OfflineQueue:
    def __init__(self, store: LocalStore, path: str) -> None:
        self.store = store
        self.path = path

    def add(self, record: Dict[str, Any]) -> None:
        records = self.store.load_offline_records(self.path)
        records.append(record)
        self.store.save_offline_records(self.path, records)

    def contains(self, key: str, value: str) -> bool:
        records = self.store.load_offline_records(self.path)
        return any(str(item.get(key, "")).strip() == value for item in records)

    def list(self) -> List[Dict[str, Any]]:
        return self.store.load_offline_records(self.path)

    def clear(self) -> None:
        self.store.save_offline_records(self.path, [])


# ═══════════════════════════════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ═══════════════════════════════════════════════════════════════════════════════

def format_datetime(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone().strftime("%d.%m.%Y %H:%M:%S")
    except ValueError:
        return value


def run_async(
    root: tk.Misc,
    func: Callable[[], Any],
    on_success: Callable[[Any], None],
    on_error: Callable[[Exception], None],
) -> None:
    def worker() -> None:
        try:
            result = func()
            root.after(0, lambda: on_success(result))
        except Exception as exc:
            root.after(0, lambda: on_error(exc))

    threading.Thread(target=worker, daemon=True).start()


# ═══════════════════════════════════════════════════════════════════════════════
# КАСТОМНЫЕ ВИДЖЕТЫ
# ═══════════════════════════════════════════════════════════════════════════════

class ModernButton(tk.Canvas):
    def __init__(
        self,
        parent,
        text: str,
        command: Callable = None,
        variant: str = "primary",
        width: int = 140,
        height: int = 40,
        **kwargs
    ):
        super().__init__(parent, width=width, height=height, highlightthickness=0, **kwargs)
        
        self.command = command
        self.text = text
        self.variant = variant
        self.width = width
        self.height = height
        self.is_hovered = False
        self.is_pressed = False
        
        self.colors = {
            "primary": {"bg": Colors.PRIMARY, "hover": Colors.PRIMARY_HOVER, "text": "#FFFFFF", "pressed": "#4338CA"},
            "secondary": {"bg": Colors.BG_TERTIARY, "hover": Colors.BORDER_LIGHT, "text": Colors.TEXT_PRIMARY, "pressed": Colors.BG_SECONDARY},
            "success": {"bg": Colors.SUCCESS, "hover": "#059669", "text": "#FFFFFF", "pressed": "#047857"},
            "danger": {"bg": Colors.ERROR, "hover": "#DC2626", "text": "#FFFFFF", "pressed": "#B91C1C"},
            "ghost": {"bg": Colors.BG_PRIMARY, "hover": Colors.BG_TERTIARY, "text": Colors.TEXT_SECONDARY, "pressed": Colors.BG_SECONDARY}
        }
        
        try:
            self.configure(bg=parent.cget("bg"))
        except:
            self.configure(bg=Colors.BG_PRIMARY)
        
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<Button-1>", self._on_press)
        self.bind("<ButtonRelease-1>", self._on_release)
        
        self._draw()
    
    def _get_current_bg(self) -> str:
        colors = self.colors.get(self.variant, self.colors["primary"])
        if self.is_pressed:
            return colors["pressed"]
        elif self.is_hovered:
            return colors["hover"]
        return colors["bg"]
    
    def _draw(self):
        self.delete("all")
        colors = self.colors.get(self.variant, self.colors["primary"])
        bg_color = self._get_current_bg()
        
        radius = 8
        self._create_rounded_rect(2, 2, self.width - 2, self.height - 2, radius, bg_color)
        
        self.create_text(
            self.width // 2,
            self.height // 2,
            text=self.text,
            fill=colors["text"],
            font=(Fonts.FAMILY, Fonts.BUTTON_SIZE, "bold")
        )
    
    def _create_rounded_rect(self, x1, y1, x2, y2, radius, color):
        points = [
            x1 + radius, y1, x2 - radius, y1, x2, y1, x2, y1 + radius,
            x2, y2 - radius, x2, y2, x2 - radius, y2, x1 + radius, y2,
            x1, y2, x1, y2 - radius, x1, y1 + radius, x1, y1,
        ]
        self.create_polygon(points, fill=color, smooth=True)
    
    def _on_enter(self, event):
        self.is_hovered = True
        self._draw()
    
    def _on_leave(self, event):
        self.is_hovered = False
        self.is_pressed = False
        self._draw()
    
    def _on_press(self, event):
        self.is_pressed = True
        self._draw()
    
    def _on_release(self, event):
        self.is_pressed = False
        self._draw()
        if self.is_hovered and self.command:
            self.command()


class ModernEntry(tk.Frame):
    def __init__(
        self,
        parent,
        label: str = "",
        placeholder: str = "",
        show: str = "",
        icon: str = "",
        label_size: Optional[int] = None,
        entry_size: Optional[int] = None,
        entry_padding: Optional[int] = None,
        **kwargs,
    ):
        super().__init__(parent, bg=Colors.BG_CARD)
        
        self.placeholder = placeholder
        self.show_char = show
        label_font_size = label_size if label_size is not None else Fonts.SMALL_SIZE
        entry_font_size = entry_size if entry_size is not None else Fonts.BODY_SIZE
        entry_pad = entry_padding if entry_padding is not None else Spacing.SM
        
        if label:
            self.label = tk.Label(self, text=label, font=(Fonts.FAMILY, label_font_size), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD)
            self.label.pack(anchor="w", pady=(0, Spacing.XS))
        
        self.entry_frame = tk.Frame(self, bg=Colors.BG_INPUT, highlightbackground=Colors.BORDER, highlightthickness=1, highlightcolor=Colors.PRIMARY)
        self.entry_frame.pack(fill=tk.X)
        
        if icon:
            self.icon_label = tk.Label(self.entry_frame, text=icon, font=(Fonts.FAMILY, entry_font_size), fg=Colors.TEXT_MUTED, bg=Colors.BG_INPUT, padx=Spacing.SM)
            self.icon_label.pack(side=tk.LEFT)
        
        self.entry = tk.Entry(self.entry_frame, font=(Fonts.FAMILY, entry_font_size), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_INPUT, insertbackground=Colors.PRIMARY, relief="flat", show=show)
        self.entry.pack(fill=tk.X, padx=Spacing.SM, pady=entry_pad, expand=True)
        
        self.entry.bind("<FocusIn>", self._highlight_border)
        self.entry.bind("<FocusOut>", self._unhighlight_border)
    
    def _highlight_border(self, event):
        self.entry_frame.config(highlightbackground=Colors.PRIMARY)
    
    def _unhighlight_border(self, event):
        self.entry_frame.config(highlightbackground=Colors.BORDER)
    
    def get(self) -> str:
        return self.entry.get()
    
    def set(self, value: str):
        self.entry.delete(0, tk.END)
        if value:
            self.entry.insert(0, value)
    
    def bind(self, sequence, func, add=None):
        self.entry.bind(sequence, func, add)
    
    def focus(self):
        self.entry.focus()


class ModernCard(tk.Frame):
    def __init__(self, parent, title: str = "", padding: int = Spacing.LG, **kwargs):
        super().__init__(parent, bg=Colors.BG_CARD, **kwargs)
        
        self.inner_frame = tk.Frame(self, bg=Colors.BG_CARD)
        self.inner_frame.pack(fill=tk.BOTH, expand=True, padx=padding, pady=padding)
        
        if title:
            self.title_label = tk.Label(self.inner_frame, text=title, font=(Fonts.FAMILY, Fonts.HEADER_SIZE, "bold"), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_CARD)
            self.title_label.pack(anchor="w", pady=(0, Spacing.MD))
        
        self.content = tk.Frame(self.inner_frame, bg=Colors.BG_CARD)
        self.content.pack(fill=tk.BOTH, expand=True)


class ModernNotebook(tk.Frame):
    def __init__(self, parent, **kwargs):
        super().__init__(parent, bg=Colors.BG_PRIMARY, **kwargs)
        
        self.tabs: Dict[str, tk.Frame] = {}
        self.tab_buttons: Dict[str, tk.Label] = {}
        self.current_tab: Optional[str] = None
        
        self.tab_bar = tk.Frame(self, bg=Colors.BG_SECONDARY)
        self.tab_bar.pack(fill=tk.X)
        
        self.content_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        self.content_frame.pack(fill=tk.BOTH, expand=True)
    
    def add_tab(self, name: str, title: str, frame: tk.Frame):
        tab_btn = tk.Label(self.tab_bar, text=title, font=(Fonts.FAMILY, Fonts.BODY_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY, padx=Spacing.LG, pady=Spacing.SM, cursor="hand2")
        tab_btn.pack(side=tk.LEFT)
        tab_btn.bind("<Button-1>", lambda e, n=name: self.select_tab(n))
        
        self.tab_buttons[name] = tab_btn
        self.tabs[name] = frame
        
        frame.place(in_=self.content_frame, x=0, y=0, relwidth=1, relheight=1)
        frame.lower()
        
        if len(self.tabs) == 1:
            self.select_tab(name)
    
    def select_tab(self, name: str):
        if name not in self.tabs:
            return
        
        for tab_name, btn in self.tab_buttons.items():
            if tab_name == name:
                btn.config(fg=Colors.PRIMARY, bg=Colors.BG_PRIMARY)
            else:
                btn.config(fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY)
        
        for tab_name, frame in self.tabs.items():
            if tab_name == name:
                frame.lift()
            else:
                frame.lower()
        
        self.current_tab = name
        
        if hasattr(self.tabs[name], "refresh"):
            self.tabs[name].refresh()


class ModernTreeview(tk.Frame):
    def __init__(self, parent, columns: List[Tuple[str, str, int]], **kwargs):
        super().__init__(parent, bg=Colors.BG_CARD, **kwargs)
        
        style = ttk.Style()
        style.configure("Modern.Treeview", background=Colors.BG_CARD, foreground=Colors.TEXT_PRIMARY, fieldbackground=Colors.BG_CARD, rowheight=36, font=(Fonts.FAMILY, Fonts.BODY_SIZE))
        style.configure("Modern.Treeview.Heading", background=Colors.BG_SECONDARY, foreground=Colors.TEXT_SECONDARY, font=(Fonts.FAMILY, Fonts.SMALL_SIZE, "bold"), relief="flat")
        style.map("Modern.Treeview", background=[("selected", Colors.PRIMARY)], foreground=[("selected", "#FFFFFF")])
        
        self.tree = ttk.Treeview(self, columns=[col[0] for col in columns], show="headings", style="Modern.Treeview")
        
        for col_id, col_name, col_width in columns:
            self.tree.heading(col_id, text=col_name)
            self.tree.column(col_id, width=col_width, anchor="center")
        
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(fill=tk.BOTH, expand=True)
    
    def insert(self, values: tuple):
        self.tree.insert("", tk.END, values=values)
    
    def clear(self):
        self.tree.delete(*self.tree.get_children())
    
    def selection(self):
        return self.tree.selection()
    
    def item(self, item):
        return self.tree.item(item)


# ═══════════════════════════════════════════════════════════════════════════════
# СТАРТОВЫЙ ЭКРАН
# ═══════════════════════════════════════════════════════════════════════════════

class StartFrame(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp") -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        
        center_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center_frame.place(relx=0.5, rely=0.5, anchor="center")
        
        logo_frame = tk.Frame(center_frame, bg=Colors.BG_PRIMARY)
        logo_frame.pack(pady=(0, Spacing.XL))
        
        tk.Label(logo_frame, text="📦", font=(Fonts.FAMILY, 48), bg=Colors.BG_PRIMARY).pack()
        tk.Label(logo_frame, text="TrackingApp", font=(Fonts.FAMILY, Fonts.TITLE_SIZE, "bold"), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_PRIMARY).pack(pady=(Spacing.SM, 0))
        tk.Label(logo_frame, text="Система відстеження та управління", font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_PRIMARY).pack(pady=(Spacing.XS, 0))
        
        card = ModernCard(center_frame, padding=Spacing.XL)
        card.pack(fill=tk.X, padx=Spacing.XXL)
        
        tk.Label(card.content, text="Оберіть модуль для роботи", font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(pady=(0, Spacing.LG))
        
        ModernButton(card.content, text="🚚  TrackingApp", command=lambda: app.show_frame("TrackingLoginFrame"), variant="primary", width=280, height=50).pack(pady=Spacing.SM)
        
        tk.Label(center_frame, text="v2.0.0", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_MUTED, bg=Colors.BG_PRIMARY).pack(pady=(Spacing.LG, 0))


# ═══════════════════════════════════════════════════════════════════════════════
# ЭКРАН ВХОДА TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class TrackingLoginFrame(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp") -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.message = tk.StringVar(value="")
        self.current_tab = "login"
        
        center_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center_frame.place(relx=0.5, rely=0.5, anchor="center")
        
        header = tk.Frame(center_frame, bg=Colors.BG_PRIMARY)
        header.pack(pady=(0, Spacing.LG))
        
        ModernButton(header, text="← Назад", command=lambda: app.show_frame("StartFrame"), variant="ghost", width=100, height=36).pack(side=tk.LEFT)
        tk.Label(header, text="TrackingApp", font=(Fonts.FAMILY, Fonts.HEADER_SIZE, "bold"), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_PRIMARY).pack(side=tk.LEFT, padx=Spacing.LG)
        
        card = ModernCard(center_frame, padding=Spacing.XL)
        card.pack()
        
        tab_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        tab_frame.pack(fill=tk.X, pady=(0, Spacing.LG))
        
        self.login_tab_btn = tk.Label(tab_frame, text="Вхід", font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"), fg=Colors.PRIMARY, bg=Colors.BG_CARD, padx=Spacing.LG, pady=Spacing.SM, cursor="hand2")
        self.login_tab_btn.pack(side=tk.LEFT)
        self.login_tab_btn.bind("<Button-1>", lambda e: self._switch_tab("login"))
        
        self.register_tab_btn = tk.Label(tab_frame, text="Реєстрація", font=(Fonts.FAMILY, Fonts.BODY_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD, padx=Spacing.LG, pady=Spacing.SM, cursor="hand2")
        self.register_tab_btn.pack(side=tk.LEFT)
        self.register_tab_btn.bind("<Button-1>", lambda e: self._switch_tab("register"))
        
        self.form_container = tk.Frame(card.content, bg=Colors.BG_CARD)
        self.form_container.pack(fill=tk.BOTH, expand=True)
        
        self.login_form = tk.Frame(self.form_container, bg=Colors.BG_CARD)
        self._build_login_form()
        
        self.register_form = tk.Frame(self.form_container, bg=Colors.BG_CARD)
        self._build_register_form()
        
        self.login_form.pack(fill=tk.BOTH, expand=True)
        
        self.message_label = tk.Label(card.content, textvariable=self.message, font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.ERROR, bg=Colors.BG_CARD, wraplength=300)
        self.message_label.pack(pady=(Spacing.MD, 0))
        
        ModernButton(card.content, text="🔐 Адмін панель", command=self.open_admin_panel, variant="ghost", width=160, height=36).pack(pady=(Spacing.LG, 0))
    
    def _switch_tab(self, tab: str):
        self.current_tab = tab
        self.message.set("")
        
        if tab == "login":
            self.login_tab_btn.config(fg=Colors.PRIMARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"))
            self.register_tab_btn.config(fg=Colors.TEXT_SECONDARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE))
            self.register_form.pack_forget()
            self.login_form.pack(fill=tk.BOTH, expand=True)
        else:
            self.register_tab_btn.config(fg=Colors.PRIMARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"))
            self.login_tab_btn.config(fg=Colors.TEXT_SECONDARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE))
            self.login_form.pack_forget()
            self.register_form.pack(fill=tk.BOTH, expand=True)
    
    def _build_login_form(self):
        self.login_surname_entry = ModernEntry(self.login_form, label="Прізвище", icon="👤")
        self.login_surname_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.login_password_entry = ModernEntry(self.login_form, label="Пароль", icon="🔒", show="•")
        self.login_password_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        ModernButton(self.login_form, text="Увійти", command=self.handle_login, variant="primary", width=320, height=44).pack(pady=(Spacing.LG, 0))
        
        self.login_password_entry.bind("<Return>", lambda e: self.handle_login())
    
    def _build_register_form(self):
        self.register_surname_entry = ModernEntry(self.register_form, label="Прізвище", icon="👤")
        self.register_surname_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.register_password_entry = ModernEntry(self.register_form, label="Пароль", icon="🔒", show="•")
        self.register_password_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.register_confirm_entry = ModernEntry(self.register_form, label="Підтвердження пароля", icon="🔒", show="•")
        self.register_confirm_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        ModernButton(self.register_form, text="Надіслати заявку", command=self.handle_register, variant="primary", width=320, height=44).pack(pady=(Spacing.LG, 0))
    
    def handle_login(self) -> None:
        surname = self.login_surname_entry.get().strip()
        password = self.login_password_entry.get().strip()
        
        if not surname or not password:
            self.message.set("Введіть прізвище та пароль")
            return
        
        self.message.set("")
        
        def task() -> Dict[str, Any]:
            return self.app.api.request_json("POST", "/login", payload={"surname": surname, "password": password})
        
        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                self.message.set("Сервер не повернув токен")
                return
            self.app.update_state({"token": token, "access_level": data.get("access_level"), "user_name": data.get("surname", surname), "user_role": data.get("role")})
            self.app.show_frame("TrackingMainFrame")
        
        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                self.message.set(exc.message)
            else:
                self.message.set("Не вдалося зʼєднатися з сервером")
        
        run_async(self, task, on_success, on_error)
    
    def handle_register(self) -> None:
        surname = self.register_surname_entry.get().strip()
        password = self.register_password_entry.get().strip()
        confirm = self.register_confirm_entry.get().strip()
        
        if not surname or not password or not confirm:
            self.message.set("Заповніть усі поля")
            return
        if len(password) < 6:
            self.message.set("Пароль має містити щонайменше 6 символів")
            return
        if password != confirm:
            self.message.set("Паролі не співпадають")
            return
        
        def task() -> Any:
            return self.app.api.request_json("POST", "/register", payload={"surname": surname, "password": password})
        
        def on_success(_: Any) -> None:
            self.message_label.config(fg=Colors.SUCCESS)
            self.message.set("Заявку на реєстрацію відправлено.")
            self.register_surname_entry.set("")
            self.register_password_entry.set("")
            self.register_confirm_entry.set("")
        
        def on_error(exc: Exception) -> None:
            self.message_label.config(fg=Colors.ERROR)
            if isinstance(exc, ApiError):
                self.message.set(exc.message)
            else:
                self.message.set("Не вдалося відправити заявку")
        
        run_async(self, task, on_success, on_error)
    
    def open_admin_panel(self) -> None:
        password = simple_prompt(self, "Пароль адміністратора")
        if not password:
            return
        
        def task() -> Dict[str, Any]:
            return self.app.api.request_json("POST", "/admin_login", payload={"password": password})
        
        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                messagebox.showerror("Помилка", "Сервер не повернув токен")
                return
            AdminPanel(self, self.app, token)
        
        def on_error(exc: Exception) -> None:
            if isinstance(exc, ApiError):
                messagebox.showerror("Помилка", exc.message)
            else:
                messagebox.showerror("Помилка", "Не вдалося зʼєднатися з сервером")
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# ГЛАВНЫЙ ЭКРАН TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class TrackingMainFrame(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp") -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.status = tk.StringVar(value="")
        self.user_label = tk.StringVar(value="")
        self.role_label = tk.StringVar(value="")
        
        header = tk.Frame(self, bg=Colors.BG_SECONDARY, height=60)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        
        header_content = tk.Frame(header, bg=Colors.BG_SECONDARY)
        header_content.pack(fill=tk.BOTH, expand=True, padx=Spacing.LG)
        
        tk.Label(header_content, text="📦 TrackingApp", font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_SECONDARY).pack(side=tk.LEFT, pady=Spacing.MD)
        
        user_frame = tk.Frame(header_content, bg=Colors.BG_SECONDARY)
        user_frame.pack(side=tk.RIGHT, pady=Spacing.SM)
        
        ModernButton(user_frame, text="🚪 Вийти", command=self.logout, variant="ghost", width=90, height=32).pack(side=tk.RIGHT)
        tk.Label(user_frame, textvariable=self.role_label, font=(Fonts.FAMILY, Fonts.SMALL_SIZE, "bold"), fg=Colors.PRIMARY, bg=Colors.BG_SECONDARY).pack(side=tk.RIGHT, padx=Spacing.SM)
        tk.Label(user_frame, textvariable=self.user_label, font=(Fonts.FAMILY, Fonts.BODY_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY).pack(side=tk.RIGHT, padx=Spacing.SM)
        
        content = tk.Frame(self, bg=Colors.BG_PRIMARY)
        content.pack(fill=tk.BOTH, expand=True, padx=Spacing.MD, pady=Spacing.MD)
        
        self.notebook = ModernNotebook(content)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        self.scan_tab = TrackingScanTab(self.notebook.content_frame, app, self.status)
        self.notebook.add_tab("scan", "📷 Сканер", self.scan_tab)
        
        status_bar = tk.Frame(self, bg=Colors.BG_SECONDARY, height=36)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
        status_bar.pack_propagate(False)
        
        tk.Label(status_bar, textvariable=self.status, font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY).pack(side=tk.LEFT, padx=Spacing.MD, pady=Spacing.XS)
    
    def refresh(self) -> None:
        user = self.app.state_data.get("user_name", "оператор")
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        
        role_text = "👁 Перегляд"
        if role == "admin" or access_level == 1:
            role_text = "🔑 Адмін"
        elif role == "operator" or access_level == 0:
            role_text = "🧰 Оператор"
        
        self.user_label.set(f"👤 {user}")
        self.role_label.set(role_text)
        
        self.scan_tab.refresh()
    
    def logout(self) -> None:
        self.app.clear_state(["token", "access_level", "user_name", "user_role"])
        self.app.show_frame("StartFrame")


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА СКАНЕРА TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class TrackingScanTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp", status: tk.StringVar) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.status = status
        self.inflight = tk.IntVar(value=0)
        self.scan_message = tk.StringVar(value="")
        
        center = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center.place(relx=0.5, rely=0.4, anchor="center")
        
        card = ModernCard(center, title="📷 Сканування", padding=Spacing.XXL + Spacing.MD)
        card.pack(fill=tk.X)
        
        self.box_entry = ModernEntry(
            card.content,
            label="BoxID",
            icon="📦",
            label_size=Fonts.HEADER_SIZE,
            entry_size=28,
            entry_padding=Spacing.LG,
        )
        self.box_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.ttn_entry = ModernEntry(
            card.content,
            label="ТТН",
            icon="🏷️",
            label_size=Fonts.HEADER_SIZE,
            entry_size=28,
            entry_padding=Spacing.LG,
        )
        self.ttn_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        btn_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        btn_frame.pack(fill=tk.X, pady=(Spacing.LG, 0))
        
        ModernButton(btn_frame, text="📤 Надіслати", command=self.send_record, variant="primary", width=240, height=70).pack(side=tk.LEFT)
        ModernButton(btn_frame, text="🔄 Синхронізувати", command=self.sync_offline, variant="secondary", width=260, height=70).pack(side=tk.LEFT, padx=Spacing.SM)

        self.scan_message_label = tk.Label(
            card.content,
            textvariable=self.scan_message,
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_CARD,
        )
        self.scan_message_label.pack(fill=tk.X, pady=(Spacing.MD, 0))
        
        offline_frame = tk.Frame(card.content, bg=Colors.BG_TERTIARY)
        offline_frame.pack(fill=tk.X, pady=(Spacing.LG, 0))
        
        offline_inner = tk.Frame(offline_frame, bg=Colors.BG_TERTIARY)
        offline_inner.pack(padx=Spacing.MD, pady=Spacing.SM)
        
        tk.Label(offline_inner, text="📴 В черзі офлайн:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_TERTIARY).pack(side=tk.LEFT)
        tk.Label(offline_inner, textvariable=self.inflight, font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"), fg=Colors.WARNING, bg=Colors.BG_TERTIARY).pack(side=tk.LEFT, padx=Spacing.XS)
        
        self.box_entry.bind("<Return>", lambda e: self.ttn_entry.focus())
        self.ttn_entry.bind("<Return>", lambda e: self.send_record())

    def _play_tone(self, tone: str) -> None:
        if winsound:
            tones = {
                "success": (880, 140),
                "negative": (220, 220),
                "both": (440, 200),
                "warning": (330, 160),
            }
            frequency, duration = tones.get(tone, (330, 160))
            winsound.Beep(frequency, duration)
        else:
            self.bell()

    def _set_scan_message(self, message: str, tone: str) -> None:
        colors = {
            "success": Colors.SUCCESS,
            "negative": Colors.ERROR,
            "both": Colors.WARNING,
            "warning": Colors.WARNING,
            "info": Colors.TEXT_SECONDARY,
        }
        self.scan_message.set(message)
        self.scan_message_label.config(fg=colors.get(tone, Colors.TEXT_SECONDARY))
        self.status.set(message)
        self._play_tone(tone)

    def _describe_duplicate(self, note: str) -> Tuple[str, str]:
        note_lower = note.lower()
        has_boxid = "box" in note_lower
        has_ttn = "ттн" in note_lower or "ttn" in note_lower
        if has_boxid and has_ttn:
            return ("Такий BoxID та ТТН уже було внесено", "both")
        if has_boxid:
            return ("Такий BoxID вже було внесено", "negative")
        if has_ttn:
            return ("Такий ТТН вже було внесено", "negative")
        return ("Такий запис вже було внесено", "negative")
    
    def refresh(self) -> None:
        self.inflight.set(len(self.app.tracking_offline.list()))
    
    def send_record(self) -> None:
        token = self.app.state_data.get("token")
        user_name = self.app.state_data.get("user_name", "operator")
        boxid = "".join(filter(str.isdigit, self.box_entry.get()))
        ttn = "".join(filter(str.isdigit, self.ttn_entry.get()))
        
        if not boxid or not ttn:
            self._set_scan_message("Заповніть BoxID та ТТН", "warning")
            return
        
        record = {"user_name": user_name, "boxid": boxid, "ttn": ttn}
        self.box_entry.set("")
        self.ttn_entry.set("")
        self.box_entry.focus()
        
        def task() -> Dict[str, Any]:
            if not token:
                raise ApiError("Відсутній токен", 401)
            return self.app.api.request_json("POST", "/add_record", token=token, payload=record)
        
        def on_success(data: Dict[str, Any]) -> None:
            note = data.get("note") if isinstance(data, dict) else None
            if note:
                message, tone = self._describe_duplicate(str(note))
                self._set_scan_message(message, tone)
            else:
                self._set_scan_message("BoxID та ТТН внесені", "success")
            self.sync_offline()
        
        def on_error(exc: Exception) -> None:
            self.app.tracking_offline.add(record)
            self.inflight.set(len(self.app.tracking_offline.list()))
            self.scan_message.set("Збережено локально (офлайн)")
            self.scan_message_label.config(fg=Colors.INFO)
            self.status.set("📦 Збережено локально (офлайн)")
        
        run_async(self, task, on_success, on_error)
    
    def sync_offline(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return
        
        pending = self.app.tracking_offline.list()
        if not pending:
            self.status.set("✅ Офлайн-черга порожня")
            self.refresh()
            return
        
        def task() -> int:
            synced = 0
            for record in pending:
                try:
                    self.app.api.request_json("POST", "/add_record", token=token, payload=record)
                    synced += 1
                except ApiError:
                    break
            return synced
        
        def on_success(count: int) -> None:
            if count:
                self.app.tracking_offline.clear()
                self.status.set(f"✅ Синхронізовано {count} записів")
            self.refresh()
        
        def on_error(_: Exception) -> None:
            self.status.set("❌ Не вдалося синхронізувати")
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# АДМИН ПАНЕЛИ
# ═══════════════════════════════════════════════════════════════════════════════

class AdminPanel(tk.Toplevel):
    def __init__(self, parent: tk.Misc, app: "TrackingApp", token: str) -> None:
        super().__init__(parent)
        self.app = app
        self.token = token
        self.title("🔐 Адмін панель TrackingApp")
        self.geometry("1000x700")
        self.configure(bg=Colors.BG_PRIMARY)
        
        header = tk.Frame(self, bg=Colors.BG_SECONDARY, height=50)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        
        tk.Label(header, text="🔐 Адмін панель TrackingApp", font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_SECONDARY).pack(side=tk.LEFT, padx=Spacing.LG, pady=Spacing.SM)
        ModernButton(header, text="✕ Закрити", command=self.destroy, variant="ghost", width=100, height=32).pack(side=tk.RIGHT, padx=Spacing.MD, pady=Spacing.SM)
        
        content = tk.Frame(self, bg=Colors.BG_PRIMARY)
        content.pack(fill=tk.BOTH, expand=True, padx=Spacing.MD, pady=Spacing.MD)
        
        self.notebook = ModernNotebook(content)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        self.pending_tab = AdminPendingTab(self.notebook.content_frame, app, token)
        self.users_tab = AdminUsersTab(self.notebook.content_frame, app, token)
        self.password_tab = AdminPasswordsTab(self.notebook.content_frame, app, token)
        
        self.notebook.add_tab("pending", "📝 Запити", self.pending_tab)
        self.notebook.add_tab("users", "👥 Користувачі", self.users_tab)
        self.notebook.add_tab("passwords", "🔑 Паролі", self.password_tab)


class AdminPendingTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp", token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(actions, text="🔄 Оновити", command=self.fetch_requests, variant="primary", width=120, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="✅ Підтвердити", command=self.approve_request, variant="success", width=130, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="❌ Відхилити", command=self.reject_request, variant="danger", width=120, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        
        tk.Label(actions, text="Роль:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT, padx=(Spacing.LG, Spacing.XS))
        self.role_var = tk.StringVar(value="operator")
        ttk.Combobox(actions, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=12, state="readonly").pack(side=tk.LEFT)
        
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(table_frame, columns=[("id", "ID", 80), ("surname", "Прізвище", 200), ("created", "Дата", 200)])
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        self.fetch_requests()
    
    def fetch_requests(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/admin/registration_requests", token=self.token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.clear()
            for req in data:
                self.tree.insert((req.get("id"), req.get("surname", ""), format_datetime(req.get("created_at", ""))))
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def _selected_id(self) -> Optional[int]:
        selection = self.tree.selection()
        if not selection:
            return None
        return int(self.tree.item(selection[0])["values"][0])
    
    def approve_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Увага", "Оберіть запит")
            return
        
        role = self.role_var.get()
        
        def task() -> Any:
            return self.app.api.request_json("POST", f"/admin/registration_requests/{request_id}/approve", token=self.token, payload={"role": role})
        
        def on_success(_: Any) -> None:
            self.fetch_requests()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def reject_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Увага", "Оберіть запит")
            return
        
        def task() -> Any:
            return self.app.api.request_json("POST", f"/admin/registration_requests/{request_id}/reject", token=self.token)
        
        def on_success(_: Any) -> None:
            self.fetch_requests()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


class AdminUsersTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp", token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(actions, text="🔄 Оновити", command=self.fetch_users, variant="primary", width=120, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="🔄 Змінити роль", command=self.change_role, variant="secondary", width=140, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="⚡ Активність", command=self.toggle_active, variant="secondary", width=130, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        
        tk.Label(actions, text="Роль:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT, padx=(Spacing.LG, Spacing.XS))
        self.role_var = tk.StringVar(value="operator")
        ttk.Combobox(actions, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=10, state="readonly").pack(side=tk.LEFT)
        
        tk.Label(actions, text="Активний:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT, padx=(Spacing.MD, Spacing.XS))
        self.active_var = tk.StringVar(value="true")
        ttk.Combobox(actions, textvariable=self.active_var, values=["true", "false"], width=8, state="readonly").pack(side=tk.LEFT)
        
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(table_frame, columns=[("id", "ID", 60), ("surname", "Прізвище", 180), ("role", "Роль", 120), ("active", "Статус", 100)])
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        self.fetch_users()
    
    def fetch_users(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/admin/users", token=self.token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.clear()
            for user in data:
                status = "✅" if user.get("is_active", False) else "❌"
                self.tree.insert((user.get("id"), user.get("surname"), user.get("role"), status))
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def _selected_id(self) -> Optional[int]:
        selection = self.tree.selection()
        if not selection:
            return None
        return int(self.tree.item(selection[0])["values"][0])
    
    def change_role(self) -> None:
        user_id = self._selected_id()
        if user_id is None:
            messagebox.showinfo("Увага", "Оберіть користувача")
            return
        
        role = self.role_var.get()
        
        def task() -> Any:
            return self.app.api.request_json("PATCH", f"/admin/users/{user_id}", token=self.token, payload={"role": role})
        
        def on_success(_: Any) -> None:
            self.fetch_users()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def toggle_active(self) -> None:
        user_id = self._selected_id()
        if user_id is None:
            messagebox.showinfo("Увага", "Оберіть користувача")
            return
        
        is_active = self.active_var.get().lower() == "true"
        
        def task() -> Any:
            return self.app.api.request_json("PATCH", f"/admin/users/{user_id}", token=self.token, payload={"is_active": is_active})
        
        def on_success(_: Any) -> None:
            self.fetch_users()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


class AdminPasswordsTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: "TrackingApp", token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        self.passwords: Dict[str, str] = {}
        
        card = ModernCard(self, title="🔑 Паролі ролей", padding=Spacing.XL)
        card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        role_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        role_frame.pack(fill=tk.X, pady=Spacing.SM)
        
        tk.Label(role_frame, text="Роль:", font=(Fonts.FAMILY, Fonts.BODY_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT)
        self.role_var = tk.StringVar(value="operator")
        role_combo = ttk.Combobox(role_frame, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=15, state="readonly")
        role_combo.pack(side=tk.LEFT, padx=Spacing.SM)
        role_combo.bind("<<ComboboxSelected>>", lambda e: self._on_role_change())
        
        self.password_entry = ModernEntry(card.content, label="Новий пароль", icon="🔒", show="•")
        self.password_entry.pack(fill=tk.X, pady=Spacing.MD)
        
        btn_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        btn_frame.pack(fill=tk.X, pady=(Spacing.MD, 0))
        
        ModernButton(btn_frame, text="🔄 Завантажити", command=self.fetch_passwords, variant="secondary", width=140, height=44).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(btn_frame, text="💾 Зберегти", command=self.update_password, variant="primary", width=120, height=44).pack(side=tk.LEFT, padx=Spacing.XS)
        
        self.fetch_passwords()
    
    def _on_role_change(self) -> None:
        role = self.role_var.get()
        self.password_entry.set(self.passwords.get(role, ""))
    
    def fetch_passwords(self) -> None:
        def task() -> Dict[str, Any]:
            data = self.app.api.request_json("GET", "/admin/role-passwords", token=self.token)
            return data if isinstance(data, dict) else {}
        
        def on_success(data: Dict[str, Any]) -> None:
            self.passwords = {str(k): str(v) for k, v in data.items()}
            self._on_role_change()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def update_password(self) -> None:
        role = self.role_var.get()
        password = self.password_entry.get().strip()
        
        if not password:
            messagebox.showinfo("Увага", "Введіть пароль")
            return
        
        def task() -> Any:
            return self.app.api.request_json("POST", f"/admin/role-passwords/{role}", token=self.token, payload={"password": password})
        
        def on_success(_: Any) -> None:
            messagebox.showinfo("Успіх", "Пароль оновлено")
            self.fetch_passwords()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# ДИАЛОГОВОЕ ОКНО
# ═══════════════════════════════════════════════════════════════════════════════

def simple_prompt(root: tk.Misc, prompt: str) -> Optional[str]:
    dialog = tk.Toplevel(root)
    dialog.title("Введення")
    dialog.geometry("400x200")
    dialog.configure(bg=Colors.BG_PRIMARY)
    dialog.resizable(False, False)
    dialog.transient(root)
    dialog.grab_set()
    
    dialog.update_idletasks()
    x = (dialog.winfo_screenwidth() - 400) // 2
    y = (dialog.winfo_screenheight() - 200) // 2
    dialog.geometry(f"+{x}+{y}")
    
    content = tk.Frame(dialog, bg=Colors.BG_PRIMARY)
    content.pack(fill=tk.BOTH, expand=True, padx=Spacing.LG, pady=Spacing.LG)
    
    header = tk.Frame(content, bg=Colors.BG_PRIMARY)
    header.pack(fill=tk.X)
    
    tk.Label(header, text="🔐", font=(Fonts.FAMILY, 24), bg=Colors.BG_PRIMARY).pack(side=tk.LEFT)
    tk.Label(header, text=prompt, font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE), fg=Colors.TEXT_PRIMARY, bg=Colors.BG_PRIMARY).pack(side=tk.LEFT, padx=Spacing.SM)
    
    password_entry = ModernEntry(content, label="", icon="🔒", show="•")
    password_entry.pack(fill=tk.X, pady=Spacing.MD)
    password_entry.focus()
    
    result: List[Optional[str]] = [None]
    
    def submit() -> None:
        value = password_entry.get().strip()
        if value:
            result[0] = value
            dialog.destroy()
    
    def cancel() -> None:
        dialog.destroy()
    
    btn_frame = tk.Frame(content, bg=Colors.BG_PRIMARY)
    btn_frame.pack(fill=tk.X, pady=(Spacing.MD, 0))
    
    ModernButton(btn_frame, text="Скасувати", command=cancel, variant="secondary", width=100, height=40).pack(side=tk.LEFT)
    ModernButton(btn_frame, text="OK", command=submit, variant="primary", width=100, height=40).pack(side=tk.RIGHT)
    
    password_entry.bind("<Return>", lambda e: submit())
    dialog.bind("<Escape>", lambda e: cancel())
    
    dialog.wait_window()
    return result[0]


# ═══════════════════════════════════════════════════════════════════════════════
# ГЛАВНОЕ ПРИЛОЖЕНИЕ
# ═══════════════════════════════════════════════════════════════════════════════

class TrackingApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("TrackingApp")
        self.geometry("1280x800")
        self.minsize(1100, 700)
        self.configure(bg=Colors.BG_PRIMARY)
        
        self.store = LocalStore()
        self.state_data = self.store.load_state()
        self.api = ApiClient(BASE_URL)
        self.tracking_offline = OfflineQueue(self.store, self.store.tracking_offline_path)
        
        self.container = tk.Frame(self, bg=Colors.BG_PRIMARY)
        self.container.pack(fill=tk.BOTH, expand=True)
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)
        
        self.frames: Dict[str, tk.Frame] = {}
        for frame_class in (StartFrame, TrackingLoginFrame, TrackingMainFrame):
            frame = frame_class(self.container, self)
            self.frames[frame_class.__name__] = frame
            frame.grid(row=0, column=0, sticky="nsew")
        
        self.show_frame("StartFrame")
    
    def show_frame(self, name: str) -> None:
        frame = self.frames[name]
        frame.tkraise()
        if hasattr(frame, "refresh"):
            frame.refresh()
    
    def update_state(self, updates: Dict[str, Any]) -> None:
        self.state_data.update(updates)
        self.store.save_state(self.state_data)
    
    def clear_state(self, keys: List[str]) -> None:
        for key in keys:
            self.state_data.pop(key, None)
        self.store.save_state(self.state_data)


# ═══════════════════════════════════════════════════════════════════════════════
# ТОЧКА ВХОДА
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app = TrackingApp()
    app.mainloop()
