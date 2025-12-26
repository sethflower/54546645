import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
from platformdirs import user_data_dir
import tkinter as tk
from tkinter import messagebox, ttk

BASE_URL = "http://173.242.53.38:1000"
SCANPAK_BASE_PATH = "/scanpak"
APP_NAME = "TrackingApp"

# ═══════════════════════════════════════════════════════════════════════════════
# ЦВЕТОВАЯ СХЕМА И КОНСТАНТЫ ДИЗАЙНА
# ═══════════════════════════════════════════════════════════════════════════════

class Colors:
    # Основные цвета
    PRIMARY = "#6366F1"  # Indigo
    PRIMARY_HOVER = "#4F46E5"
    PRIMARY_LIGHT = "#EEF2FF"
    
    # Вторичные цвета
    SECONDARY = "#8B5CF6"  # Purple
    SECONDARY_HOVER = "#7C3AED"
    
    # Акцентные цвета
    SUCCESS = "#10B981"
    SUCCESS_LIGHT = "#D1FAE5"
    WARNING = "#F59E0B"
    WARNING_LIGHT = "#FEF3C7"
    ERROR = "#EF4444"
    ERROR_LIGHT = "#FEE2E2"
    INFO = "#3B82F6"
    INFO_LIGHT = "#DBEAFE"
    
    # Нейтральные цвета
    BG_PRIMARY = "#0F172A"  # Dark slate
    BG_SECONDARY = "#1E293B"
    BG_TERTIARY = "#334155"
    BG_CARD = "#1E293B"
    BG_INPUT = "#0F172A"
    
    # Текстовые цвета
    TEXT_PRIMARY = "#F8FAFC"
    TEXT_SECONDARY = "#94A3B8"
    TEXT_MUTED = "#64748B"
    
    # Границы
    BORDER = "#334155"
    BORDER_LIGHT = "#475569"
    
    # Градиенты (для canvas)
    GRADIENT_START = "#6366F1"
    GRADIENT_END = "#8B5CF6"


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
# БАЗОВЫЕ КЛАССЫ (БЕЗ ИЗМЕНЕНИЙ ЛОГИКИ)
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
        self.scanpak_offline_path = os.path.join(self.base_dir, "scanpak_offline_scans.json")

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

def parse_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_time(value: str) -> Optional[time]:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError:
        return None


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
    """Современная кнопка с эффектами hover"""
    
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
        
        # Цвета для разных вариантов
        self.colors = {
            "primary": {
                "bg": Colors.PRIMARY,
                "hover": Colors.PRIMARY_HOVER,
                "text": "#FFFFFF",
                "pressed": "#4338CA"
            },
            "secondary": {
                "bg": Colors.BG_TERTIARY,
                "hover": Colors.BORDER_LIGHT,
                "text": Colors.TEXT_PRIMARY,
                "pressed": Colors.BG_SECONDARY
            },
            "success": {
                "bg": Colors.SUCCESS,
                "hover": "#059669",
                "text": "#FFFFFF",
                "pressed": "#047857"
            },
            "danger": {
                "bg": Colors.ERROR,
                "hover": "#DC2626",
                "text": "#FFFFFF",
                "pressed": "#B91C1C"
            },
            "ghost": {
                "bg": "transparent",
                "hover": Colors.BG_TERTIARY,
                "text": Colors.TEXT_SECONDARY,
                "pressed": Colors.BG_SECONDARY
            }
        }
        
        self.configure(bg=parent.cget("bg") if hasattr(parent, "cget") else Colors.BG_PRIMARY)
        
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
        
        # Рисуем скругленный прямоугольник
        radius = 8
        if bg_color != "transparent":
            self._create_rounded_rect(2, 2, self.width - 2, self.height - 2, radius, bg_color)
        
        # Текст кнопки
        self.create_text(
            self.width // 2,
            self.height // 2,
            text=self.text,
            fill=colors["text"],
            font=(Fonts.FAMILY, Fonts.BUTTON_SIZE, "bold")
        )
    
    def _create_rounded_rect(self, x1, y1, x2, y2, radius, color):
        """Создает скругленный прямоугольник"""
        points = [
            x1 + radius, y1,
            x2 - radius, y1,
            x2, y1,
            x2, y1 + radius,
            x2, y2 - radius,
            x2, y2,
            x2 - radius, y2,
            x1 + radius, y2,
            x1, y2,
            x1, y2 - radius,
            x1, y1 + radius,
            x1, y1,
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
    """Современное поле ввода с label и иконкой"""
    
    def __init__(
        self,
        parent,
        label: str = "",
        placeholder: str = "",
        show: str = "",
        icon: str = "",
        **kwargs
    ):
        super().__init__(parent, bg=Colors.BG_CARD)
        
        self.placeholder = placeholder
        self.show_char = show
        self.has_content = False
        
        # Label
        if label:
            self.label = tk.Label(
                self,
                text=label,
                font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
                fg=Colors.TEXT_SECONDARY,
                bg=Colors.BG_CARD
            )
            self.label.pack(anchor="w", pady=(0, Spacing.XS))
        
        # Контейнер для поля ввода
        self.entry_frame = tk.Frame(
            self,
            bg=Colors.BG_INPUT,
            highlightbackground=Colors.BORDER,
            highlightthickness=1,
            highlightcolor=Colors.PRIMARY
        )
        self.entry_frame.pack(fill=tk.X)
        
        # Иконка (если есть)
        if icon:
            self.icon_label = tk.Label(
                self.entry_frame,
                text=icon,
                font=(Fonts.FAMILY, Fonts.BODY_SIZE),
                fg=Colors.TEXT_MUTED,
                bg=Colors.BG_INPUT,
                padx=Spacing.SM
            )
            self.icon_label.pack(side=tk.LEFT)
        
        # Поле ввода
        self.entry = tk.Entry(
            self.entry_frame,
            font=(Fonts.FAMILY, Fonts.BODY_SIZE),
            fg=Colors.TEXT_PRIMARY,
            bg=Colors.BG_INPUT,
            insertbackground=Colors.PRIMARY,
            relief="flat",
            show=show,
            **kwargs
        )
        self.entry.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM, expand=True)
        
        # Placeholder
        if placeholder and not show:
            self.entry.insert(0, placeholder)
            self.entry.config(fg=Colors.TEXT_MUTED)
            self.entry.bind("<FocusIn>", self._on_focus_in)
            self.entry.bind("<FocusOut>", self._on_focus_out)
        
        # Привязка событий для эффекта фокуса
        self.entry.bind("<FocusIn>", self._highlight_border, add="+")
        self.entry.bind("<FocusOut>", self._unhighlight_border, add="+")
    
    def _on_focus_in(self, event):
        if self.entry.get() == self.placeholder:
            self.entry.delete(0, tk.END)
            self.entry.config(fg=Colors.TEXT_PRIMARY)
    
    def _on_focus_out(self, event):
        if not self.entry.get():
            self.entry.insert(0, self.placeholder)
            self.entry.config(fg=Colors.TEXT_MUTED)
    
    def _highlight_border(self, event):
        self.entry_frame.config(highlightbackground=Colors.PRIMARY)
    
    def _unhighlight_border(self, event):
        self.entry_frame.config(highlightbackground=Colors.BORDER)
    
    def get(self) -> str:
        value = self.entry.get()
        if value == self.placeholder:
            return ""
        return value
    
    def set(self, value: str):
        self.entry.delete(0, tk.END)
        if value:
            self.entry.insert(0, value)
            self.entry.config(fg=Colors.TEXT_PRIMARY)
        elif self.placeholder and not self.show_char:
            self.entry.insert(0, self.placeholder)
            self.entry.config(fg=Colors.TEXT_MUTED)
    
    def bind(self, sequence, func, add=None):
        self.entry.bind(sequence, func, add)
    
    def focus(self):
        self.entry.focus()


class ModernCard(tk.Frame):
    """Карточка с тенью и скругленными углами"""
    
    def __init__(self, parent, title: str = "", padding: int = Spacing.LG, **kwargs):
        super().__init__(parent, bg=Colors.BG_CARD, **kwargs)
        
        self.inner_frame = tk.Frame(self, bg=Colors.BG_CARD)
        self.inner_frame.pack(fill=tk.BOTH, expand=True, padx=padding, pady=padding)
        
        if title:
            self.title_label = tk.Label(
                self.inner_frame,
                text=title,
                font=(Fonts.FAMILY, Fonts.HEADER_SIZE, "bold"),
                fg=Colors.TEXT_PRIMARY,
                bg=Colors.BG_CARD
            )
            self.title_label.pack(anchor="w", pady=(0, Spacing.MD))
        
        self.content = tk.Frame(self.inner_frame, bg=Colors.BG_CARD)
        self.content.pack(fill=tk.BOTH, expand=True)


class StatusBadge(tk.Frame):
    """Бейдж статуса"""
    
    def __init__(self, parent, text: str, variant: str = "info", **kwargs):
        super().__init__(parent, **kwargs)
        
        colors = {
            "success": (Colors.SUCCESS_LIGHT, Colors.SUCCESS),
            "warning": (Colors.WARNING_LIGHT, Colors.WARNING),
            "error": (Colors.ERROR_LIGHT, Colors.ERROR),
            "info": (Colors.INFO_LIGHT, Colors.INFO),
        }
        
        bg_color, text_color = colors.get(variant, colors["info"])
        
        self.configure(bg=bg_color)
        
        self.label = tk.Label(
            self,
            text=text,
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE, "bold"),
            fg=text_color,
            bg=bg_color,
            padx=Spacing.SM,
            pady=Spacing.XS
        )
        self.label.pack()
    
    def set_text(self, text: str):
        self.label.config(text=text)


class ModernNotebook(tk.Frame):
    """Современный notebook с кастомными вкладками"""
    
    def __init__(self, parent, **kwargs):
        super().__init__(parent, bg=Colors.BG_PRIMARY, **kwargs)
        
        self.tabs: Dict[str, tk.Frame] = {}
        self.tab_buttons: Dict[str, tk.Label] = {}
        self.current_tab: Optional[str] = None
        
        # Панель вкладок
        self.tab_bar = tk.Frame(self, bg=Colors.BG_SECONDARY)
        self.tab_bar.pack(fill=tk.X)
        
        # Контейнер для контента
        self.content_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        self.content_frame.pack(fill=tk.BOTH, expand=True)
    
    def add_tab(self, name: str, title: str, frame: tk.Frame):
        # Создаем кнопку вкладки
        tab_btn = tk.Label(
            self.tab_bar,
            text=title,
            font=(Fonts.FAMILY, Fonts.BODY_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_SECONDARY,
            padx=Spacing.LG,
            pady=Spacing.SM,
            cursor="hand2"
        )
        tab_btn.pack(side=tk.LEFT)
        tab_btn.bind("<Button-1>", lambda e, n=name: self.select_tab(n))
        tab_btn.bind("<Enter>", lambda e, b=tab_btn: self._on_tab_hover(b, True))
        tab_btn.bind("<Leave>", lambda e, b=tab_btn, n=name: self._on_tab_hover(b, False, n))
        
        self.tab_buttons[name] = tab_btn
        self.tabs[name] = frame
        
        # Размещаем фрейм
        frame.place(in_=self.content_frame, x=0, y=0, relwidth=1, relheight=1)
        frame.lower()
        
        # Выбираем первую вкладку
        if len(self.tabs) == 1:
            self.select_tab(name)
    
    def _on_tab_hover(self, button: tk.Label, entering: bool, tab_name: str = None):
        if entering:
            button.config(fg=Colors.TEXT_PRIMARY)
        else:
            if tab_name and tab_name == self.current_tab:
                button.config(fg=Colors.PRIMARY)
            else:
                button.config(fg=Colors.TEXT_SECONDARY)
    
    def select_tab(self, name: str):
        if name not in self.tabs:
            return
        
        # Обновляем стили кнопок
        for tab_name, btn in self.tab_buttons.items():
            if tab_name == name:
                btn.config(fg=Colors.PRIMARY, bg=Colors.BG_PRIMARY)
            else:
                btn.config(fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY)
        
        # Показываем нужную вкладку
        for tab_name, frame in self.tabs.items():
            if tab_name == name:
                frame.lift()
            else:
                frame.lower()
        
        self.current_tab = name
        
        # Вызываем refresh если есть
        if hasattr(self.tabs[name], "refresh"):
            self.tabs[name].refresh()


class ModernTreeview(tk.Frame):
    """Современная таблица"""
    
    def __init__(self, parent, columns: List[Tuple[str, str, int]], **kwargs):
        super().__init__(parent, bg=Colors.BG_CARD, **kwargs)
        
        # Стиль для Treeview
        style = ttk.Style()
        style.configure(
            "Modern.Treeview",
            background=Colors.BG_CARD,
            foreground=Colors.TEXT_PRIMARY,
            fieldbackground=Colors.BG_CARD,
            rowheight=36,
            font=(Fonts.FAMILY, Fonts.BODY_SIZE)
        )
        style.configure(
            "Modern.Treeview.Heading",
            background=Colors.BG_SECONDARY,
            foreground=Colors.TEXT_SECONDARY,
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE, "bold"),
            relief="flat"
        )
        style.map(
            "Modern.Treeview",
            background=[("selected", Colors.PRIMARY)],
            foreground=[("selected", "#FFFFFF")]
        )
        
        # Создаем Treeview
        self.tree = ttk.Treeview(
            self,
            columns=[col[0] for col in columns],
            show="headings",
            style="Modern.Treeview"
        )
        
        # Настраиваем колонки
        for col_id, col_name, col_width in columns:
            self.tree.heading(col_id, text=col_name)
            self.tree.column(col_id, width=col_width, anchor="center")
        
        # Scrollbar
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
    
    def get_children(self):
        return self.tree.get_children()
    
    def delete(self, *items):
        self.tree.delete(*items)




# ═══════════════════════════════════════════════════════════════════════════════
# СТАРТОВЫЙ ЭКРАН
# ═══════════════════════════════════════════════════════════════════════════════

class StartFrame(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        
        # Центральный контейнер
        center_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center_frame.place(relx=0.5, rely=0.5, anchor="center")
        
        # Логотип/Заголовок
        logo_frame = tk.Frame(center_frame, bg=Colors.BG_PRIMARY)
        logo_frame.pack(pady=(0, Spacing.XL))
        
        # Иконка приложения
        icon_label = tk.Label(
            logo_frame,
            text="📦",
            font=(Fonts.FAMILY, 48),
            bg=Colors.BG_PRIMARY
        )
        icon_label.pack()
        
        title_label = tk.Label(
            logo_frame,
            text="TrackingApp",
            font=(Fonts.FAMILY, Fonts.TITLE_SIZE, "bold"),
            fg=Colors.TEXT_PRIMARY,
            bg=Colors.BG_PRIMARY
        )
        title_label.pack(pady=(Spacing.SM, 0))
        
        subtitle_label = tk.Label(
            logo_frame,
            text="Система відстеження та управління",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_PRIMARY
        )
        subtitle_label.pack(pady=(Spacing.XS, 0))
        
        # Карточка с кнопками
        card = ModernCard(center_frame, padding=Spacing.XL)
        card.pack(fill=tk.X, padx=Spacing.XXL)
        
        # Заголовок карточки
        card_title = tk.Label(
            card.content,
            text="Оберіть модуль для роботи",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_CARD
        )
        card_title.pack(pady=(0, Spacing.LG))
        
        # Кнопка TrackingApp
        tracking_btn = ModernButton(
            card.content,
            text="🚚  TrackingApp",
            command=lambda: app.show_frame("TrackingLoginFrame"),
            variant="primary",
            width=280,
            height=50
        )
        tracking_btn.pack(pady=Spacing.SM)
        
        # Кнопка СканПак
        scanpak_btn = ModernButton(
            card.content,
            text="📱  СканПак",
            command=lambda: app.show_frame("ScanpakLoginFrame"),
            variant="secondary",
            width=280,
            height=50
        )
        scanpak_btn.pack(pady=Spacing.SM)
        
        # Версия
        version_label = tk.Label(
            center_frame,
            text="v2.0.0",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_MUTED,
            bg=Colors.BG_PRIMARY
        )
        version_label.pack(pady=(Spacing.LG, 0))


# ═══════════════════════════════════════════════════════════════════════════════
# ЭКРАН ВХОДА TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class TrackingLoginFrame(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.is_busy = False
        self.message = tk.StringVar(value="")
        self.current_tab = "login"
        
        # Центральный контейнер
        center_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center_frame.place(relx=0.5, rely=0.5, anchor="center")
        
        # Заголовок
        header = tk.Frame(center_frame, bg=Colors.BG_PRIMARY)
        header.pack(pady=(0, Spacing.LG))
        
        back_btn = ModernButton(
            header,
            text="← Назад",
            command=lambda: app.show_frame("StartFrame"),
            variant="ghost",
            width=100,
            height=36
        )
        back_btn.pack(side=tk.LEFT)
        
        title = tk.Label(
            header,
            text="TrackingApp",
            font=(Fonts.FAMILY, Fonts.HEADER_SIZE, "bold"),
            fg=Colors.TEXT_PRIMARY,
            bg=Colors.BG_PRIMARY
        )
        title.pack(side=tk.LEFT, padx=Spacing.LG)
        
        # Карточка
        card = ModernCard(center_frame, padding=Spacing.XL)
        card.pack()
        
        # Переключатель вкладок
        tab_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        tab_frame.pack(fill=tk.X, pady=(0, Spacing.LG))
        
        self.login_tab_btn = tk.Label(
            tab_frame,
            text="Вхід",
            font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"),
            fg=Colors.PRIMARY,
            bg=Colors.BG_CARD,
            padx=Spacing.LG,
            pady=Spacing.SM,
            cursor="hand2"
        )
        self.login_tab_btn.pack(side=tk.LEFT)
        self.login_tab_btn.bind("<Button-1>", lambda e: self._switch_tab("login"))
        
        self.register_tab_btn = tk.Label(
            tab_frame,
            text="Реєстрація",
            font=(Fonts.FAMILY, Fonts.BODY_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_CARD,
            padx=Spacing.LG,
            pady=Spacing.SM,
            cursor="hand2"
        )
        self.register_tab_btn.pack(side=tk.LEFT)
        self.register_tab_btn.bind("<Button-1>", lambda e: self._switch_tab("register"))
        
        # Контейнер для форм
        self.form_container = tk.Frame(card.content, bg=Colors.BG_CARD)
        self.form_container.pack(fill=tk.BOTH, expand=True)
        
        # Форма входа
        self.login_form = tk.Frame(self.form_container, bg=Colors.BG_CARD)
        self._build_login_form()
        
        # Форма регистрации
        self.register_form = tk.Frame(self.form_container, bg=Colors.BG_CARD)
        self._build_register_form()
        
        self.login_form.pack(fill=tk.BOTH, expand=True)
        
        # Сообщение об ошибке
        self.message_label = tk.Label(
            card.content,
            textvariable=self.message,
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.ERROR,
            bg=Colors.BG_CARD,
            wraplength=300
        )
        self.message_label.pack(pady=(Spacing.MD, 0))
        
        # Кнопка админ панели
        admin_btn = ModernButton(
            card.content,
            text="🔐 Адмін панель",
            command=self.open_admin_panel,
            variant="ghost",
            width=160,
            height=36
        )
        admin_btn.pack(pady=(Spacing.LG, 0))
    
    def _switch_tab(self, tab: str):
        self.current_tab = tab
        self.message.set("")
        
        if tab == "login":
            self.login_tab_btn.config(fg=Colors.SECONDARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"))
            self.register_tab_btn.config(fg=Colors.TEXT_SECONDARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE))
            self.register_form.pack_forget()
            self.login_form.pack(fill=tk.BOTH, expand=True)
        else:
            self.register_tab_btn.config(fg=Colors.SECONDARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"))
            self.login_tab_btn.config(fg=Colors.TEXT_SECONDARY, font=(Fonts.FAMILY, Fonts.BODY_SIZE))
            self.login_form.pack_forget()
            self.register_form.pack(fill=tk.BOTH, expand=True)
    
    def _build_login_form(self):
        self.login_surname_entry = ModernEntry(self.login_form, label="Прізвище", icon="👤")
        self.login_surname_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.login_password_entry = ModernEntry(self.login_form, label="Пароль", icon="🔒", show="•")
        self.login_password_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        login_btn = ModernButton(
            self.login_form,
            text="Увійти",
            command=self.handle_login,
            variant="primary",
            width=320,
            height=44
        )
        login_btn.pack(pady=(Spacing.LG, 0))
        
        self.login_password_entry.bind("<Return>", lambda e: self.handle_login())
    
    def _build_register_form(self):
        self.register_surname_entry = ModernEntry(self.register_form, label="Прізвище", icon="👤")
        self.register_surname_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.register_password_entry = ModernEntry(self.register_form, label="Пароль", icon="🔒", show="•")
        self.register_password_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.register_confirm_entry = ModernEntry(self.register_form, label="Підтвердження пароля", icon="🔒", show="•")
        self.register_confirm_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        register_btn = ModernButton(
            self.register_form,
            text="Надіслати заявку",
            command=self.handle_register,
            variant="primary",
            width=320,
            height=44
        )
        register_btn.pack(pady=(Spacing.LG, 0))
        
        self.register_confirm_entry.bind("<Return>", lambda e: self.handle_register())
    
    def handle_login(self) -> None:
        surname = self.login_surname_entry.get().strip()
        password = self.login_password_entry.get().strip()
        
        if not surname or not password:
            self.message.set("Введіть прізвище та пароль")
            return
        
        self.message.set("")
        
        def task() -> Dict[str, Any]:
            return self.app.scanpak_api.request_json(
                "POST", "/login", payload={"surname": surname, "password": password}
            )
        
        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                self.message.set("Сервер не повернув токен")
                return
            self.app.update_state({
                "scanpak_token": token,
                "scanpak_user_name": data.get("surname", surname),
                "scanpak_user_role": data.get("role"),
            })
            self.app.show_frame("ScanpakMainFrame")
        
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
            return self.app.scanpak_api.request_json(
                "POST", "/register", payload={"surname": surname, "password": password}
            )
        
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
        password = simple_prompt(self, "Пароль адміністратора СканПак")
        if not password:
            return
        
        def task() -> Dict[str, Any]:
            return self.app.scanpak_api.request_json(
                "POST", "/admin_login", payload={"password": password}
            )
        
        def on_success(data: Dict[str, Any]) -> None:
            token = str(data.get("token", ""))
            if not token:
                messagebox.showerror("Помилка", "Сервер не повернув токен")
                return
            ScanpakAdminPanel(self, self.app, token)
        
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
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.status = tk.StringVar(value="")
        self.user_label = tk.StringVar(value="")
        self.role_label = tk.StringVar(value="")
        self.access_level = None
        
        # Хедер
        header = tk.Frame(self, bg=Colors.BG_SECONDARY, height=60)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        
        header_content = tk.Frame(header, bg=Colors.BG_SECONDARY)
        header_content.pack(fill=tk.BOTH, expand=True, padx=Spacing.LG)
        
        # Логотип
        logo_frame = tk.Frame(header_content, bg=Colors.BG_SECONDARY)
        logo_frame.pack(side=tk.LEFT, pady=Spacing.SM)
        
        tk.Label(
            logo_frame,
            text="📦",
            font=(Fonts.FAMILY, 20),
            bg=Colors.BG_SECONDARY
        ).pack(side=tk.LEFT)
        
        tk.Label(
            logo_frame,
            text="TrackingApp",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.TEXT_PRIMARY,
            bg=Colors.BG_SECONDARY
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Информация о пользователе (справа)
        user_frame = tk.Frame(header_content, bg=Colors.BG_SECONDARY)
        user_frame.pack(side=tk.RIGHT, pady=Spacing.SM)
        
        logout_btn = ModernButton(
            user_frame,
            text="🚪 Вийти",
            command=self.logout,
            variant="ghost",
            width=90,
            height=32
        )
        logout_btn.pack(side=tk.RIGHT, padx=Spacing.XS)
        
        self.role_badge = tk.Label(
            user_frame,
            textvariable=self.role_label,
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE, "bold"),
            fg=Colors.PRIMARY,
            bg=Colors.BG_TERTIARY,
            padx=Spacing.SM,
            pady=Spacing.XS
        )
        self.role_badge.pack(side=tk.RIGHT, padx=Spacing.SM)
        
        tk.Label(
            user_frame,
            textvariable=self.user_label,
            font=(Fonts.FAMILY, Fonts.BODY_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_SECONDARY
        ).pack(side=tk.RIGHT, padx=Spacing.SM)
        
        # Контент
        content = tk.Frame(self, bg=Colors.BG_PRIMARY)
        content.pack(fill=tk.BOTH, expand=True, padx=Spacing.MD, pady=Spacing.MD)
        
        # Notebook
        self.notebook = ModernNotebook(content)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Вкладки
        self.scan_tab = TrackingScanTab(self.notebook.content_frame, app, self.status)
        self.history_tab = HistoryTab(self.notebook.content_frame, app)
        self.errors_tab = ErrorsTab(self.notebook.content_frame, app)
        self.stats_tab = StatisticsTab(self.notebook.content_frame, app)
        
        self.notebook.add_tab("scan", "📷 Сканер", self.scan_tab)
        self.notebook.add_tab("history", "📋 Історія", self.history_tab)
        self.notebook.add_tab("errors", "⚠️ Помилки", self.errors_tab)
        self.notebook.add_tab("stats", "📊 Статистика", self.stats_tab)
        
        # Статус бар
        status_bar = tk.Frame(self, bg=Colors.BG_SECONDARY, height=36)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
        status_bar.pack_propagate(False)
        
        self.status_icon = tk.Label(
            status_bar,
            text="💡",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            bg=Colors.BG_SECONDARY
        )
        self.status_icon.pack(side=tk.LEFT, padx=(Spacing.MD, Spacing.XS), pady=Spacing.XS)
        
        tk.Label(
            status_bar,
            textvariable=self.status,
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_SECONDARY
        ).pack(side=tk.LEFT, pady=Spacing.XS)
    
    def refresh(self) -> None:
        user = self.app.state_data.get("user_name", "оператор")
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        self.access_level = access_level
        
        role_text = "👁 Перегляд"
        if role == "admin" or access_level == 1:
            role_text = "🔑 Адмін"
        elif role == "operator" or access_level == 0:
            role_text = "🧰 Оператор"
        
        self.user_label.set(f"👤 {user}")
        self.role_label.set(role_text)
        
        self.scan_tab.refresh()
        self.history_tab.refresh()
        self.errors_tab.refresh()
        self.stats_tab.refresh()
    
    def logout(self) -> None:
        self.app.clear_state(["token", "access_level", "user_name", "user_role"])
        self.app.show_frame("StartFrame")


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА СКАНЕРА TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

class TrackingScanTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, status: tk.StringVar) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.status = status
        self.inflight = tk.IntVar(value=0)
        
        # Центральный контейнер
        center = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center.place(relx=0.5, rely=0.4, anchor="center")
        
        # Карточка сканера
        card = ModernCard(center, title="📷 Сканування", padding=Spacing.XL)
        card.pack()
        
        # Описание
        tk.Label(
            card.content,
            text="Введіть дані для реєстрації відправлення",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_MUTED,
            bg=Colors.BG_CARD
        ).pack(anchor="w", pady=(0, Spacing.MD))
        
        # Поля ввода
        self.box_entry = ModernEntry(card.content, label="BoxID", icon="📦")
        self.box_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        self.ttn_entry = ModernEntry(card.content, label="ТТН (номер накладної)", icon="🏷️")
        self.ttn_entry.pack(fill=tk.X, pady=Spacing.SM)
        
        # Кнопки
        btn_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        btn_frame.pack(fill=tk.X, pady=(Spacing.LG, 0))
        
        send_btn = ModernButton(
            btn_frame,
            text="📤 Надіслати",
            command=self.send_record,
            variant="primary",
            width=150,
            height=44
        )
        send_btn.pack(side=tk.LEFT)
        
        sync_btn = ModernButton(
            btn_frame,
            text="🔄 Синхронізувати",
            command=self.sync_offline,
            variant="secondary",
            width=160,
            height=44
        )
        sync_btn.pack(side=tk.LEFT, padx=Spacing.SM)
        
        # Счетчик офлайн
        offline_card = tk.Frame(card.content, bg=Colors.BG_TERTIARY)
        offline_card.pack(fill=tk.X, pady=(Spacing.LG, 0))
        
        offline_inner = tk.Frame(offline_card, bg=Colors.BG_TERTIARY)
        offline_inner.pack(padx=Spacing.MD, pady=Spacing.SM)
        
        tk.Label(
            offline_inner,
            text="📴",
            font=(Fonts.FAMILY, 16),
            bg=Colors.BG_TERTIARY
        ).pack(side=tk.LEFT)
        
        tk.Label(
            offline_inner,
            text="Записів в офлайн черзі:",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_SECONDARY,
            bg=Colors.BG_TERTIARY
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        self.offline_count_label = tk.Label(
            offline_inner,
            textvariable=self.inflight,
            font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"),
            fg=Colors.WARNING,
            bg=Colors.BG_TERTIARY
        )
        self.offline_count_label.pack(side=tk.LEFT)
        
        # Привязка клавиш
        self.box_entry.bind("<Return>", lambda e: self.ttn_entry.focus())
        self.ttn_entry.bind("<Return>", lambda e: self.send_record())
    
    def refresh(self) -> None:
        count = len(self.app.tracking_offline.list())
        self.inflight.set(count)
        if count > 0:
            self.offline_count_label.config(fg=Colors.WARNING)
        else:
            self.offline_count_label.config(fg=Colors.SUCCESS)
    
    def send_record(self) -> None:
        token = self.app.state_data.get("token")
        user_name = self.app.state_data.get("user_name", "operator")
        boxid = "".join(filter(str.isdigit, self.box_entry.get()))
        ttn = "".join(filter(str.isdigit, self.ttn_entry.get()))
        
        if not boxid or not ttn:
            self.status.set("⚠️ Заповніть BoxID та ТТН")
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
                self.status.set(f"⚠️ Дублікат: {note}")
            else:
                self.status.set("✅ Успішно додано")
            self.sync_offline()
        
        def on_error(exc: Exception) -> None:
            self.app.tracking_offline.add(record)
            self.inflight.set(len(self.app.tracking_offline.list()))
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
# ВКЛАДКА ИСТОРИИ
# ═══════════════════════════════════════════════════════════════════════════════

class HistoryTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.records: List[Dict[str, Any]] = []
        self.filtered: List[Dict[str, Any]] = []
        
        # Фильтры
        filters_card = ModernCard(self, title="🔍 Фільтри", padding=Spacing.MD)
        filters_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        # Первая строка фильтров
        row1 = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        row1.pack(fill=tk.X, pady=Spacing.XS)
        
        filter_frame1 = tk.Frame(row1, bg=Colors.BG_CARD)
        filter_frame1.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.box_filter_entry = ModernEntry(filter_frame1, label="BoxID")
        self.box_filter_entry.pack(fill=tk.X)
        
        filter_frame2 = tk.Frame(row1, bg=Colors.BG_CARD)
        filter_frame2.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.ttn_filter_entry = ModernEntry(filter_frame2, label="ТТН")
        self.ttn_filter_entry.pack(fill=tk.X)
        
        filter_frame3 = tk.Frame(row1, bg=Colors.BG_CARD)
        filter_frame3.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.user_filter_entry = ModernEntry(filter_frame3, label="Оператор")
        self.user_filter_entry.pack(fill=tk.X)
        
        # Вторая строка фильтров
        row2 = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        row2.pack(fill=tk.X, pady=Spacing.XS)
        
        filter_frame4 = tk.Frame(row2, bg=Colors.BG_CARD)
        filter_frame4.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.date_filter_entry = ModernEntry(filter_frame4, label="Дата (YYYY-MM-DD)")
        self.date_filter_entry.pack(fill=tk.X)
        
        filter_frame5 = tk.Frame(row2, bg=Colors.BG_CARD)
        filter_frame5.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.start_time_entry = ModernEntry(filter_frame5, label="Час від (HH:MM)")
        self.start_time_entry.pack(fill=tk.X)
        
        filter_frame6 = tk.Frame(row2, bg=Colors.BG_CARD)
        filter_frame6.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.end_time_entry = ModernEntry(filter_frame6, label="Час до (HH:MM)")
        self.end_time_entry.pack(fill=tk.X)
        
        # Кнопки фильтров
        btn_row = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        btn_row.pack(fill=tk.X, pady=(Spacing.MD, 0))
        
        ModernButton(
            btn_row, text="🔍 Застосувати", command=self.apply_filters,
            variant="primary", width=130, height=36
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            btn_row, text="🧹 Очистити", command=self.clear_filters,
            variant="secondary", width=110, height=36
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            btn_row, text="🔄 Оновити", command=self.fetch_history,
            variant="secondary", width=110, height=36
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            btn_row, text="🗑️ Очистити історію", command=self.clear_history,
            variant="danger", width=160, height=36
        ).pack(side=tk.RIGHT, padx=Spacing.XS)
        
        # Счетчик записей
        self.count_label = tk.Label(
            self,
            text="Записів: 0",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_MUTED,
            bg=Colors.BG_PRIMARY
        )
        self.count_label.pack(anchor="w", padx=Spacing.MD, pady=Spacing.XS)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("datetime", "Дата та час", 180),
                ("user", "Оператор", 150),
                ("boxid", "BoxID", 150),
                ("ttn", "ТТН", 180),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
    
    def refresh(self) -> None:
        self.fetch_history()
    
    def fetch_history(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return
        
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/get_history", token=token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = sorted(data, key=lambda item: item.get("datetime", ""), reverse=True)
            self.apply_filters()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def apply_filters(self) -> None:
        filtered = list(self.records)
        
        box_filter = self.box_filter_entry.get().strip()
        if box_filter:
            filtered = [rec for rec in filtered if box_filter in str(rec.get("boxid", ""))]
        
        ttn_filter = self.ttn_filter_entry.get().strip()
        if ttn_filter:
            filtered = [rec for rec in filtered if ttn_filter in str(rec.get("ttn", ""))]
        
        user_filter = self.user_filter_entry.get().strip().lower()
        if user_filter:
            filtered = [rec for rec in filtered if user_filter in str(rec.get("user_name", "")).lower()]
        
        selected_date = parse_date(self.date_filter_entry.get().strip())
        if selected_date:
            filtered = [rec for rec in filtered if self._match_date(rec.get("datetime"), selected_date)]
        
        start_time = parse_time(self.start_time_entry.get().strip())
        end_time = parse_time(self.end_time_entry.get().strip())
        if start_time or end_time:
            filtered = [rec for rec in filtered if self._match_time(rec.get("datetime"), start_time, end_time)]
        
        self.filtered = filtered
        self.count_label.config(text=f"Записів: {len(filtered)}")
        self._refresh_tree()
    
    def _match_date(self, value: Any, selected: date) -> bool:
        try:
            parsed = datetime.fromisoformat(str(value)).date()
        except ValueError:
            return False
        return parsed == selected
    
    def _match_time(self, value: Any, start: Optional[time], end: Optional[time]) -> bool:
        try:
            parsed = datetime.fromisoformat(str(value)).time()
        except ValueError:
            return False
        if start and parsed < start:
            return False
        if end and parsed > end:
            return False
        return True
    
    def _refresh_tree(self) -> None:
        self.tree.clear()
        for record in self.filtered:
            self.tree.insert((
                format_datetime(record.get("datetime", "")),
                record.get("user_name", ""),
                record.get("boxid", ""),
                record.get("ttn", ""),
            ))
    
    def clear_filters(self) -> None:
        self.box_filter_entry.set("")
        self.ttn_filter_entry.set("")
        self.user_filter_entry.set("")
        self.date_filter_entry.set("")
        self.start_time_entry.set("")
        self.end_time_entry.set("")
        self.apply_filters()
    
    def clear_history(self) -> None:
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        if role != "admin" and access_level != 1:
            messagebox.showinfo("Доступ", "Очистка доступна лише адміну")
            return
        
        token = self.app.state_data.get("token")
        if not token:
            return
        
        if not messagebox.askyesno("Підтвердження", "Ви впевнені, що хочете очистити всю історію?"):
            return
        
        def task() -> Any:
            return self.app.api.request_json("DELETE", "/clear_tracking", token=token)
        
        def on_success(_: Any) -> None:
            self.records = []
            self.apply_filters()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА ОШИБОК
# ═══════════════════════════════════════════════════════════════════════════════

class ErrorsTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.records: List[Dict[str, Any]] = []
        
        # Панель действий
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(
            actions, text="🔄 Оновити", command=self.fetch_errors,
            variant="primary", width=120, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            actions, text="🗑️ Очистити всі", command=self.clear_errors,
            variant="danger", width=140, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            actions, text="❌ Видалити вибране", command=self.delete_selected,
            variant="secondary", width=170, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Счетчик
        self.count_label = tk.Label(
            actions,
            text="Помилок: 0",
            font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"),
            fg=Colors.ERROR,
            bg=Colors.BG_CARD
        )
        self.count_label.pack(side=tk.RIGHT, padx=Spacing.MD)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("datetime", "Дата", 150),
                ("user", "Оператор", 120),
                ("boxid", "BoxID", 120),
                ("ttn", "ТТН", 140),
                ("note", "Примітка", 200),
                ("id", "ID", 60),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
    
    def refresh(self) -> None:
        self.fetch_errors()
    
    def fetch_errors(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return
        
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/get_errors", token=token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = data
            self.count_label.config(text=f"Помилок: {len(data)}")
            self.tree.clear()
            for record in data:
                self.tree.insert((
                    format_datetime(record.get("datetime", "")),
                    record.get("user_name", ""),
                    record.get("boxid", ""),
                    record.get("ttn", ""),
                    record.get("note", ""),
                    record.get("id", ""),
                ))
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def clear_errors(self) -> None:
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        if role not in {"admin", "operator"} and access_level not in {0, 1}:
            messagebox.showinfo("Доступ", "Недостатньо прав")
            return
        
        token = self.app.state_data.get("token")
        if not token:
            return
        
        if not messagebox.askyesno("Підтвердження", "Очистити всі помилки?"):
            return
        
        def task() -> Any:
            return self.app.api.request_json("DELETE", "/clear_errors", token=token)
        
        def on_success(_: Any) -> None:
            self.records = []
            self.tree.clear()
            self.count_label.config(text="Помилок: 0")
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def delete_selected(self) -> None:
        selection = self.tree.selection()
        if not selection:
            messagebox.showinfo("Помилка", "Оберіть запис для видалення")
            return
        
        item = self.tree.item(selection[0])
        record_id = item["values"][5]
        token = self.app.state_data.get("token")
        if not token:
            return
        
        def task() -> Any:
            return self.app.api.request_json("DELETE", f"/delete_error/{record_id}", token=token)
        
        def on_success(_: Any) -> None:
            self.fetch_errors()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА СТАТИСТИКИ
# ═══════════════════════════════════════════════════════════════════════════════

class StatisticsTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.history: List[Dict[str, Any]] = []
        self.errors: List[Dict[str, Any]] = []
        self.summary = tk.StringVar(value="")
        
        # Фильтры
        filters_card = ModernCard(self, title="📅 Період", padding=Spacing.MD)
        filters_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        filters_row = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        filters_row.pack(fill=tk.X)
        
        date_frame1 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        date_frame1.pack(side=tk.LEFT, padx=Spacing.XS)
        self.start_date_entry = ModernEntry(date_frame1, label="Початок (YYYY-MM-DD)")
        self.start_date_entry.pack()
        
        date_frame2 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        date_frame2.pack(side=tk.LEFT, padx=Spacing.XS)
        self.end_date_entry = ModernEntry(date_frame2, label="Кінець (YYYY-MM-DD)")
        self.end_date_entry.pack()
        
        ModernButton(
            filters_row, text="📊 Оновити статистику", command=self.fetch_data,
            variant="primary", width=180, height=40
        ).pack(side=tk.LEFT, padx=Spacing.MD)
        
        # Карточки со статистикой
        stats_cards = tk.Frame(self, bg=Colors.BG_PRIMARY)
        stats_cards.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        # Карточка сканов
        self.scans_card = self._create_stat_card(stats_cards, "📦", "Всього сканів", "0", Colors.PRIMARY)
        self.scans_card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        
        # Карточка ошибок
        self.errors_card = self._create_stat_card(stats_cards, "⚠️", "Помилок", "0", Colors.ERROR)
        self.errors_card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        
        # Карточка топ оператора
        self.top_card = self._create_stat_card(stats_cards, "🏆", "Топ оператор", "—", Colors.SUCCESS)
        self.top_card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        
        # Таблица по операторам
        table_label = tk.Label(
            self,
            text="📋 Статистика по операторах",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.TEXT_PRIMARY,
            bg=Colors.BG_PRIMARY
        )
        table_label.pack(anchor="w", padx=Spacing.MD, pady=(Spacing.MD, Spacing.SM))
        
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("user", "Оператор", 200),
                ("scans", "Сканів", 150),
                ("errors", "Помилок", 150),
                ("efficiency", "Ефективність", 150),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
    
    def _create_stat_card(self, parent, icon: str, title: str, value: str, color: str) -> tk.Frame:
        card = tk.Frame(parent, bg=Colors.BG_CARD)
        
        inner = tk.Frame(card, bg=Colors.BG_CARD)
        inner.pack(padx=Spacing.MD, pady=Spacing.MD)
        
        tk.Label(inner, text=icon, font=(Fonts.FAMILY, 24), bg=Colors.BG_CARD).pack()
        tk.Label(inner, text=title, font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_MUTED, bg=Colors.BG_CARD).pack()
        
        value_label = tk.Label(inner, text=value, font=(Fonts.FAMILY, Fonts.HEADER_SIZE, "bold"), fg=color, bg=Colors.BG_CARD)
        value_label.pack()
        
        card.value_label = value_label
        return card
    
    def refresh(self) -> None:
        role = self.app.state_data.get("user_role")
        access_level = self.app.state_data.get("access_level")
        if role != "admin" and access_level != 1:
            self.scans_card.value_label.config(text="🔒")
            self.errors_card.value_label.config(text="🔒")
            self.top_card.value_label.config(text="Тільки для адмінів")
            return
        
        if not self.start_date_entry.get() or not self.end_date_entry.get():
            today = datetime.now().date()
            self.start_date_entry.set(today.replace(day=1).isoformat())
            self.end_date_entry.set(today.isoformat())
        
        self.fetch_data()
    
    def fetch_data(self) -> None:
        token = self.app.state_data.get("token")
        if not token:
            return
        
        def task() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            history = self.app.api.request_json("GET", "/get_history", token=token)
            errors = self.app.api.request_json("GET", "/get_errors", token=token)
            return (
                history if isinstance(history, list) else [],
                errors if isinstance(errors, list) else [],
            )
        
        def on_success(data: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]) -> None:
            self.history, self.errors = data
            self.apply_stats()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def apply_stats(self) -> None:
        start = parse_date(self.start_date_entry.get().strip())
        end = parse_date(self.end_date_entry.get().strip())
        
        if not start or not end:
            messagebox.showinfo("Фільтр", "Вкажіть коректні дати")
            return
        
        def in_range(record: Dict[str, Any]) -> bool:
            try:
                dt = datetime.fromisoformat(record.get("datetime", "")).date()
            except ValueError:
                return False
            return start <= dt <= end
        
        history = [rec for rec in self.history if in_range(rec)]
        errors = [rec for rec in self.errors if in_range(rec)]
        
        counts: Dict[str, int] = {}
        error_counts: Dict[str, int] = {}
        
        for rec in history:
            user = rec.get("user_name", "—")
            counts[user] = counts.get(user, 0) + 1
        
        for rec in errors:
            user = rec.get("user_name", "—")
            error_counts[user] = error_counts.get(user, 0) + 1
        
        total_scans = sum(counts.values())
        total_errors = sum(error_counts.values())
        top_user = max(counts.items(), key=lambda item: item[1], default=("—", 0))
        
        # Обновляем карточки
        self.scans_card.value_label.config(text=str(total_scans))
        self.errors_card.value_label.config(text=str(total_errors))
        self.top_card.value_label.config(text=f"{top_user[0]} ({top_user[1]})")
        
        # Обновляем таблицу
        self.tree.clear()
        for user, scans in sorted(counts.items(), key=lambda item: item[1], reverse=True):
            user_errors = error_counts.get(user, 0)
            efficiency = f"{((scans - user_errors) / scans * 100):.1f}%" if scans > 0 else "—"
            self.tree.insert((user, scans, user_errors, efficiency))


# ═══════════════════════════════════════════════════════════════════════════════
# ГЛАВНЫЙ ЭКРАН SCANPAK
# ═══════════════════════════════════════════════════════════════════════════════

class ScanpakMainFrame(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.status = tk.StringVar(value="")
        self.user_label = tk.StringVar(value="")
        
        # Хедер
        header = tk.Frame(self, bg=Colors.BG_SECONDARY, height=60)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        
        header_content = tk.Frame(header, bg=Colors.BG_SECONDARY)
        header_content.pack(fill=tk.BOTH, expand=True, padx=Spacing.LG)
        
        # Логотип
        logo_frame = tk.Frame(header_content, bg=Colors.BG_SECONDARY)
        logo_frame.pack(side=tk.LEFT, pady=Spacing.SM)
        
        tk.Label(logo_frame, text="📱", font=(Fonts.FAMILY, 20), bg=Colors.BG_SECONDARY).pack(side=tk.LEFT)
        tk.Label(
            logo_frame, text="СканПак",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.SECONDARY, bg=Colors.BG_SECONDARY
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Пользователь
        user_frame = tk.Frame(header_content, bg=Colors.BG_SECONDARY)
        user_frame.pack(side=tk.RIGHT, pady=Spacing.SM)
        
        ModernButton(user_frame, text="🚪 Вийти", command=self.logout, variant="ghost", width=90, height=32).pack(side=tk.RIGHT, padx=Spacing.XS)
        
        tk.Label(
            user_frame, textvariable=self.user_label,
            font=(Fonts.FAMILY, Fonts.BODY_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY
        ).pack(side=tk.RIGHT, padx=Spacing.SM)
        
        # Контент
        content = tk.Frame(self, bg=Colors.BG_PRIMARY)
        content.pack(fill=tk.BOTH, expand=True, padx=Spacing.MD, pady=Spacing.MD)
        
        self.notebook = ModernNotebook(content)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        self.scan_tab = ScanpakScanTab(self.notebook.content_frame, app, self.status)
        self.history_tab = ScanpakHistoryTab(self.notebook.content_frame, app)
        self.stats_tab = ScanpakStatsTab(self.notebook.content_frame, app)
        
        self.notebook.add_tab("scan", "📷 Сканування", self.scan_tab)
        self.notebook.add_tab("history", "📋 Історія", self.history_tab)
        self.notebook.add_tab("stats", "📊 Статистика", self.stats_tab)
        
        # Статус бар
        status_bar = tk.Frame(self, bg=Colors.BG_SECONDARY, height=36)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM)
        status_bar.pack_propagate(False)
        
        tk.Label(status_bar, text="💡", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), bg=Colors.BG_SECONDARY).pack(side=tk.LEFT, padx=(Spacing.MD, Spacing.XS))
        tk.Label(status_bar, textvariable=self.status, font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_SECONDARY).pack(side=tk.LEFT)
    
    def refresh(self) -> None:
        name = self.app.state_data.get("scanpak_user_name", "оператор")
        role = self.app.state_data.get("scanpak_user_role", "")
        role_label = "🔑 Адмін" if role == "admin" else "🧰 Оператор"
        self.user_label.set(f"👤 {name} • {role_label}")
        
        self.scan_tab.refresh()
        self.history_tab.refresh()
        self.stats_tab.refresh()
    
    def logout(self) -> None:
        self.app.clear_state(["scanpak_token", "scanpak_user_name", "scanpak_user_role"])
        self.app.show_frame("StartFrame")


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА СКАНЕРА SCANPAK
# ═══════════════════════════════════════════════════════════════════════════════

class ScanpakScanTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, status: tk.StringVar) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.status = status
        self.offline_count = tk.IntVar(value=0)
        
        center = tk.Frame(self, bg=Colors.BG_PRIMARY)
        center.place(relx=0.5, rely=0.4, anchor="center")
        
        card = ModernCard(center, title="📷 Сканування відправлень", padding=Spacing.XL)
        card.pack()
        
        tk.Label(
            card.content, text="Введіть або відскануйте номер відправлення",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_MUTED, bg=Colors.BG_CARD
        ).pack(anchor="w", pady=(0, Spacing.MD))
        
        self.number_entry = ModernEntry(card.content, label="Номер відправлення", icon="📦")
        self.number_entry.pack(fill=tk.X, pady=Spacing.SM)
        self.number_entry.bind("<Return>", lambda e: self.submit())
        
        btn_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        btn_frame.pack(fill=tk.X, pady=(Spacing.LG, 0))
        
        ModernButton(btn_frame, text="💾 Зберегти", command=self.submit, variant="primary", width=140, height=44).pack(side=tk.LEFT)
        ModernButton(btn_frame, text="🔄 Синхронізувати", command=self.sync_offline, variant="secondary", width=160, height=44).pack(side=tk.LEFT, padx=Spacing.SM)
        
        # Офлайн счетчик
        offline_card = tk.Frame(card.content, bg=Colors.BG_TERTIARY)
        offline_card.pack(fill=tk.X, pady=(Spacing.LG, 0))
        
        offline_inner = tk.Frame(offline_card, bg=Colors.BG_TERTIARY)
        offline_inner.pack(padx=Spacing.MD, pady=Spacing.SM)
        
        tk.Label(offline_inner, text="📴", font=(Fonts.FAMILY, 16), bg=Colors.BG_TERTIARY).pack(side=tk.LEFT)
        tk.Label(offline_inner, text="В черзі офлайн:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_TERTIARY).pack(side=tk.LEFT, padx=Spacing.XS)
        
        self.offline_label = tk.Label(offline_inner, textvariable=self.offline_count, font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"), fg=Colors.WARNING, bg=Colors.BG_TERTIARY)
        self.offline_label.pack(side=tk.LEFT)
    
    def refresh(self) -> None:
        count = len(self.app.scanpak_offline.list())
        self.offline_count.set(count)
        self.offline_label.config(fg=Colors.WARNING if count > 0 else Colors.SUCCESS)
    
    def submit(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        number = "".join(filter(str.isdigit, self.number_entry.get()))
        
        if not number:
            self.status.set("⚠️ Введіть номер")
            return
        
        if self.app.scanpak_offline.contains("parcel_number", number):
            self.status.set("⚠️ Увага, це дублікат в офлайн черзі")
            self.number_entry.set("")
            return
        
        self.number_entry.set("")
        self.number_entry.focus()
        
        def task() -> Dict[str, Any]:
            if not token:
                raise ApiError("Відсутній токен", 401)
            return self.app.scanpak_api.request_json("POST", "/scans", token=token, payload={"parcel_number": number})
        
        def on_success(_: Dict[str, Any]) -> None:
            self.status.set("✅ Збережено")
            self.sync_offline()
        
        def on_error(exc: Exception) -> None:
            self.app.scanpak_offline.add({"parcel_number": number})
            self.refresh()
            if isinstance(exc, ApiError):
                self.status.set(f"⚠️ {exc.message}")
            else:
                self.status.set("📦 Збережено локально (офлайн)")
        
        run_async(self, task, on_success, on_error)
    
    def sync_offline(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        if not token:
            return
        
        pending = self.app.scanpak_offline.list()
        if not pending:
            self.status.set("✅ Офлайн-черга порожня")
            self.refresh()
            return
        
        def task() -> int:
            synced = 0
            for record in pending:
                try:
                    self.app.scanpak_api.request_json("POST", "/scans", token=token, payload=record)
                    synced += 1
                except ApiError:
                    break
            return synced
        
        def on_success(count: int) -> None:
            if count:
                self.app.scanpak_offline.clear()
            self.refresh()
            self.status.set(f"✅ Синхронізовано {count} записів")
        
        def on_error(_: Exception) -> None:
            self.status.set("❌ Не вдалося синхронізувати")
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА ИСТОРИИ SCANPAK
# ═══════════════════════════════════════════════════════════════════════════════

class ScanpakHistoryTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.records: List[Dict[str, Any]] = []
        self.filtered: List[Dict[str, Any]] = []
        
        # Фильтры
        filters_card = ModernCard(self, title="🔍 Фільтри", padding=Spacing.MD)
        filters_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        filters_row = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        filters_row.pack(fill=tk.X)
        
        filter_frame1 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        filter_frame1.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.parcel_filter_entry = ModernEntry(filter_frame1, label="Номер відправлення")
        self.parcel_filter_entry.pack(fill=tk.X)
        
        filter_frame2 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        filter_frame2.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.user_filter_entry = ModernEntry(filter_frame2, label="Оператор")
        self.user_filter_entry.pack(fill=tk.X)
        
        filter_frame3 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        filter_frame3.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        self.date_filter_entry = ModernEntry(filter_frame3, label="Дата (YYYY-MM-DD)")
        self.date_filter_entry.pack(fill=tk.X)
        
        # Кнопки
        btn_row = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        btn_row.pack(fill=tk.X, pady=(Spacing.MD, 0))
        
        ModernButton(
            btn_row, text="🔍 Застосувати", command=self.apply_filters,
            variant="primary", width=130, height=36
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            btn_row, text="🧹 Очистити", command=self.clear_filters,
            variant="secondary", width=110, height=36
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            btn_row, text="🔄 Оновити", command=self.fetch_history,
            variant="secondary", width=110, height=36
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Счетчик записей
        self.count_label = tk.Label(
            self,
            text="Записів: 0",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_MUTED,
            bg=Colors.BG_PRIMARY
        )
        self.count_label.pack(anchor="w", padx=Spacing.MD, pady=Spacing.XS)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("datetime", "Дата та час", 200),
                ("user", "Оператор", 180),
                ("parcel", "Номер відправлення", 220),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
    
    def refresh(self) -> None:
        self.fetch_history()
    
    def fetch_history(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        if not token:
            return
        
        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/history", token=token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = sorted(data, key=lambda x: x.get("timestamp", x.get("datetime", "")), reverse=True)
            self.apply_filters()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def apply_filters(self) -> None:
        filtered = list(self.records)
        
        parcel_filter = self.parcel_filter_entry.get().strip()
        if parcel_filter:
            filtered = [rec for rec in filtered if parcel_filter in str(rec.get("parcel_number", ""))]
        
        user_filter = self.user_filter_entry.get().strip().lower()
        if user_filter:
            filtered = [rec for rec in filtered if user_filter in str(rec.get("user", rec.get("user_name", ""))).lower()]
        
        selected_date = parse_date(self.date_filter_entry.get().strip())
        if selected_date:
            filtered = [rec for rec in filtered if self._match_date(rec.get("timestamp", rec.get("datetime")), selected_date)]
        
        self.filtered = filtered
        self.count_label.config(text=f"Записів: {len(filtered)}")
        self._refresh_tree()
    
    def _match_date(self, value: Any, selected: date) -> bool:
        try:
            parsed = datetime.fromisoformat(str(value)).date()
        except ValueError:
            return False
        return parsed == selected
    
    def _refresh_tree(self) -> None:
        self.tree.clear()
        for record in self.filtered:
            timestamp = record.get("timestamp", record.get("datetime", ""))
            self.tree.insert((
                format_datetime(str(timestamp)),
                record.get("user", record.get("user_name", "")),
                record.get("parcel_number", ""),
            ))
    
    def clear_filters(self) -> None:
        self.parcel_filter_entry.set("")
        self.user_filter_entry.set("")
        self.date_filter_entry.set("")
        self.apply_filters()


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА СТАТИСТИКИ SCANPAK
# ═══════════════════════════════════════════════════════════════════════════════

class ScanpakStatsTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.records: List[Dict[str, Any]] = []
        
        # Фильтры дат
        filters_card = ModernCard(self, title="📅 Період", padding=Spacing.MD)
        filters_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        filters_row = tk.Frame(filters_card.content, bg=Colors.BG_CARD)
        filters_row.pack(fill=tk.X)
        
        date_frame1 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        date_frame1.pack(side=tk.LEFT, padx=Spacing.XS)
        self.start_date_entry = ModernEntry(date_frame1, label="Початок (YYYY-MM-DD)")
        self.start_date_entry.pack()
        
        date_frame2 = tk.Frame(filters_row, bg=Colors.BG_CARD)
        date_frame2.pack(side=tk.LEFT, padx=Spacing.XS)
        self.end_date_entry = ModernEntry(date_frame2, label="Кінець (YYYY-MM-DD)")
        self.end_date_entry.pack()
        
        ModernButton(
            filters_row, text="📊 Оновити статистику", command=self.fetch_data,
            variant="primary", width=180, height=40
        ).pack(side=tk.LEFT, padx=Spacing.MD)
        
        # Карточки статистики
        stats_cards = tk.Frame(self, bg=Colors.BG_PRIMARY)
        stats_cards.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        self.scans_card = self._create_stat_card(stats_cards, "📦", "Всього сканів", "0", Colors.SECONDARY)
        self.scans_card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        
        self.top_card = self._create_stat_card(stats_cards, "🏆", "Топ оператор", "—", Colors.SUCCESS)
        self.top_card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        
        self.avg_card = self._create_stat_card(stats_cards, "📈", "Середнє/день", "0", Colors.INFO)
        self.avg_card.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=Spacing.XS)
        
        # Таблица
        table_label = tk.Label(
            self,
            text="📋 Статистика по операторах",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.TEXT_PRIMARY,
            bg=Colors.BG_PRIMARY
        )
        table_label.pack(anchor="w", padx=Spacing.MD, pady=(Spacing.MD, Spacing.SM))
        
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("user", "Оператор", 250),
                ("count", "Кількість сканів", 200),
                ("percent", "% від загального", 150),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
    
    def _create_stat_card(self, parent, icon: str, title: str, value: str, color: str) -> tk.Frame:
        card = tk.Frame(parent, bg=Colors.BG_CARD)
        
        inner = tk.Frame(card, bg=Colors.BG_CARD)
        inner.pack(padx=Spacing.MD, pady=Spacing.MD)
        
        tk.Label(inner, text=icon, font=(Fonts.FAMILY, 24), bg=Colors.BG_CARD).pack()
        tk.Label(inner, text=title, font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_MUTED, bg=Colors.BG_CARD).pack()
        
        value_label = tk.Label(inner, text=value, font=(Fonts.FAMILY, Fonts.HEADER_SIZE, "bold"), fg=color, bg=Colors.BG_CARD)
        value_label.pack()
        
        card.value_label = value_label
        return card
    
    def refresh(self) -> None:
        if not self.start_date_entry.get() or not self.end_date_entry.get():
            today = datetime.now().date()
            self.start_date_entry.set(today.replace(day=1).isoformat())
            self.end_date_entry.set(today.isoformat())
        self.fetch_data()
    
    def fetch_data(self) -> None:
        token = self.app.state_data.get("scanpak_token")
        if not token:
            return
        
        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/history", token=token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.records = data
            self.apply_stats()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def apply_stats(self) -> None:
        start = parse_date(self.start_date_entry.get().strip())
        end = parse_date(self.end_date_entry.get().strip())
        
        if not start or not end:
            messagebox.showinfo("Фільтр", "Вкажіть коректні дати")
            return
        
        counts: Dict[str, int] = {}
        for record in self.records:
            timestamp = record.get("timestamp", record.get("datetime", ""))
            try:
                rec_date = datetime.fromisoformat(str(timestamp)).date()
            except ValueError:
                continue
            if start <= rec_date <= end:
                user = record.get("user", record.get("user_name", "—"))
                counts[user] = counts.get(user, 0) + 1
        
        total = sum(counts.values())
        top = max(counts.items(), key=lambda item: item[1], default=("—", 0))
        
        # Расчет среднего за день
        days = (end - start).days + 1
        avg_per_day = total / days if days > 0 else 0
        
        # Обновляем карточки
        self.scans_card.value_label.config(text=str(total))
        self.top_card.value_label.config(text=f"{top[0]}")
        self.avg_card.value_label.config(text=f"{avg_per_day:.1f}")
        
        # Обновляем таблицу
        self.tree.clear()
        for user, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
            percent = f"{(count / total * 100):.1f}%" if total > 0 else "0%"
            self.tree.insert((user, count, percent))


# ═══════════════════════════════════════════════════════════════════════════════
# АДМИН ПАНЕЛИ
# ═══════════════════════════════════════════════════════════════════════════════

class AdminPanel(tk.Toplevel):
    def __init__(self, parent: tk.Misc, app: TrackingApp, token: str) -> None:
        super().__init__(parent)
        self.app = app
        self.token = token
        self.title("🔐 Адмін панель TrackingApp")
        self.geometry("1000x700")
        self.configure(bg=Colors.BG_PRIMARY)
        
        # Центрируем окно
        self.update_idletasks()
        x = (self.winfo_screenwidth() - 1000) // 2
        y = (self.winfo_screenheight() - 700) // 2
        self.geometry(f"+{x}+{y}")
        
        # Заголовок
        header = tk.Frame(self, bg=Colors.BG_SECONDARY, height=50)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        
        tk.Label(
            header, text="🔐 Адмін панель TrackingApp",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.TEXT_PRIMARY, bg=Colors.BG_SECONDARY
        ).pack(side=tk.LEFT, padx=Spacing.LG, pady=Spacing.SM)
        
        ModernButton(
            header, text="✕ Закрити", command=self.destroy,
            variant="ghost", width=100, height=32
        ).pack(side=tk.RIGHT, padx=Spacing.MD, pady=Spacing.SM)
        
        # Контент
        content = tk.Frame(self, bg=Colors.BG_PRIMARY)
        content.pack(fill=tk.BOTH, expand=True, padx=Spacing.MD, pady=Spacing.MD)
        
        self.notebook = ModernNotebook(content)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        self.pending_tab = AdminPendingTab(self.notebook.content_frame, app, token)
        self.users_tab = AdminUsersTab(self.notebook.content_frame, app, token)
        self.password_tab = AdminPasswordsTab(self.notebook.content_frame, app, token)
        
        self.notebook.add_tab("pending", "📝 Запити на реєстрацію", self.pending_tab)
        self.notebook.add_tab("users", "👥 Користувачі", self.users_tab)
        self.notebook.add_tab("passwords", "🔑 Паролі ролей", self.password_tab)


class AdminPendingTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        self.requests: List[Dict[str, Any]] = []
        
        # Действия
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(
            actions, text="🔄 Оновити", command=self.fetch_requests,
            variant="primary", width=120, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            actions, text="✅ Підтвердити", command=self.approve_request,
            variant="success", width=130, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            actions, text="❌ Відхилити", command=self.reject_request,
            variant="danger", width=120, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Выбор роли
        role_frame = tk.Frame(actions, bg=Colors.BG_CARD)
        role_frame.pack(side=tk.LEFT, padx=Spacing.LG)
        
        tk.Label(
            role_frame, text="Призначити роль:",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD
        ).pack(side=tk.LEFT, padx=(0, Spacing.XS))
        
        self.role_var = tk.StringVar(value="operator")
        
        # Стилизация combobox
        style = ttk.Style()
        style.configure("Modern.TCombobox", padding=5)
        
        role_combo = ttk.Combobox(
            role_frame, textvariable=self.role_var,
            values=["admin", "operator", "viewer"],
            width=12, state="readonly", style="Modern.TCombobox"
        )
        role_combo.pack(side=tk.LEFT)
        
        # Счетчик
        self.count_label = tk.Label(
            actions, text="Запитів: 0",
            font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"),
            fg=Colors.WARNING, bg=Colors.BG_CARD
        )
        self.count_label.pack(side=tk.RIGHT, padx=Spacing.MD)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("id", "ID", 80),
                ("surname", "Прізвище", 200),
                ("created", "Дата створення", 200),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        self.fetch_requests()
    
    def fetch_requests(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/admin/registration_requests", token=self.token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.requests = data
            self.count_label.config(text=f"Запитів: {len(data)}")
            self.tree.clear()
            for req in data:
                self.tree.insert((
                    req.get("id"),
                    req.get("surname", ""),
                    format_datetime(req.get("created_at", ""))
                ))
        
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
            messagebox.showinfo("Увага", "Оберіть запит зі списку")
            return
        
        role = self.role_var.get()
        
        def task() -> Any:
            return self.app.api.request_json(
                "POST", f"/admin/registration_requests/{request_id}/approve",
                token=self.token, payload={"role": role}
            )
        
        def on_success(_: Any) -> None:
            messagebox.showinfo("Успіх", "Запит підтверджено")
            self.fetch_requests()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)
    
    def reject_request(self) -> None:
        request_id = self._selected_id()
        if request_id is None:
            messagebox.showinfo("Увага", "Оберіть запит зі списку")
            return
        
        if not messagebox.askyesno("Підтвердження", "Відхилити цей запит?"):
            return
        
        def task() -> Any:
            return self.app.api.request_json(
                "POST", f"/admin/registration_requests/{request_id}/reject",
                token=self.token
            )
        
        def on_success(_: Any) -> None:
            messagebox.showinfo("Успіх", "Запит відхилено")
            self.fetch_requests()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


class AdminUsersTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        
        # Действия
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(
            actions, text="🔄 Оновити", command=self.fetch_users,
            variant="primary", width=120, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            actions, text="🔄 Змінити роль", command=self.change_role,
            variant="secondary", width=140, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            actions, text="⚡ Змінити активність", command=self.toggle_active,
            variant="secondary", width=170, height=40
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Параметры
        params_frame = tk.Frame(actions, bg=Colors.BG_CARD)
        params_frame.pack(side=tk.LEFT, padx=Spacing.LG)
        
        tk.Label(params_frame, text="Роль:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT)
        self.role_var = tk.StringVar(value="operator")
        ttk.Combobox(params_frame, textvariable=self.role_var, values=["admin", "operator", "viewer"], width=10, state="readonly").pack(side=tk.LEFT, padx=Spacing.XS)
        
        tk.Label(params_frame, text="Активний:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT, padx=(Spacing.MD, 0))
        self.active_var = tk.StringVar(value="true")
        ttk.Combobox(params_frame, textvariable=self.active_var, values=["true", "false"], width=8, state="readonly").pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(
            table_frame,
            columns=[
                ("id", "ID", 60),
                ("surname", "Прізвище", 180),
                ("role", "Роль", 120),
                ("active", "Статус", 100),
            ]
        )
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        self.fetch_users()
    
    def fetch_users(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.api.request_json("GET", "/admin/users", token=self.token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.clear()
            for user in data:
                status = "✅ Активний" if user.get("is_active", False) else "❌ Неактивний"
                self.tree.insert((
                    user.get("id"),
                    user.get("surname"),
                    user.get("role"),
                    status
                ))
        
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
            return self.app.api.request_json(
                "PATCH", f"/admin/users/{user_id}",
                token=self.token, payload={"role": role}
            )
        
        def on_success(_: Any) -> None:
            messagebox.showinfo("Успіх", f"Роль змінено на {role}")
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
            return self.app.api.request_json(
                "PATCH", f"/admin/users/{user_id}",
                token=self.token, payload={"is_active": is_active}
            )
        
        def on_success(_: Any) -> None:
            status = "активовано" if is_active else "деактивовано"
            messagebox.showinfo("Успіх", f"Користувача {status}")
            self.fetch_users()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


class AdminPasswordsTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        self.passwords: Dict[str, str] = {}
        
        # Карточка
        card = ModernCard(self, title="🔑 Управління паролями ролей", padding=Spacing.XL)
        card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        tk.Label(
            card.content,
            text="Тут ви можете змінити паролі для різних ролей користувачів",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_MUTED,
            bg=Colors.BG_CARD
        ).pack(anchor="w", pady=(0, Spacing.MD))
        
        # Выбор роли
        role_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        role_frame.pack(fill=tk.X, pady=Spacing.SM)
        
        tk.Label(
            role_frame, text="Оберіть роль:",
            font=(Fonts.FAMILY, Fonts.BODY_SIZE),
            fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD
        ).pack(side=tk.LEFT)
        
        self.role_var = tk.StringVar(value="operator")
        role_combo = ttk.Combobox(
            role_frame, textvariable=self.role_var,
            values=["admin", "operator", "viewer"],
            width=15, state="readonly"
        )
        role_combo.pack(side=tk.LEFT, padx=Spacing.SM)
        role_combo.bind("<<ComboboxSelected>>", lambda e: self._on_role_change())
        
        # Поле пароля
        self.password_entry = ModernEntry(card.content, label="Новий пароль для обраної ролі", icon="🔒", show="•")
        self.password_entry.pack(fill=tk.X, pady=Spacing.MD)
        
        # Кнопки
        btn_frame = tk.Frame(card.content, bg=Colors.BG_CARD)
        btn_frame.pack(fill=tk.X, pady=(Spacing.MD, 0))
        
        ModernButton(
            btn_frame, text="🔄 Завантажити поточні", command=self.fetch_passwords,
            variant="secondary", width=180, height=44
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        ModernButton(
            btn_frame, text="💾 Зберегти пароль", command=self.update_password,
            variant="primary", width=160, height=44
        ).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Информационное сообщение
        info_frame = tk.Frame(self, bg=Colors.BG_TERTIARY)
        info_frame.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.MD)
        
        info_inner = tk.Frame(info_frame, bg=Colors.BG_TERTIARY)
        info_inner.pack(padx=Spacing.MD, pady=Spacing.MD)
        
        tk.Label(
            info_inner, text="ℹ️",
            font=(Fonts.FAMILY, 16), bg=Colors.BG_TERTIARY
        ).pack(side=tk.LEFT)
        
        tk.Label(
            info_inner,
            text="Паролі ролей використовуються для додаткової верифікації при виконанні певних дій",
            font=(Fonts.FAMILY, Fonts.SMALL_SIZE),
            fg=Colors.TEXT_SECONDARY, bg=Colors.BG_TERTIARY,
            wraplength=500
        ).pack(side=tk.LEFT, padx=Spacing.SM)
        
        self.fetch_passwords()
    
    def _on_role_change(self) -> None:
        role = self.role_var.get()
        current_password = self.passwords.get(role, "")
        self.password_entry.set(current_password)
    
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
            messagebox.showinfo("Увага", "Введіть новий пароль")
            return
        
        if len(password) < 4:
            messagebox.showinfo("Увага", "Пароль занадто короткий (мінімум 4 символи)")
            return
        
        def task() -> Any:
            return self.app.api.request_json(
                "POST", f"/admin/role-passwords/{role}",
                token=self.token, payload={"password": password}
            )
        
        def on_success(_: Any) -> None:
            self.passwords[role] = password
            messagebox.showinfo("Успіх", f"Пароль для ролі '{role}' оновлено")
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# АДМИН ПАНЕЛЬ SCANPAK
# ═══════════════════════════════════════════════════════════════════════════════

class ScanpakAdminPanel(tk.Toplevel):
    def __init__(self, parent: tk.Misc, app: TrackingApp, token: str) -> None:
        super().__init__(parent)
        self.app = app
        self.token = token
        self.title("🔐 Адмін панель СканПак")
        self.geometry("1000x700")
        self.configure(bg=Colors.BG_PRIMARY)
        
        # Центрируем
        self.update_idletasks()
        x = (self.winfo_screenwidth() - 1000) // 2
        y = (self.winfo_screenheight() - 700) // 2
        self.geometry(f"+{x}+{y}")
        
        # Заголовок
        header = tk.Frame(self, bg=Colors.BG_SECONDARY, height=50)
        header.pack(fill=tk.X)
        header.pack_propagate(False)
        
        tk.Label(
            header, text="🔐 Адмін панель СканПак",
            font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
            fg=Colors.SECONDARY, bg=Colors.BG_SECONDARY
        ).pack(side=tk.LEFT, padx=Spacing.LG, pady=Spacing.SM)
        
        ModernButton(
            header, text="✕ Закрити", command=self.destroy,
            variant="ghost", width=100, height=32
        ).pack(side=tk.RIGHT, padx=Spacing.MD, pady=Spacing.SM)
        
        # Контент
        content = tk.Frame(self, bg=Colors.BG_PRIMARY)
        content.pack(fill=tk.BOTH, expand=True, padx=Spacing.MD, pady=Spacing.MD)
        
        self.notebook = ModernNotebook(content)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        self.pending_tab = ScanpakAdminPendingTab(self.notebook.content_frame, app, token)
        self.users_tab = ScanpakAdminUsersTab(self.notebook.content_frame, app, token)
        
        self.notebook.add_tab("pending", "📝 Запити на реєстрацію", self.pending_tab)
        self.notebook.add_tab("users", "👥 Користувачі", self.users_tab)


class ScanpakAdminPendingTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        
        # Действия
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(actions, text="🔄 Оновити", command=self.fetch_requests, variant="primary", width=120, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="✅ Підтвердити", command=self.approve_request, variant="success", width=130, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="❌ Відхилити", command=self.reject_request, variant="danger", width=120, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Роль
        role_frame = tk.Frame(actions, bg=Colors.BG_CARD)
        role_frame.pack(side=tk.LEFT, padx=Spacing.LG)
        
        tk.Label(role_frame, text="Роль:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT)
        self.role_var = tk.StringVar(value="operator")
        ttk.Combobox(role_frame, textvariable=self.role_var, values=["admin", "operator"], width=12, state="readonly").pack(side=tk.LEFT, padx=Spacing.XS)
        
        self.count_label = tk.Label(actions, text="Запитів: 0", font=(Fonts.FAMILY, Fonts.BODY_SIZE, "bold"), fg=Colors.WARNING, bg=Colors.BG_CARD)
        self.count_label.pack(side=tk.RIGHT, padx=Spacing.MD)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(table_frame, columns=[("id", "ID", 80), ("surname", "Прізвище", 200), ("created", "Дата", 200)])
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        self.fetch_requests()
    
    def fetch_requests(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/admin/registration_requests", token=self.token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.count_label.config(text=f"Запитів: {len(data)}")
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
            return self.app.scanpak_api.request_json("POST", f"/admin/registration_requests/{request_id}/approve", token=self.token, payload={"role": role})
        
        def on_success(_: Any) -> None:
            messagebox.showinfo("Успіх", "Запит підтверджено")
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
            return self.app.scanpak_api.request_json("POST", f"/admin/registration_requests/{request_id}/reject", token=self.token)
        
        def on_success(_: Any) -> None:
            messagebox.showinfo("Успіх", "Запит відхилено")
            self.fetch_requests()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


class ScanpakAdminUsersTab(tk.Frame):
    def __init__(self, parent: tk.Frame, app: TrackingApp, token: str) -> None:
        super().__init__(parent, bg=Colors.BG_PRIMARY)
        self.app = app
        self.token = token
        
        # Действия
        actions_card = ModernCard(self, padding=Spacing.MD)
        actions_card.pack(fill=tk.X, padx=Spacing.SM, pady=Spacing.SM)
        
        actions = tk.Frame(actions_card.content, bg=Colors.BG_CARD)
        actions.pack(fill=tk.X)
        
        ModernButton(actions, text="🔄 Оновити", command=self.fetch_users, variant="primary", width=120, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="🔄 Змінити роль", command=self.change_role, variant="secondary", width=140, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        ModernButton(actions, text="⚡ Активність", command=self.toggle_active, variant="secondary", width=130, height=40).pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Параметры
        params = tk.Frame(actions, bg=Colors.BG_CARD)
        params.pack(side=tk.LEFT, padx=Spacing.LG)
        
        tk.Label(params, text="Роль:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT)
        self.role_var = tk.StringVar(value="operator")
        ttk.Combobox(params, textvariable=self.role_var, values=["admin", "operator"], width=10, state="readonly").pack(side=tk.LEFT, padx=Spacing.XS)
        
        tk.Label(params, text="Активний:", font=(Fonts.FAMILY, Fonts.SMALL_SIZE), fg=Colors.TEXT_SECONDARY, bg=Colors.BG_CARD).pack(side=tk.LEFT, padx=(Spacing.MD, 0))
        self.active_var = tk.StringVar(value="true")
        ttk.Combobox(params, textvariable=self.active_var, values=["true", "false"], width=8, state="readonly").pack(side=tk.LEFT, padx=Spacing.XS)
        
        # Таблица
        table_frame = tk.Frame(self, bg=Colors.BG_PRIMARY)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=Spacing.SM, pady=(0, Spacing.SM))
        
        self.tree = ModernTreeview(table_frame, columns=[("id", "ID", 60), ("surname", "Прізвище", 180), ("role", "Роль", 120), ("active", "Статус", 100)])
        self.tree.pack(fill=tk.BOTH, expand=True)
        
        self.fetch_users()
    
    def fetch_users(self) -> None:
        def task() -> List[Dict[str, Any]]:
            data = self.app.scanpak_api.request_json("GET", "/admin/users", token=self.token)
            return data if isinstance(data, list) else []
        
        def on_success(data: List[Dict[str, Any]]) -> None:
            self.tree.clear()
            for user in data:
                status = "✅ Активний" if user.get("is_active", False) else "❌ Неактивний"
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
            return self.app.scanpak_api.request_json("PATCH", f"/admin/users/{user_id}", token=self.token, payload={"role": role})
        
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
            return self.app.scanpak_api.request_json("PATCH", f"/admin/users/{user_id}", token=self.token, payload={"is_active": is_active})
        
        def on_success(_: Any) -> None:
            self.fetch_users()
        
        def on_error(exc: Exception) -> None:
            messagebox.showerror("Помилка", str(exc))
        
        run_async(self, task, on_success, on_error)


# ═══════════════════════════════════════════════════════════════════════════════
# ДИАЛОГОВЫЕ ОКНА
# ═══════════════════════════════════════════════════════════════════════════════

def simple_prompt(root: tk.Misc, prompt: str) -> Optional[str]:
    """Современное диалоговое окно для ввода"""
    dialog = tk.Toplevel(root)
    dialog.title("Введення даних")
    dialog.geometry("420x220")
    dialog.configure(bg=Colors.BG_PRIMARY)
    dialog.resizable(False, False)
    dialog.transient(root)
    dialog.grab_set()
    
    # Центрируем
    dialog.update_idletasks()
    x = (dialog.winfo_screenwidth() - 420) // 2
    y = (dialog.winfo_screenheight() - 220) // 2
    dialog.geometry(f"+{x}+{y}")
    
    # Контент
    content = tk.Frame(dialog, bg=Colors.BG_PRIMARY)
    content.pack(fill=tk.BOTH, expand=True, padx=Spacing.LG, pady=Spacing.LG)
    
    # Заголовок
    header = tk.Frame(content, bg=Colors.BG_PRIMARY)
    header.pack(fill=tk.X)
    
    tk.Label(header, text="🔐", font=(Fonts.FAMILY, 28), bg=Colors.BG_PRIMARY).pack(side=tk.LEFT)
    tk.Label(
        header, text=prompt,
        font=(Fonts.FAMILY, Fonts.SUBHEADER_SIZE, "bold"),
        fg=Colors.TEXT_PRIMARY, bg=Colors.BG_PRIMARY
    ).pack(side=tk.LEFT, padx=Spacing.SM)
    
    # Поле ввода
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
    
    # Кнопки
    btn_frame = tk.Frame(content, bg=Colors.BG_PRIMARY)
    btn_frame.pack(fill=tk.X, pady=(Spacing.MD, 0))
    
    ModernButton(btn_frame, text="Скасувати", command=cancel, variant="secondary", width=110, height=40).pack(side=tk.LEFT)
    ModernButton(btn_frame, text="Підтвердити", command=submit, variant="primary", width=120, height=40).pack(side=tk.RIGHT)
    
    # Привязки
    password_entry.bind("<Return>", lambda e: submit())
    dialog.bind("<Escape>", lambda e: cancel())
    
    # Фокус на поле
    dialog.after(100, lambda: password_entry.focus())
    
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
        
        # Инициализация данных
        self.store = LocalStore()
        self.state_data = self.store.load_state()
        self.api = ApiClient(BASE_URL)
        self.scanpak_api = ApiClient(f"{BASE_URL}{SCANPAK_BASE_PATH}")
        self.tracking_offline = OfflineQueue(self.store, self.store.tracking_offline_path)
        self.scanpak_offline = OfflineQueue(self.store, self.store.scanpak_offline_path)
        
        # Основной контейнер
        self.container = tk.Frame(self, bg=Colors.BG_PRIMARY)
        self.container.pack(fill=tk.BOTH, expand=True)
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)
        
        # Создаем фреймы
        self.frames: Dict[str, tk.Frame] = {}
        for frame_class in (
            StartFrame,
            TrackingLoginFrame,
            ScanpakLoginFrame,
            TrackingMainFrame,
            ScanpakMainFrame,
        ):
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

        
